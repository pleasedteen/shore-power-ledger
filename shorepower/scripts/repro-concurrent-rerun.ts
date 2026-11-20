/**
 * 并发重跑与中断恢复复现脚本（本地子进程）。
 *
 * 运行：npx tsx scripts/repro-concurrent-rerun.ts
 *
 * 场景：同一数据目录上两笔互不相干的更正——
 *   - A 笔：重跑 2026-11-03，修正电表 MA 的起点读数（CONN-A 日账变化）；
 *   - B 笔：重跑 2026-11-05，消除 CONN-B 的读数冲突，使其由隔离转日账（旧隔离文档撤下）。
 *
 * 复现/验证内容：
 *   1. A 笔算完、提交前暂停（子进程轮询状态文件），暂停期间不持锁；
 *   2. 暂停期间查询始终读到一份完整发布（基线），暂存发布 id 不在历史清单、无法按号查询；
 *   3. B 笔在另一子进程正常发布，查询立刻看到 B 的更正；
 *   4. A 笔恢复提交，按接电分区合并：两笔更正同时生效，各自日账/隔离/血缘保留，
 *      撤下墓碑（QUAR-CONN-B）随并集保留；
 *   5. SIGKILL 模拟提交前进程被杀：暂存不丢、指针不变，CLI resume 后正常发布；
 *   6. 重试相同输入被指纹幂等跳过。
 */

import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { spawn, type ChildProcess } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { encodeGb18030 } from '../src/encoding.js';
import { startHttpServer, type HttpServerHandle } from '../src/server.js';
import { listPublicationIds, loadCurrentPublication } from '../src/storage.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(here, '..');
const cliPath = path.join(projectRoot, 'src', 'cli.ts');

let failures = 0;
function check(label: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) {
    console.log(`  ✔ ${label}`);
  } else {
    failures++;
    console.log(`  ✘ ${label}\n      期望: ${JSON.stringify(expected)}\n      实际: ${JSON.stringify(actual)}`);
  }
}
function checkTrue(label: string, cond: boolean): void {
  check(label, cond, true);
}

interface CsvRow {
  connection_id: string;
  revision: number;
  vessel: string;
  meter_id: string;
  start_beijing: string;
  end_beijing: string;
}
interface JsonlRow {
  reading_id: string;
  revision: number;
  meter_id: string;
  sampled_at_utc: string;
  unit: 'Wh' | 'kWh';
  cumulative: string;
}

const csvHeader = 'connection_id,revision,vessel,meter_id,start_beijing,end_beijing';
function toCsv(rows: CsvRow[]): string {
  return [csvHeader, ...rows.map((r) => Object.values(r).join(','))].join('\n') + '\n';
}
function toJsonl(rows: JsonlRow[]): string {
  return rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
}

async function writeCsv(file: string, rows: CsvRow[]): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, encodeGb18030(toCsv(rows)));
}
async function writeJsonl(file: string, rows: JsonlRow[]): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, toJsonl(rows), 'utf-8');
}

/** 子进程方式跑 CLI，返回退出码与输出 */
function runCli(args: string[], env: NodeJS.ProcessEnv = {}): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', cliPath, ...args], {
      cwd: projectRoot,
      env: { ...process.env, ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (b) => (stdout += b.toString()));
    child.stderr.on('data', (b) => (stderr += b.toString()));
    child.on('error', reject);
    child.on('close', (code) => resolve({ code: code ?? -1, stdout, stderr }));
  });
}

/** 带子进程暂停钩子的 CLI；返回子进程句柄与状态文件路径 */
function spawnPausedRerun(
  args: string[],
  statusFile: string,
): ChildProcess {
  return spawn(process.execPath, ['--import', 'tsx', cliPath, ...args], {
    cwd: projectRoot,
    env: { ...process.env, SHOREPOWER_PAUSE_STATUS_FILE: statusFile },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

async function waitForPauseState(statusFile: string): Promise<{ token: string; publicationId: string }> {
  const deadline = Date.now() + 30_000;
  for (;;) {
    try {
      const raw = JSON.parse(await readFile(statusFile, 'utf-8')) as {
        state: string;
        token: string;
        publicationId: string;
      };
      if (raw.state === 'paused') return { token: raw.token, publicationId: raw.publicationId };
    } catch {
      // 尚未写出
    }
    if (Date.now() > deadline) throw new Error(`等待暂停状态超时：${statusFile}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

async function setPauseState(statusFile: string, state: 'resume' | 'cancel'): Promise<void> {
  const raw = JSON.parse(await readFile(statusFile, 'utf-8'));
  await writeFile(statusFile, JSON.stringify({ ...raw, state }, null, 2) + '\n', 'utf-8');
}

function childExit(child: ChildProcess): Promise<number | null> {
  return new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code) => resolve(code));
  });
}

async function main(): Promise<void> {
  const work = await mkdtemp(path.join(os.tmpdir(), 'shorepower-concurrent-'));
  const inputBase = path.join(work, 'input-base');
  const inputA = path.join(work, 'input-a');
  const inputB = path.join(work, 'input-b');
  const inputA2 = path.join(work, 'input-a2');
  const data = path.join(work, 'data');
  await mkdir(data, { recursive: true });
  console.log(`工作目录：${work}`);

  // ── 夹具：A/C 覆盖 11-03，B 覆盖 11-05，Q 跨 11-03/11-04 且缺午夜读数被隔离 ──
  const baseConnections: CsvRow[] = [
    { connection_id: 'CONN-A', revision: 1, vessel: '甲船', meter_id: 'MA', start_beijing: '2026-11-03 08:00:00', end_beijing: '2026-11-03 20:00:00' },
    { connection_id: 'CONN-B', revision: 1, vessel: '乙船', meter_id: 'MB', start_beijing: '2026-11-05 08:00:00', end_beijing: '2026-11-05 20:00:00' },
    { connection_id: 'CONN-C', revision: 1, vessel: '丙船', meter_id: 'MC', start_beijing: '2026-11-03 08:00:00', end_beijing: '2026-11-05 20:00:00' },
    { connection_id: 'CONN-Q', revision: 1, vessel: '丁船', meter_id: 'MQ', start_beijing: '2026-11-03 20:00:00', end_beijing: '2026-11-04 04:00:00' },
  ];
  const baseReadings: JsonlRow[] = [
    // MA：北京 11-03 08:00=UTC 00:00 起，20:00=UTC 12:00 止；基线用量 12000 Wh
    { reading_id: 'RA1', revision: 1, meter_id: 'MA', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '10000' },
    { reading_id: 'RA2', revision: 1, meter_id: 'MA', sampled_at_utc: '2026-11-03T12:00:00Z', unit: 'Wh', cumulative: '22000' },
    // MB：同一时刻两条不一致读数 → CONFLICTING_READING 隔离
    { reading_id: 'RB1', revision: 1, meter_id: 'MB', sampled_at_utc: '2026-11-05T00:00:00Z', unit: 'Wh', cumulative: '10000' },
    { reading_id: 'RB2', revision: 1, meter_id: 'MB', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '20000' },
    { reading_id: 'RB2X', revision: 1, meter_id: 'MB', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '19999' },
    // MC：跨 11-03/04/05 三天，含两个北京午夜（UTC 16:00）
    { reading_id: 'RC1', revision: 1, meter_id: 'MC', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '0' },
    { reading_id: 'RC2', revision: 1, meter_id: 'MC', sampled_at_utc: '2026-11-03T16:00:00Z', unit: 'Wh', cumulative: '5000' },
    { reading_id: 'RC3', revision: 1, meter_id: 'MC', sampled_at_utc: '2026-11-04T16:00:00Z', unit: 'Wh', cumulative: '9000' },
    { reading_id: 'RC4', revision: 1, meter_id: 'MC', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '14000' },
    // MQ：缺北京 11-04 午夜（UTC 11-03T16:00）读数
    { reading_id: 'RQ1', revision: 1, meter_id: 'MQ', sampled_at_utc: '2026-11-03T12:00:00Z', unit: 'Wh', cumulative: '1000' },
    { reading_id: 'RQ3', revision: 1, meter_id: 'MQ', sampled_at_utc: '2026-11-03T20:00:00Z', unit: 'Wh', cumulative: '5000' },
  ];

  await writeCsv(path.join(inputBase, 'connections', 'base_connections.csv'), baseConnections);
  await writeJsonl(path.join(inputBase, 'readings', 'base_readings.jsonl'), baseReadings);

  // A 笔输入：基线 + RA1 修订（10000 → 9000），CONN-A 当日用量 12000 → 13000
  await cp(inputBase, inputA, { recursive: true });
  await writeJsonl(path.join(inputA, 'readings', 'a_correction.jsonl'), [
    { reading_id: 'RA1', revision: 2, meter_id: 'MA', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '9000' },
  ]);

  // B 笔输入：基线 + 冲突读数双双升修订对齐为 20000，CONN-B 解除隔离转日账
  await cp(inputBase, inputB, { recursive: true });
  await writeJsonl(path.join(inputB, 'readings', 'b_correction.jsonl'), [
    { reading_id: 'RB2', revision: 2, meter_id: 'MB', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '20000' },
    { reading_id: 'RB2X', revision: 2, meter_id: 'MB', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '20000' },
  ]);

  // A2 输入：在 A 基础上再修订 RA2（22000 → 23000），用于中断恢复后的新一笔
  await cp(inputA, inputA2, { recursive: true });
  await writeJsonl(path.join(inputA2, 'readings', 'a2_correction.jsonl'), [
    { reading_id: 'RA2', revision: 2, meter_id: 'MA', sampled_at_utc: '2026-11-03T12:00:00Z', unit: 'Wh', cumulative: '23000' },
  ]);

  // ── 0. 基线全量发布（子进程） ──
  console.log('\n① 基线全量发布（子进程）');
  const base = await runCli(['run', '--input', inputBase, '--data', data]);
  check('基线命令退出码为 0', base.code, 0);
  const pub0 = (await loadCurrentPublication(data))!;
  // A 单日 1 条 + C 跨 11-03/04/05 共 3 条；B、Q 被隔离无日账
  check('基线日账 4 条', pub0.accounts.length, 4);
  check('基线隔离 2 条（CONN-B 读数冲突、CONN-Q 缺午夜）', pub0.quarantines.length, 2);
  const baseId = pub0.manifest.publicationId;
  const usage = async (conn: string, day: string) => {
    const cur = (await loadCurrentPublication(data))!;
    return cur.accounts.find((a) => a.connectionId === conn && a.beijingDay === day)?.usageWh;
  };
  check('基线 CONN-A 用量 12000 Wh', await usage('CONN-A', '2026-11-03'), 12000);

  // ── 查询服务：整个复现过程中持续验证快照完整性 ──
  const server: HttpServerHandle = await startHttpServer(data, 0);
  const httpBase = `http://localhost:${server.port}`;
  const get = async (p: string) => (await fetch(`${httpBase}${p}`)).json() as Promise<Record<string, any>>;

  // ── 1. A 笔暂停在“算完、提交前” ──
  console.log('\n② A 笔重跑 2026-11-03，提交前暂停（子进程 + 状态文件钩子）');
  const statusA = path.join(work, 'pause-a.json');
  const childA = spawnPausedRerun(
    ['rerun', '--day', '2026-11-03', '--input', inputA, '--data', data],
    statusA,
  );
  childA.stdout?.on('data', () => {});
  childA.stderr?.on('data', () => {});
  const pausedA = await waitForPauseState(statusA);
  console.log(`   A 已暂停：token=${pausedA.token} 暂存发布=${pausedA.publicationId}`);

  // ── 2. 暂停期间：查询只能读到完整基线发布，暂存版本不可见 ──
  console.log('\n③ 暂停期间查询完整性与暂存不可见性');
  const healthWhilePaused = await get('/health');
  check('暂停中当前发布仍为基线', healthWhilePaused.snapshot?.publicationId, baseId);
  const accountsWhilePaused = await get('/accounts?day=2026-11-03');
  // 11-03 当日日账：CONN-A 一条 + CONN-C 一条 = 2（Q 在 11-03 只有起点窗口，隔离无日账）
  check('暂停中 11-03 日账为 A、C 两条，快照完整', accountsWhilePaused.count, 2);
  const hidden = await get(`/health?publicationId=${pausedA.publicationId}`);
  checkTrue('暂存发布无法按发布号查询', typeof hidden.error === 'string');
  const listedWhilePaused = await listPublicationIds(data);
  checkTrue('暂存发布不进入历史发布清单', !listedWhilePaused.includes(pausedA.publicationId));

  // ── 3. B 笔在另一子进程正常发布（A 仍暂停、不持锁） ──
  console.log('\n④ B 笔重跑 2026-11-05 并发发布（另一子进程，A 仍暂停）');
  const runB = await runCli(['rerun', '--day', '2026-11-05', '--input', inputB, '--data', data]);
  check('B 笔命令退出码为 0', runB.code, 0);
  if (runB.code !== 0) console.log(runB.stdout, runB.stderr);
  const pubB = (await loadCurrentPublication(data))!;
  check('B 笔产生新发布', pubB.manifest.publicationId !== baseId, true);
  check('CONN-B 解除隔离', pubB.quarantines.some((q) => q.connectionId === 'CONN-B'), false);
  check('CONN-B 11-05 用量 10000 Wh', await usage('CONN-B', '2026-11-05'), 10000);
  check('A 尚未恢复，CONN-A 仍是基线 12000 Wh', await usage('CONN-A', '2026-11-03'), 12000);
  check('B 笔撤下旧隔离文档 QUAR-CONN-B', pubB.manifest.withdrawnDocIds.includes('QUAR-CONN-B'), true);
  const healthAfterB = await get('/health');
  check('查询快照已切换到 B 的完整发布', healthAfterB.snapshot?.publicationId, pubB.manifest.publicationId);

  // ── 4. 恢复 A：分区合并，两笔更正同时生效 ──
  console.log('\n⑤ 恢复 A 笔提交，按接电分区合并两笔更正');
  await setPauseState(statusA, 'resume');
  const codeA = await childExit(childA);
  check('A 恢复后退出码为 0', codeA, 0);
  const pubMerged = (await loadCurrentPublication(data))!;
  check('合并后再次产生新发布', pubMerged.manifest.publicationId !== pubB.manifest.publicationId, true);
  check('A 笔更正生效：CONN-A 用量 13000 Wh', await usage('CONN-A', '2026-11-03'), 13000);
  check('B 笔更正保留：CONN-B 用量 10000 Wh', await usage('CONN-B', '2026-11-05'), 10000);
  check('CONN-C 三天日账完整保留', pubMerged.accounts.filter((a) => a.connectionId === 'CONN-C').length, 3);
  check('CONN-Q 隔离保留', pubMerged.quarantines.some((q) => q.connectionId === 'CONN-Q'), true);
  check('CONN-B 未回到隔离', pubMerged.quarantines.some((q) => q.connectionId === 'CONN-B'), false);
  check('撤下墓碑并集保留 QUAR-CONN-B', pubMerged.manifest.withdrawnDocIds.includes('QUAR-CONN-B'), true);

  // 血缘：两笔更正的输入文件与修订出处都在
  const lineageA = pubMerged.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-A-2026-11-03');
  const lineageB = pubMerged.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-B-2026-11-05');
  checkTrue('A 的血缘引用 a_correction.jsonl', JSON.stringify(lineageA ?? {}).includes('a_correction.jsonl'));
  checkTrue('A 的血缘包含 RA1 rev=2 出处', JSON.stringify(lineageA ?? {}).includes('"revision":2'));
  checkTrue('B 的血缘引用 b_correction.jsonl', JSON.stringify(lineageB ?? {}).includes('b_correction.jsonl'));
  const allFiles = new Set(pubMerged.manifest.inputFiles.map((f) => f.file));
  checkTrue('清单输入文件为两笔并集', allFiles.has('readings/a_correction.jsonl') && allFiles.has('readings/b_correction.jsonl'));
  checkTrue(
    '清单带并发合并审计告警',
    pubMerged.manifest.warnings.some((w) => w.includes('分区合并')),
  );
  const finalIds = await listPublicationIds(data);
  checkTrue('A 的暂存发布 id 从未物化', !finalIds.includes(pausedA.publicationId));
  const mergedHealth = await get('/health');
  check('查询快照最终指向合并发布', mergedHealth.snapshot?.publicationId, pubMerged.manifest.publicationId);

  // ── 5. SIGKILL 中断：提交前杀进程，暂存保留、指针不变，resume 后发布 ──
  console.log('\n⑥ SIGKILL 模拟提交前进程退出，验证暂存可恢复');
  const currentBeforeKill = (await loadCurrentPublication(data))!.manifest.publicationId;
  const statusA2 = path.join(work, 'pause-a2.json');
  const childA2 = spawnPausedRerun(
    ['rerun', '--day', '2026-11-03', '--input', inputA2, '--data', data],
    statusA2,
  );
  childA2.stdout?.on('data', () => {});
  childA2.stderr?.on('data', () => {});
  const pausedA2 = await waitForPauseState(statusA2);
  childA2.kill('SIGKILL');
  const killCode = await childExit(childA2);
  check('子进程被 SIGKILL 终止', killCode === null || killCode === 137, true);
  check('被杀后当前发布指针不变', (await loadCurrentPublication(data))!.manifest.publicationId, currentBeforeKill);
  const stagingList = await runCli(['staging', '--data', data]);
  checkTrue('CLI 可列出中断残留暂存', stagingList.stdout.includes(pausedA2.token));
  const killedHidden = await get(`/health?publicationId=${pausedA2.publicationId}`);
  checkTrue('中断版本无法按发布号查询', typeof killedHidden.error === 'string');
  checkTrue('中断版本不在历史清单', !(await listPublicationIds(data)).includes(pausedA2.publicationId));

  const resume = await runCli(['resume', '--token', pausedA2.token, '--data', data]);
  check('resume 命令退出码为 0', resume.code, 0);
  if (resume.code !== 0) console.log(resume.stdout, resume.stderr);
  const pubRecovered = (await loadCurrentPublication(data))!;
  // RA2 23000 - RA1 9000 = 14000 Wh
  check('恢复后 A2 更正生效：CONN-A 用量 14000 Wh', await usage('CONN-A', '2026-11-03'), 14000);
  check('恢复后 B 笔更正仍在：CONN-B 用量 10000 Wh', await usage('CONN-B', '2026-11-05'), 10000);
  check('恢复后 CONN-Q 隔离仍在', pubRecovered.quarantines.some((q) => q.connectionId === 'CONN-Q'), true);
  check('恢复后墓碑仍含 QUAR-CONN-B', pubRecovered.manifest.withdrawnDocIds.includes('QUAR-CONN-B'), true);
  const stagingAfter = await runCli(['staging', '--data', data]);
  check('提交成功后暂存被清理', stagingAfter.stdout.includes('没有待提交的暂存发布'), true);

  // ── 6. 相同输入重试：幂等跳过 ──
  console.log('\n⑦ 相同输入重试，验证幂等跳过');
  const retry = await runCli(['rerun', '--day', '2026-11-03', '--input', inputA2, '--data', data]);
  check('重试命令退出码为 0', retry.code, 0);
  checkTrue('重试被幂等跳过', retry.stdout.includes('跳过新发布'));
  check('指针保持不变', (await loadCurrentPublication(data))!.manifest.publicationId, pubRecovered.manifest.publicationId);

  // ── 7. 无暂停的真并发压力：两个不同日期的重跑同时启动、重复多轮，靠锁串行 + 分区合并不丢更正 ──
  console.log('\n⑧ 无暂停真并发压力：A/B 两笔重跑同时启动 ×3 轮，互不相干的更正都不丢');
  for (let round = 0; round < 3; round++) {
    const results = await Promise.all([
      runCli(['rerun', '--day', '2026-11-03', '--input', inputA2, '--data', data]),
      runCli(['rerun', '--day', '2026-11-05', '--input', inputB, '--data', data]),
    ]);
    check(`第 ${round + 1} 轮两个并发重跑都成功退出`, results.every((r) => r.code === 0), true);
    const cur = (await loadCurrentPublication(data))!;
    check(`第 ${round + 1} 轮后 A 更正仍在（14000 Wh）`, cur.accounts.find((a) => a.connectionId === 'CONN-A')?.usageWh, 14000);
    check(`第 ${round + 1} 轮后 B 更正仍在（10000 Wh）`, cur.accounts.find((a) => a.connectionId === 'CONN-B')?.usageWh, 10000);
    checkTrue(
      `第 ${round + 1} 轮后 CONN-B 未回退为隔离`,
      !cur.quarantines.some((q) => q.connectionId === 'CONN-B'),
    );
    checkTrue(
      `第 ${round + 1} 轮后墓碑仍含 QUAR-CONN-B`,
      cur.manifest.withdrawnDocIds.includes('QUAR-CONN-B'),
    );
  }
  // 压力轮内容相同时应被幂等/合并吸收，正式发布数有界
  const idsAfterStress = await listPublicationIds(data);
  checkTrue('压力轮未产生失控的发布目录（≤ 8 个）', idsAfterStress.length <= 8);
  const noStagingLeft = await runCli(['staging', '--data', data]);
  check('压力轮后无残留暂存', noStagingLeft.stdout.includes('没有待提交的暂存发布'), true);

  await server.close();

  console.log('\n────────────── 复现小结 ──────────────');
  console.log(`历史发布目录数：${idsAfterStress.length}（并发发布由锁串行化、相同内容幂等跳过，数量有界）`);
  if (failures === 0) {
    console.log('全部检查通过 ✅');
  } else {
    console.log(`${failures} 项检查失败 ❌`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error('复现失败：', err);
  process.exit(1);
});
