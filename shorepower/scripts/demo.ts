/**
 * 端到端演示（npm run demo，依赖安装后 80 秒内完成）。
 *
 * 流程：
 *   1. 生成双源夹具，准备仅含 run1 文件的输入目录与独立数据目录；
 *   2. 全量发布，核对跨日拆账、单位等值、缺失/冲突/回退隔离；
 *   3. 拷入 run2 补发/更正/乱序重送文件，指定 2026-11-04 重跑；
 *   4. 验证修订替换、旧文档撤下、缺失补发后恢复、未涉及接电沿用；
 *   5. 再跑一次相同输入，验证幂等（不产生重复发布）；
 *   6. 模拟发布中途退出残留，验证最近完整发布仍可查询；
 *   7. 启动临时 HTTP 服务，验证单发布快照查询与跨日结果。
 */

import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

import { runFull, rerunDay } from '../src/pipeline.js';
import { cleanupStaging, loadCurrentPublication } from '../src/storage.js';
import { startHttpServer } from '../src/server.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(here, '..');
const fixturesRoot = path.join(projectRoot, 'fixtures');

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

async function copyTree(src: string, dst: string): Promise<void> {
  await cp(src, dst, { recursive: true });
}

async function main(): Promise<void> {
  // 0. 确保夹具已生成（子进程，避免与演示目录状态耦合）
  console.log('① 生成双源夹具（GB18030 CSV + UTF-8 JSONL）');
  execFileSync(process.execPath, ['--import', 'tsx', path.join(here, 'generate-fixtures.ts')], {
    cwd: projectRoot,
    stdio: 'inherit',
  });

  const work = await mkdtemp(path.join(os.tmpdir(), 'shorepower-demo-'));
  const inputDir = path.join(work, 'input');
  const dataDir = path.join(work, 'data');
  await mkdir(path.join(inputDir, 'connections'), { recursive: true });
  await mkdir(path.join(inputDir, 'readings'), { recursive: true });
  await mkdir(dataDir, { recursive: true });

  await copyTree(
    path.join(fixturesRoot, 'run1', 'connections'),
    path.join(inputDir, 'connections'),
  );
  await copyTree(
    path.join(fixturesRoot, 'run1', 'readings'),
    path.join(inputDir, 'readings'),
  );

  // 2. 首次全量发布
  console.log('\n② 首次全量发布（仅 run1 文件）');
  const first = await runFull(inputDir, dataDir);
  checkTrue('首次发布产生新版本', !first.skipped);
  check('日账条数', first.accountCount, 5);
  check('隔离条数', first.quarantineCount, 4);
  const pub1 = await loadCurrentPublication(dataDir);
  if (!pub1) throw new Error('发布后读不到当前发布');

  const acc = (id: string, day: string) =>
    pub1.accounts.find((a) => a.connectionId === id && a.beijingDay === day);
  // 11-03 窗口 = 28kWh(午夜) - 12kWh(起点) = 16000 Wh；11-04 = 34kWh - 28kWh
  check('CONN-101 11-03 用量(Wh)', acc('CONN-101', '2026-11-03')?.usageWh, 16000);
  check('CONN-101 11-04 用量(Wh)', acc('CONN-101', '2026-11-04')?.usageWh, 6000);
  check('CONN-102 单日用量(Wh)（kWh/Wh 等值读数不冲突）', acc('CONN-102', '2026-11-03')?.usageWh, 9500);
  check('CONN-106 11-03 用量(Wh)', acc('CONN-106', '2026-11-03')?.usageWh, 12000);
  check('CONN-106 11-04 用量(Wh)', acc('CONN-106', '2026-11-04')?.usageWh, 18000);

  const quar = (id: string) => pub1.quarantines.find((q) => q.connectionId === id);
  check('CONN-103 隔离原因', quar('CONN-103')?.reasons, ['MISSING_MIDNIGHT_READING']);
  check('CONN-104 隔离原因', quar('CONN-104')?.reasons, ['CONFLICTING_READING']);
  check('CONN-105 隔离原因', quar('CONN-105')?.reasons, ['CUMULATIVE_REGRESSION']);
  checkTrue('CONN-107 缺午夜读数被隔离', !!quar('CONN-107'));

  // 血缘可追溯
  const lin101 = pub1.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-101-2026-11-03');
  checkTrue('日账血缘包含接电记录出处', (lin101?.connectionSources.length ?? 0) > 0);
  checkTrue('日账血缘包含读数出处', (lin101?.readingSources.length ?? 0) >= 3);
  checkTrue('血缘记录输入文件清单', (lin101?.inputFiles.length ?? 0) >= 2);
  const lin103 = pub1.lineage.find((l) => l.subjectDocId === 'QUAR-CONN-103');
  checkTrue('隔离结果同样带血缘', !!lin103);

  // 3. 拷入补发/更正/重送文件后重跑 11-04
  console.log('\n③ 拷入 run2（更正、补发、乱序重送），按北京时间 2026-11-04 重跑');
  await copyTree(
    path.join(fixturesRoot, 'run2', 'connections'),
    path.join(inputDir, 'connections'),
  );
  await copyTree(
    path.join(fixturesRoot, 'run2', 'readings'),
    path.join(inputDir, 'readings'),
  );
  const second = await rerunDay(inputDir, dataDir, '2026-11-04');
  checkTrue('重跑产生新版本', !second.skipped);
  const pub2 = await loadCurrentPublication(dataDir);
  if (!pub2) throw new Error('重跑后读不到当前发布');

  check('重算接电集合（排序后）', second.recomputedConnectionIds.sort(), [
    'CONN-101',
    'CONN-103',
    'CONN-104',
    'CONN-105',
    'CONN-106',
    'CONN-107',
  ]);
  check('沿用接电集合（排序后）', second.carriedConnectionIds.sort(), ['CONN-102']);
  check('撤下清单（CONN-107 隔离转日账，旧隔离文档撤下）', second.withdrawnDocIds, ['QUAR-CONN-107']);

  const acc2 = (id: string, day: string) =>
    pub2.accounts.find((a) => a.connectionId === id && a.beijingDay === day);
  // R101 起点读数修订 12000 -> 11900 Wh 后，11-03 用量增加 100 Wh，11-04 不受影响
  check('CONN-101 修订后 11-03 用量(Wh)', acc2('CONN-101', '2026-11-03')?.usageWh, 16100);
  check('CONN-101 修订后 11-04 用量(Wh)', acc2('CONN-101', '2026-11-04')?.usageWh, 6000);
  // CONN-106 rev2 缩短窗口：旧 18000 Wh 日账被 12000 Wh 替换
  check('CONN-106 rev2 11-04 用量(Wh)', acc2('CONN-106', '2026-11-04')?.usageWh, 12000);
  check('CONN-106 修订号已升为 2', acc2('CONN-106', '2026-11-04')?.revision, 2);
  // CONN-107 补发午夜读数后解除隔离
  checkTrue('CONN-107 解除隔离', !pub2.quarantines.some((q) => q.connectionId === 'CONN-107'));
  check('CONN-107 11-03 用量(Wh)', acc2('CONN-107', '2026-11-03')?.usageWh, 3900);
  check('CONN-107 11-04 用量(Wh)', acc2('CONN-107', '2026-11-04')?.usageWh, 3600);
  // 未涉及接电沿用：CONN-102 文档原样保留且仍只有一条
  check('CONN-102 沿用后仍为 1 条日账', pub2.accounts.filter((a) => a.connectionId === 'CONN-102').length, 1);
  check('重跑后日账总数', pub2.accounts.length, 7);
  check('重跑后隔离总数', pub2.quarantines.length, 3);
  // 重送文件被哈希识别
  checkTrue(
    '清单记录字节级重送文件',
    pub2.manifest.inputFiles.some((f) => f.duplicateOf !== undefined),
  );

  // 4. 相同输入再重跑：幂等
  console.log('\n④ 相同输入再次重跑，验证幂等（不产生重复发布）');
  const third = await rerunDay(inputDir, dataDir, '2026-11-04');
  checkTrue('重复运行被跳过', third.skipped);
  check('指针仍指向上一完整发布', third.publicationId, pub2.manifest.publicationId);

  // 5. 原子发布：注入临时残留目录，清理后当前发布不变
  console.log('\n⑤ 模拟发布中途退出：残留临时目录不影响最近完整发布的可查询性');
  const tmpResidual = path.join(
    dataDir,
    'publications',
    `pub-full-20990101000000-abcde.tmp-${process.pid}-dead`,
  );
  await mkdir(tmpResidual, { recursive: true });
  await writeFile(path.join(tmpResidual, 'manifest.json'), '{损坏的半成品');
  await cleanupStaging(dataDir);
  const afterCleanup = await loadCurrentPublication(dataDir);
  check('清理残留后当前发布不变', afterCleanup?.manifest.publicationId, pub2.manifest.publicationId);

  // 6. HTTP 快照查询
  console.log('\n⑥ 启动本地 HTTP 接口验证快照隔离与跨日查询（临时端口，自动关闭）');
  const handle = await startHttpServer(dataDir, 0);
  const base = `http://localhost:${handle.port}`;
  const get = async (p: string) => (await fetch(`${base}${p}`)).json() as Promise<Record<string, any>>;

  const health = await get('/health');
  check('健康检查返回当前发布 id', health.snapshot?.publicationId, pub2.manifest.publicationId);

  const dayRows = await get('/accounts?day=2026-11-04');
  // 11-04 日账：CONN-101/106/107 各一条（103/104/105 仍隔离，102 为 11-03 单日）
  check('11-04 跨日查询命中 3 条日账', dayRows.count, 3);
  checkTrue('响应自带同一发布快照', dayRows.snapshot?.publicationId === pub2.manifest.publicationId);

  const connRows = await get('/connection?id=CONN-106');
  check('CONN-106 返回 2 条日账', connRows.accounts?.length, 2);
  checkTrue('CONN-106 无隔离', connRows.quarantine === null);
  checkTrue('CONN-106 血缘随行', (connRows.lineage?.length ?? 0) === 2);

  const qRows = await get('/quarantines');
  check('隔离区返回 3 条', qRows.count, 3);

  const linRows = await get('/lineage?docId=DAILY-CONN-107-2026-11-04');
  check('按文档 id 查血缘命中 1 条', linRows.count, 1);
  checkTrue('补发读数 R703 进入血缘', JSON.stringify(linRows.lineage?.[0] ?? {}).includes('R703'));

  const pubs = await get('/publications');
  checkTrue('历史发布可枚举', (pubs.allPublicationIds?.length ?? 0) >= 2);
  await handle.close();

  console.log('\n────────────── 演示小结 ──────────────');
  console.log(`工作目录（临时）：${work}`);
  console.log('存储布局：data/publications/<pub-id>/{daily-accounts,quarantines,lineage}.jsonl + manifest.json');
  if (failures === 0) {
    console.log('全部检查通过 ✅');
  } else {
    console.log(`${failures} 项检查失败 ❌`);
    process.exitCode = 1;
  }
  // 临时目录保留以便人工查看；如需清理可删除上述路径
  void readFile;
  void rm;
}

main().catch((err) => {
  console.error('演示失败：', err);
  process.exit(1);
});
