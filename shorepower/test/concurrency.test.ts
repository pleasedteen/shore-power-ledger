/**
 * 并发重跑与中断恢复的进程内集成测试。
 *
 * 与 scripts/repro-concurrent-rerun.ts（真实子进程 + SIGKILL）互补：
 * 这里用暂停钩子 waitForResume 在单进程内确定性复现“暂存期间另一笔重跑抢先发布”，
 * 并覆盖暂存持久化、僵死锁接管、暂存不可见、取消暂存等边界。
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { encodeGb18030 } from '../src/encoding.js';
import {
  listPendingStaging,
  rerunDay,
  resumePublication,
  runFull,
} from '../src/pipeline.js';
import {
  acquirePublicationLock,
  listPublicationIds,
  loadCurrentPublication,
  loadPublication,
  prepareStaging,
  type StagingState,
} from '../src/storage.js';

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

async function freshDirs(): Promise<{ root: string; input: string; data: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), 'concurrent-test-'));
  const input = path.join(root, 'input');
  const data = path.join(root, 'data');
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(data, { recursive: true });
  return { root, input, data };
}

function toCsv(rows: CsvRow[]): string {
  const header = 'connection_id,revision,vessel,meter_id,start_beijing,end_beijing';
  return [header, ...rows.map((r) => Object.values(r).join(','))].join('\n') + '\n';
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

/**
 * 基线夹具：
 * - CONN-X（11-03，电表 MX）正常日账，用量 2000 Wh；
 * - CONN-Y（11-05，电表 MY）同一时刻读数冲突，被隔离。
 */
async function writeBaseline(input: string): Promise<void> {
  await writeCsv(path.join(input, 'connections', 'base.csv'), [
    { connection_id: 'CONN-X', revision: 1, vessel: '甲船', meter_id: 'MX', start_beijing: '2026-11-03 08:00:00', end_beijing: '2026-11-03 20:00:00' },
    { connection_id: 'CONN-Y', revision: 1, vessel: '乙船', meter_id: 'MY', start_beijing: '2026-11-05 08:00:00', end_beijing: '2026-11-05 20:00:00' },
  ]);
  await writeJsonl(path.join(input, 'readings', 'base.jsonl'), [
    { reading_id: 'RX1', revision: 1, meter_id: 'MX', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '1000' },
    { reading_id: 'RX2', revision: 1, meter_id: 'MX', sampled_at_utc: '2026-11-03T12:00:00Z', unit: 'Wh', cumulative: '3000' },
    { reading_id: 'RY1', revision: 1, meter_id: 'MY', sampled_at_utc: '2026-11-05T00:00:00Z', unit: 'Wh', cumulative: '5000' },
    { reading_id: 'RY2', revision: 1, meter_id: 'MY', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '8000' },
    { reading_id: 'RY2X', revision: 1, meter_id: 'MY', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '7999' },
  ]);
}

/** A 笔输入：MX 起点读数修订 1000 → 900，CONN-X 用量 2000 → 2100 */
async function writeInputA(dst: string): Promise<void> {
  await rm(dst, { recursive: true, force: true });
  await mkdir(dst, { recursive: true });
  await writeBaseline(dst);
  await writeJsonl(path.join(dst, 'readings', 'a-correction.jsonl'), [
    { reading_id: 'RX1', revision: 2, meter_id: 'MX', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '900' },
  ]);
}

/** B 笔输入：MY 冲突读数双双升修订对齐 8000，CONN-Y 解除隔离，用量 3000 */
async function writeInputB(dst: string): Promise<void> {
  await rm(dst, { recursive: true, force: true });
  await mkdir(dst, { recursive: true });
  await writeBaseline(dst);
  await writeJsonl(path.join(dst, 'readings', 'b-correction.jsonl'), [
    { reading_id: 'RY2', revision: 2, meter_id: 'MY', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '8000' },
    { reading_id: 'RY2X', revision: 2, meter_id: 'MY', sampled_at_utc: '2026-11-05T12:00:00Z', unit: 'Wh', cumulative: '8000' },
  ]);
}

test('两笔互不相干的并发重跑：暂存期间另一笔抢先发布，恢复后分区合并、两笔更正都生效', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const baseInput = path.join(root, 'base-input');
  const inputA = path.join(root, 'input-a');
  const inputB = path.join(root, 'input-b');
  await writeBaseline(baseInput);
  await writeInputA(inputA);
  await writeInputB(inputB);

  const first = await runFull(baseInput, data);
  assert.equal(first.skipped, false);
  const baselineId = (await loadCurrentPublication(data))!.manifest.publicationId;

  // A 笔暂停在提交前；暂停钩子内让 B 笔先完成发布
  const outA = await rerunDay(inputA, data, '2026-11-03', {
    pause: {
      statusFile: path.join(root, 'pause-a.json'),
      waitForResume: async () => {
        const outB = await rerunDay(inputB, data, '2026-11-05');
        assert.equal(outB.skipped, false);
        // B 发布后 A 的更正尚不在当前发布中
        const mid = (await loadCurrentPublication(data))!;
        assert.equal(
          mid.accounts.find((a) => a.connectionId === 'CONN-X')?.usageWh,
          2000,
        );
        assert.equal(
          mid.accounts.find((a) => a.connectionId === 'CONN-Y')?.usageWh,
          3000,
        );
        return 'resume';
      },
    },
  });
  assert.equal(outA.skipped, false);

  const merged = (await loadCurrentPublication(data))!;
  // 两笔更正同时生效
  assert.equal(merged.accounts.find((a) => a.connectionId === 'CONN-X')?.usageWh, 2100);
  assert.equal(merged.accounts.find((a) => a.connectionId === 'CONN-Y')?.usageWh, 3000);
  assert.equal(merged.quarantines.length, 0);

  // 各自血缘保留，且引用各自更正文件与修订
  const linX = merged.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-X-2026-11-03');
  const linY = merged.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-Y-2026-11-05');
  assert.ok(JSON.stringify(linX).includes('a-correction.jsonl'));
  assert.ok(JSON.stringify(linY).includes('b-correction.jsonl'));
  assert.ok(JSON.stringify(linX).includes('"revision":2'));

  // 撤下墓碑并集：B 笔撤下的 QUAR-CONN-Y 在合并发布中仍在
  assert.ok(merged.manifest.withdrawnDocIds.includes('QUAR-CONN-Y'));
  // 输入文件清单为两笔并集
  const files = merged.manifest.inputFiles.map((f) => f.file);
  assert.ok(files.includes('readings/a-correction.jsonl'));
  assert.ok(files.includes('readings/b-correction.jsonl'));
  // 合并审计告警
  assert.ok(merged.manifest.warnings.some((w) => w.includes('分区合并')));

  // 历史发布：基线 / B 笔 / 合并，共 3 个；A 的暂存发布 id 从未物化
  const ids = await listPublicationIds(data);
  assert.equal(ids.length, 3);
  assert.ok(ids.includes(baselineId));
  assert.ok(!ids.includes(outA.stagingToken ?? ''));
  // 暂存已清理
  assert.deepEqual(await listPendingStaging(data), []);
});

test('提交前进程“崩溃”：暂存保留、指针不变，resume 后正常发布且合并并发更正', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const inputA = path.join(root, 'input-a');
  const inputB = path.join(root, 'input-b');
  await writeInputA(inputA);
  await writeInputB(inputB);
  const baseInput = path.join(root, 'base-input');
  await writeBaseline(baseInput);
  const baseOut = await runFull(baseInput, data);
  assert.equal(baseOut.skipped, false);
  const beforeId = (await loadCurrentPublication(data))!.manifest.publicationId;

  // 暂停钩子抛错模拟进程在提交前死亡
  await assert.rejects(
    rerunDay(inputA, data, '2026-11-03', {
      pause: {
        statusFile: path.join(root, 'pause.json'),
        waitForResume: async () => {
          throw new Error('simulated process crash before commit');
        },
      },
    }),
    /simulated process crash/,
  );

  // 指针未动；暂存仍在且对查询不可见
  assert.equal((await loadCurrentPublication(data))!.manifest.publicationId, beforeId);
  const pending = await listPendingStaging(data);
  assert.equal(pending.length, 1);
  const staged = pending[0]!;
  assert.equal(await loadPublication(data, staged.publicationId), null);
  assert.ok(!(await listPublicationIds(data)).includes(staged.publicationId));

  // 暂存期间 B 笔照常发布
  const outB = await rerunDay(inputB, data, '2026-11-05');
  assert.equal(outB.skipped, false);

  // 恢复 A：与 B 的发布分区合并
  const recovered = await resumePublication(data, staged.token);
  assert.equal(recovered.skipped, false);
  const cur = (await loadCurrentPublication(data))!;
  assert.equal(cur.accounts.find((a) => a.connectionId === 'CONN-X')?.usageWh, 2100);
  assert.equal(cur.accounts.find((a) => a.connectionId === 'CONN-Y')?.usageWh, 3000);
  assert.ok(cur.manifest.withdrawnDocIds.includes('QUAR-CONN-Y'));
  assert.deepEqual(await listPendingStaging(data), []);
});

test('僵死锁自动接管：持有者进程不存在时后来者可继续发布', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await writeBaseline(input);
  await runFull(input, data);

  // 伪造一个属于已死进程的锁
  const lockFile = path.join(data, 'publications', 'pipeline.lock');
  await writeFile(
    lockFile,
    JSON.stringify({ token: 'dead-owner', pid: 999999, acquiredAtMs: Date.now(), mode: 'rerun' }),
  );
  const lock = await acquirePublicationLock(data, { mode: 'rerun', timeoutMs: 5_000 });
  await lock.release();

  // 存活持有者的锁不会被抢占，超时抛错
  await writeFile(
    lockFile,
    JSON.stringify({ token: 'live-owner', pid: process.pid, acquiredAtMs: Date.now(), mode: 'rerun' }),
  );
  await assert.rejects(
    acquirePublicationLock(data, { mode: 'full', timeoutMs: 300 }),
    /等待发布锁超时/,
  );
  await rm(lockFile, { force: true });
});

test('取消暂存：指针不变、暂存清除，随后 resume 报不存在', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const inputA = path.join(root, 'input-a');
  await writeInputA(inputA);
  const baseInput = path.join(root, 'base-input');
  await writeBaseline(baseInput);
  await runFull(baseInput, data);
  const beforeId = (await loadCurrentPublication(data))!.manifest.publicationId;

  await assert.rejects(
    rerunDay(inputA, data, '2026-11-03', {
      pause: {
        statusFile: path.join(root, 'pause.json'),
        waitForResume: async () => 'cancel',
      },
    }),
    /已被取消/,
  );
  // 取消即清理暂存，指针保持不变
  assert.deepEqual(await listPendingStaging(data), []);
  assert.equal((await loadCurrentPublication(data))!.manifest.publicationId, beforeId);
  await assert.rejects(resumePublication(data, 'nonexistent-token'), /不存在/);
});

test('暂存目录不污染发布存储：只存在 .staging 与正式 pub- 目录', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const inputA = path.join(root, 'input-a');
  await writeInputA(inputA);
  const baseInput = path.join(root, 'base-input');
  await writeBaseline(baseInput);
  await runFull(baseInput, data);

  await assert.rejects(
    rerunDay(inputA, data, '2026-11-03', {
      pause: {
        statusFile: path.join(root, 'pause.json'),
        waitForResume: async () => {
          throw new Error('leave staging behind');
        },
      },
    }),
    /leave staging behind/,
  );

  const entries = await readdir(path.join(data, 'publications'));
  const pubs = entries.filter((e) => e.startsWith('pub-'));
  assert.ok(pubs.every((e) => !e.includes('.tmp-')));
  assert.ok(entries.includes('.staging'));
  assert.ok(entries.includes('current.json'));
  const stagingEntries = await readdir(path.join(data, 'publications', '.staging'));
  assert.ok(stagingEntries.some((e) => e.endsWith('.json')));
  assert.ok(stagingEntries.some((e) => e.endsWith('.dir')));
});

test('提交在“目录物化后、指针切换前”崩溃：resume 可重入，重复 resume 安全', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const baseInput = path.join(root, 'base-input');
  const inputA = path.join(root, 'input-a');
  await writeBaseline(baseInput);
  await writeInputA(inputA);
  await runFull(baseInput, data);

  // 制造一个中断暂存（钩子抛错，进程在提交前死亡，暂存已持久化）
  await assert.rejects(
    rerunDay(inputA, data, '2026-11-03', {
      pause: {
        statusFile: path.join(root, 'pause.json'),
        waitForResume: async () => {
          throw new Error('crash before commit');
        },
      },
    }),
    /crash before commit/,
  );
  const [staged] = await listPendingStaging(data);
  assert.ok(staged);

  // 模拟“正式目录已 rename、指针尚未切换”的崩溃现场
  const finalDir = path.join(data, 'publications', staged!.publicationId);
  const bundleDir = path.join(data, 'publications', '.staging', `${staged!.token}.dir`);
  await cp(bundleDir, finalDir, { recursive: true });

  // 第一次 resume：复用已物化目录，仅切换指针
  const resumed = await resumePublication(data, staged!.token);
  assert.equal(resumed.skipped, false);
  assert.equal((await loadCurrentPublication(data))!.manifest.publicationId, staged!.publicationId);
  assert.deepEqual(await listPendingStaging(data), []);

  // 第二次 resume（暂存已被清理）应明确报不存在，而不是产生重复发布
  await assert.rejects(resumePublication(data, staged!.token), /不存在/);
  assert.equal((await listPublicationIds(data)).length, 2);
});

test('prepareStaging 同 token 重入整体替换，不留旧 bundle 残留', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const baseInput = path.join(root, 'base-input');
  await writeBaseline(baseInput);
  await runFull(baseInput, data);
  const current = (await loadCurrentPublication(data))!;

  const state: StagingState = {
    token: 'stage-fixed-token',
    publicationId: 'pub-rerun-test-v1',
    basePublicationId: current.manifest.publicationId,
    createdAtMs: Date.now(),
    fingerprint: current.manifest.fingerprint,
    withdrawnDocIds: [],
    plan: {
      mode: 'rerun',
      rerunDay: '2026-11-03',
      recomputedConnectionIds: ['CONN-X'],
      carriedConnectionIds: [],
      warnings: [],
      inputFiles: [],
    },
    bundle: {
      manifest: { ...current.manifest, publicationId: 'pub-rerun-test-v1', fingerprint: 'fp-v1' },
      accounts: current.accounts,
      quarantines: current.quarantines,
      lineage: current.lineage,
    },
  };
  await prepareStaging(data, state);
  const state2: StagingState = {
    ...state,
    publicationId: 'pub-rerun-test-v2',
    fingerprint: 'fp-v2',
    bundle: { ...state.bundle, manifest: { ...state.bundle.manifest, publicationId: 'pub-rerun-test-v2', fingerprint: 'fp-v2' } },
  };
  await prepareStaging(data, state2);
  const readBack = await listPendingStaging(data);
  assert.equal(readBack.length, 1);
  assert.equal(readBack[0]!.publicationId, 'pub-rerun-test-v2');
  const entries = await readdir(path.join(data, 'publications', '.staging'));
  // 只应有一个 .json 与一个 .dir（外加可能的临时项被清理），不存在 v1 残留目录
  assert.ok(!entries.includes('stage-fixed-token-v1'));
  assert.deepEqual(entries.filter((e) => e.startsWith('stage-fixed-token')).sort(), [
    'stage-fixed-token.dir',
    'stage-fixed-token.json',
  ]);
});
