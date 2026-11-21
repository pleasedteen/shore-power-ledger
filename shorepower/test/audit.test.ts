import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runFull, rerunDay } from '../src/pipeline.js';
import { commitPublication, loadCurrentPublication, readPointer } from '../src/storage.js';
import { readAttemptAudit, startAttempt } from '../src/audit.js';
import { startHttpServer } from '../src/server.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const run1 = path.join(here, '..', 'fixtures', 'run1');
const run2 = path.join(here, '..', 'fixtures', 'run2');

async function freshDirs(): Promise<{ input: string; data: string; root: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), 'audit-test-'));
  const input = path.join(root, 'input');
  const data = path.join(root, 'data');
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(data, { recursive: true });
  return { root, input, data };
}

async function copyStage(src: string, input: string): Promise<void> {
  await cp(path.join(src, 'connections'), path.join(input, 'connections'), { recursive: true });
  await cp(path.join(src, 'readings'), path.join(input, 'readings'), { recursive: true });
}

/** 构造一个可被后续重跑读取的最小发布，并写入指针（模拟发布切换） */
async function commitSyntheticPublicaton(
  dataDir: string,
  publicationId: string,
  attemptId: string,
  tombstoneDocIds: string[],
): Promise<void> {
  const base = (await loadCurrentPublication(dataDir))!;
  const manifest = {
    ...base.manifest,
    publicationId,
    attemptId,
    createdAtMs: Date.now(),
    fingerprint: 'synthetic-' + publicationId,
  };
  const stamp = <T>(v: T): T => {
    if (Array.isArray(v)) return v.map(stamp) as T;
    if (v !== null && typeof v === 'object') {
      const out: Record<string, unknown> = {};
      for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
        out[k] = k === 'publicationId' ? publicationId : stamp(val);
      }
      return out as T;
    }
    return v;
  };
  await commitPublication(
    dataDir,
    {
      manifest,
      accounts: stamp(base.accounts),
      quarantines: stamp(base.quarantines),
      lineage: stamp(base.lineage),
    },
    tombstoneDocIds,
  );
}

test('每次运行分配尝试编号；审计记录基线、结果与纳入/沿用原因', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  assert.ok(first.attemptId);
  const a1 = (await readAttemptAudit(data, first.attemptId))!;
  assert.equal(a1.status, 'published');
  assert.equal(a1.baseline, null);
  assert.equal(a1.result?.publicationId, first.publicationId);
  assert.ok(a1.decisions.every((d) => d.action === 'recomputed'));

  await copyStage(run2, input);
  const second = await rerunDay(input, data, '2026-11-04');
  const a2 = (await readAttemptAudit(data, second.attemptId))!;
  assert.equal(a2.status, 'published');
  assert.equal(a2.baseline?.publicationId, first.publicationId);
  assert.equal(a2.rerunDay, '2026-11-04');
  const byId = new Map(a2.decisions.map((d) => [d.connectionId, d]));
  // 纳入：CONN-101 最新窗口与 11-04 重叠
  assert.equal(byId.get('CONN-101')?.action, 'recomputed');
  assert.ok(byId.get('CONN-101')?.reason.includes('重叠'));
  // 纳入依据来自上一版（CONN-107 在上一版为隔离，窗口与 11-04 重叠）
  assert.equal(byId.get('CONN-107')?.action, 'recomputed');
  assert.ok(byId.get('CONN-107')?.basis?.includes('latest'));
  // 沿用：CONN-102 为 11-03 单日
  assert.equal(byId.get('CONN-102')?.action, 'carried');
  assert.ok(byId.get('CONN-102')?.reason.includes('不重叠'));
  // 撤下：CONN-107 旧隔离文档
  assert.deepEqual(a2.result?.withdrawnDocIds, ['QUAR-CONN-107']);
});

test('重复重跑被记录为幂等跳过（与已发布区分）', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  await runFull(input, data);
  await copyStage(run2, input);
  const second = await rerunDay(input, data, '2026-11-04');
  const third = await rerunDay(input, data, '2026-11-04');

  assert.notEqual(third.attemptId, second.attemptId);
  const a3 = (await readAttemptAudit(data, third.attemptId))!;
  assert.equal(a3.status, 'skipped');
  assert.equal(a3.statusLabel, '幂等跳过');
  assert.equal(a3.result?.skipped, true);
  assert.equal(a3.result?.publicationId, second.publicationId);

  // 已发布的尝试仍为已发布
  const a2 = (await readAttemptAudit(data, second.attemptId))!;
  assert.equal(a2.status, 'published');
});

test('发布切换前退出：判定未完成，后续运行对账后仍为未完成', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);

  // 模拟进程在发布切换前退出：已写 pending 与 publishing 状态，但未切换指针
  const crashed = await startAttempt(data, {
    mode: 'rerun',
    rerunDay: '2026-11-04',
    baseline: { publicationId: first.publicationId },
  });
  await crashed.markPublishing('pub-rerun-crashed-before-switch', {
    accountCount: 0,
    quarantineCount: 0,
    withdrawnDocIds: [],
  });
  // 进程此刻“死亡”：不调用 commitPublication / markPublished

  const before = (await readAttemptAudit(data, crashed.attemptId))!;
  assert.equal(before.status, 'incomplete');
  assert.equal(before.statusLabel, '未完成');
  assert.equal(before.pendingPublicationId, 'pub-rerun-crashed-before-switch');
  assert.equal(before.result, null);
  // 指针仍指向首次发布
  assert.equal((await readPointer(data))?.publicationId, first.publicationId);

  // 后续运行：先对账（固化为未完成），再正常发布新版本
  await copyStage(run2, input);
  const next = await rerunDay(input, data, '2026-11-04');
  assert.equal(next.skipped, false);

  const afterRun = (await readAttemptAudit(data, crashed.attemptId))!;
  assert.equal(afterRun.status, 'incomplete');
  assert.ok(afterRun.note?.includes('发布切换前退出'));
});

test('发布切换后退出：判定已发布，后续发布新版本后旧尝试判定不漂移', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);

  // 模拟进程在指针切换后、审计终态落盘前退出：走真实的 commitPublication，
  // 指针已指向本尝试，但从未调用 markPublished。
  const crashed = await startAttempt(data, {
    mode: 'rerun',
    rerunDay: '2026-11-04',
    baseline: { publicationId: first.publicationId },
  });
  const crashedPub = 'pub-rerun-crashed-after-switch';
  await crashed.markPublishing(crashedPub, {
    accountCount: 0,
    quarantineCount: 0,
    withdrawnDocIds: [],
  });
  await commitSyntheticPublicaton(data, crashedPub, crashed.attemptId, []);

  // 指针确实已切换，尝试本身仍处于 publishing 阶段
  const ptr = await readPointer(data);
  assert.equal(ptr?.attemptId, crashed.attemptId);
  assert.equal(ptr?.publicationId, crashedPub);

  // 查询即按指针判定为已发布
  const before = (await readAttemptAudit(data, crashed.attemptId))!;
  assert.equal(before.status, 'published');
  assert.ok(before.note?.includes('指针'));

  // 后续发布新版本：对账固化旧尝试为已发布，且不随后续发布漂移
  await copyStage(run2, input);
  const next = await rerunDay(input, data, '2026-11-04');
  assert.equal(next.skipped, false);
  assert.notEqual(next.publicationId, crashedPub);
  assert.equal((await readPointer(data))?.publicationId, next.publicationId);

  const afterRun = (await readAttemptAudit(data, crashed.attemptId))!;
  assert.equal(afterRun.status, 'published');
  assert.equal(afterRun.result?.publicationId, crashedPub);
});

test('审计查询只读该尝试元数据：不依赖历史发布目录', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);

  // 把尝试文件复制到一个只有 audit 目录的空数据目录：仍应能查到完整审计
  const bare = path.join(root, 'bare');
  await mkdir(path.join(bare, 'audit', 'attempts'), { recursive: true });
  const raw = await readFile(
    path.join(data, 'audit', 'attempts', `${first.attemptId}.json`),
    'utf-8',
  );
  await writeFile(path.join(bare, 'audit', 'attempts', `${first.attemptId}.json`), raw);

  const audit = (await readAttemptAudit(bare, first.attemptId))!;
  assert.equal(audit.status, 'published');
  assert.equal(audit.result?.publicationId, first.publicationId);
  assert.ok(!(await readPointer(bare)));
});

test('HTTP /audit/attempt 按编号返回该次审计', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  await copyStage(run2, input);
  const second = await rerunDay(input, data, '2026-11-04');

  const handle = await startHttpServer(data, 0);
  after(() => handle.close());
  const base = `http://localhost:${handle.port}`;

  const ok = (await (await fetch(`${base}/audit/attempt?attemptId=${second.attemptId}`)).json()) as any;
  assert.equal(ok.status, 'published');
  assert.equal(ok.baseline.publicationId, first.publicationId);
  assert.ok(ok.decisions.length > 0);

  const missingAttempt = (await fetch(`${base}/audit/attempt`)).status;
  assert.equal(missingAttempt, 400);
  const notFound = (await fetch(`${base}/audit/attempt?attemptId=att-nope`)).status;
  assert.equal(notFound, 404);
});
