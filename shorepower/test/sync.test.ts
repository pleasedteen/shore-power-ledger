import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { cp, mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runFull, rerunDay } from '../src/pipeline.js';
import { encodeGb18030 } from '../src/encoding.js';
import {
  loadCurrentPublication,
  loadPublication,
  type PublicationBundle,
} from '../src/storage.js';
import {
  buildPublicationDelta,
  materializeSyncedPublication,
  type PublicationDelta,
} from '../src/sync.js';
import { startHttpServer, type HttpServerHandle } from '../src/server.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const run1 = path.join(here, '..', 'fixtures', 'run1');
const run2 = path.join(here, '..', 'fixtures', 'run2');
const projectRoot = path.join(here, '..');

async function freshDirs(): Promise<{ input: string; data: string; root: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), 'sync-test-'));
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

/** 下游物化后按目标版重新读取，三类文档与血缘必须与目标版逐字节一致 */
function assertBundleEqual(actual: PublicationBundle, expected: PublicationBundle): void {
  // publicationId 在物化时统一盖成目标发布号，比较前先归一化文档顺序
  const sortById = (a: { docId: string }, b: { docId: string }) => a.docId.localeCompare(b.docId);
  assert.deepEqual(
    actual.accounts.sort(sortById).map((d) => ({ ...d, publicationId: '' })),
    expected.accounts.sort(sortById).map((d) => ({ ...d, publicationId: '' })),
  );
  assert.deepEqual(
    actual.quarantines.sort(sortById).map((d) => ({ ...d, publicationId: '' })),
    expected.quarantines.sort(sortById).map((d) => ({ ...d, publicationId: '' })),
  );
  assert.deepEqual(
    actual.lineage.sort(sortById).map((d) => ({ ...d, publicationId: '' })),
    expected.lineage.sort(sortById).map((d) => ({ ...d, publicationId: '' })),
  );
  assert.equal(actual.manifest.fingerprint, expected.manifest.fingerprint);
}

test('两版真实发布间净差量：只含新增/替换/删除，沿用文档不重传', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  await copyStage(run2, input);
  await rerunDay(input, data, '2026-11-04');

  const pub1 = (await loadPublication(data, first.publicationId))!;
  const pub2 = (await loadCurrentPublication(data))!;
  const delta = buildPublicationDelta(pub1, pub2);

  assert.equal(delta.fromPublicationId, pub1.manifest.publicationId);
  assert.equal(delta.toPublicationId, pub2.manifest.publicationId);

  // CONN-107 隔离转日账：旧隔离删除、新日账与对应血缘新增
  assert.deepEqual(delta.quarantines.removed, ['QUAR-CONN-107']);
  assert.ok(delta.accounts.added.some((d) => d.docId === 'DAILY-CONN-107-2026-11-03'));
  assert.ok(delta.accounts.added.some((d) => d.docId === 'DAILY-CONN-107-2026-11-04'));

  // CONN-101 起始读数修订：11-03 日账数值变化走替换；11-04 数值不变，但该接电被重算、
  // 重送文件按路径排序成为出处先到者，出处文件名变化仍构成真实文档净变更 → 同样走替换
  assert.ok(delta.accounts.replaced.some((d) => d.docId === 'DAILY-CONN-101-2026-11-03'));
  assert.ok(delta.accounts.replaced.some((d) => d.docId === 'DAILY-CONN-101-2026-11-04'));

  // CONN-106 rev2 缩短窗口：11-04 数值替换；11-03 数值不变但随接电重算、出处更新 → 替换
  assert.ok(delta.accounts.replaced.some((d) => d.docId === 'DAILY-CONN-106-2026-11-04'));
  assert.ok(delta.accounts.replaced.some((d) => d.docId === 'DAILY-CONN-106-2026-11-03'));

  // CONN-102 未纳入重跑日、文档原样沿用：不出现在任何分组（净差量不重传未变化文档）
  const touched = [
    ...delta.accounts.added,
    ...delta.accounts.replaced,
    ...delta.quarantines.added,
    ...delta.quarantines.replaced,
  ].map((d) => d.connectionId);
  assert.ok(!touched.includes('CONN-102'));
  assert.ok(!delta.accounts.removed.includes('DAILY-CONN-102-2026-11-03'));
  assert.ok(!delta.lineage.added.some((d) => d.connectionId === 'CONN-102'));
  assert.ok(!delta.lineage.replaced.some((d) => d.connectionId === 'CONN-102'));

  // 净差量规模不会超过整版重传
  assert.ok(delta.stats.deltaRows > 0);
  assert.ok(delta.stats.deltaRows <= delta.stats.fullRows);
});

test('仅发布号不同的同内容发布：差量为零净变更；同版自比也为零', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  const pub1 = (await loadPublication(data, first.publicationId))!;

  // 同版自比：零差量
  const self = buildPublicationDelta(pub1, pub1);
  assert.equal(self.stats.deltaRows, 0);

  // 同内容、仅发布 id 不同的“发布”（模拟不同发布号承载同一业务快照）
  const clone: PublicationBundle = {
    ...pub1,
    manifest: { ...pub1.manifest, publicationId: 'pub-full-cloned-000000' },
    accounts: pub1.accounts.map((d) => ({ ...d, publicationId: 'pub-full-cloned-000000' })),
    quarantines: pub1.quarantines.map((d) => ({ ...d, publicationId: 'pub-full-cloned-000000' })),
    lineage: pub1.lineage.map((d) => ({ ...d, publicationId: 'pub-full-cloned-000000' })),
  };
  const delta = buildPublicationDelta(pub1, clone);
  assert.equal(delta.stats.deltaRows, 0);
  assert.deepEqual(delta.accounts, { added: [], replaced: [], removed: [] });
  assert.deepEqual(delta.quarantines, { added: [], replaced: [], removed: [] });
  assert.deepEqual(delta.lineage, { added: [], replaced: [], removed: [] });
});

test('中间改过又恢复原内容：最终两版比较不重传该行', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  const pub1 = (await loadPublication(data, first.publicationId))!;

  // 构造“中间态”：篡改一条日账，再构造“恢复态”：内容与 pub1 完全一致（仅发布号不同）
  const mid: PublicationBundle = structuredClone(pub1);
  mid.manifest = { ...mid.manifest, publicationId: 'pub-rerun-mid-000001' };
  const target = mid.accounts.find((d) => d.docId === 'DAILY-CONN-101-2026-11-03')!;
  target.usageWh += 500;
  target.usageKwh = Math.round((target.usageWh / 1000) * 1e6) / 1e6;

  const restored: PublicationBundle = structuredClone(pub1);
  restored.manifest = { ...restored.manifest, publicationId: 'pub-rerun-restored-000002' };

  // pub1 -> mid：该行应作为替换
  const d1 = buildPublicationDelta(pub1, mid);
  assert.ok(d1.accounts.replaced.some((d) => d.docId === 'DAILY-CONN-101-2026-11-03'));

  // pub1 -> restored（中间改过又恢复）：零净变更，不重传
  const d2 = buildPublicationDelta(pub1, restored);
  assert.equal(d2.stats.deltaRows, 0);
});

test('下游应用差量后三类文档与血缘和目标版一致，重启可复现', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  await copyStage(run2, input);
  await rerunDay(input, data, '2026-11-04');
  const pub1 = (await loadPublication(data, first.publicationId))!;
  const pub2 = (await loadCurrentPublication(data))!;

  // 独立下游数据目录：先引导，再增量
  const downstream = await mkdtemp(path.join(os.tmpdir(), 'sync-down-'));
  after(() => rm(downstream, { recursive: true, force: true }));

  const bootstrap = buildPublicationDelta(null, pub1);
  assert.equal(bootstrap.fromPublicationId, null);
  assert.equal(bootstrap.accounts.added.length, pub1.accounts.length);
  await materializeSyncedPublication(downstream, bootstrap, null);

  const local1 = (await loadCurrentPublication(downstream))!;
  assertBundleEqual(local1, pub1);

  // 增量差量应用
  const delta = buildPublicationDelta(pub1, pub2);
  await materializeSyncedPublication(downstream, delta, local1);
  const local2 = (await loadCurrentPublication(downstream))!;
  assertBundleEqual(local2, pub2);
  assert.equal(local2.manifest.publicationId, pub2.manifest.publicationId);

  // 模拟重启：不落内存，直接从磁盘重读，结果仍与目标版一致
  const afterRestart = (await loadCurrentPublication(downstream))!;
  assertBundleEqual(afterRestart, pub2);

  // 重复拉取同一差量：目标目录已存在，幂等不报错
  const again = await materializeSyncedPublication(downstream, delta, local1);
  assert.equal(again.alreadyPresent, true);
});

test('基线不匹配与差量前置条件不满足时拒绝应用，指针不动', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  const pub1 = (await loadPublication(data, first.publicationId))!;

  const downstream = await mkdtemp(path.join(os.tmpdir(), 'sync-down-'));
  after(() => rm(downstream, { recursive: true, force: true }));

  // 非空基线不匹配
  const delta = buildPublicationDelta(pub1, pub1);
  const fakeBase = structuredClone(pub1);
  fakeBase.manifest = { ...fakeBase.manifest, publicationId: 'pub-other-999999' };
  await assert.rejects(
    () => materializeSyncedPublication(downstream, delta, fakeBase),
    /差量基线不匹配/,
  );

  // 引导差量不能应用到已有发布的目录：传入本地当前发布即触发基线冲突
  const bootstrap = buildPublicationDelta(null, pub1);
  await materializeSyncedPublication(downstream, bootstrap, null);
  const localBefore = (await loadCurrentPublication(downstream))!;
  await assert.rejects(
    () => materializeSyncedPublication(downstream, bootstrap, localBefore),
    /引导差量只能应用于尚无发布的下游/,
  );

  // 篡改差量：要求删除本地不存在的文档 → 拒绝且指针不变
  const tampered: PublicationDelta = structuredClone(buildPublicationDelta(pub1, pub1));
  tampered.fromPublicationId = pub1.manifest.publicationId;
  tampered.accounts.removed.push('DAILY-CONN-999-2026-11-03');
  const local = (await loadCurrentPublication(downstream))!;
  await assert.rejects(
    () => materializeSyncedPublication(downstream, tampered, local),
    /删除文档/,
  );
  const still = (await loadCurrentPublication(downstream))!;
  assert.equal(still.manifest.publicationId, pub1.manifest.publicationId);
});

test('HTTP /sync：导出净差量，未提交发布号拒绝，旧版查询照旧', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  await copyStage(run2, input);
  await rerunDay(input, data, '2026-11-04');
  const pub2 = (await loadCurrentPublication(data))!;

  const handle: HttpServerHandle = await startHttpServer(data, 0);
  after(() => handle.close());
  const base = `http://localhost:${handle.port}`;
  const getJson = async (p: string) => {
    const resp = await fetch(`${base}${p}`);
    return { status: resp.status, body: (await resp.json()) as any };
  };

  const ok = await getJson(`/sync?from=${first.publicationId}&to=${pub2.manifest.publicationId}`);
  assert.equal(ok.status, 200);
  assert.equal(ok.body.toPublicationId, pub2.manifest.publicationId);
  assert.ok(Array.isArray(ok.body.accounts.replaced));

  // 省略 from：引导同步，目标版全部计入 added
  const bootstrap = await getJson(`/sync?to=${first.publicationId}`);
  assert.equal(bootstrap.status, 200);
  assert.equal(bootstrap.body.fromPublicationId, null);
  assert.equal(bootstrap.body.accounts.added.length, 5);

  // 未提交/不存在的发布号：入口不接
  const badTo = await getJson('/sync?to=pub-never-committed-000000');
  assert.equal(badTo.status, 404);
  assert.match(badTo.body.error, /不存在或尚未提交/);
  const badFrom = await getJson(`/sync?from=pub-ghost-111111&to=${pub2.manifest.publicationId}`);
  assert.equal(badFrom.status, 404);

  // 缺少 to：400
  const missing = await getJson(`/sync?from=${first.publicationId}`);
  assert.equal(missing.status, 400);

  // 旧版查询照旧：历史发布快照仍可直接查
  const oldAccounts = await getJson(`/accounts?publicationId=${first.publicationId}&day=2026-11-04`);
  assert.equal(oldAccounts.status, 200);
  assert.equal(oldAccounts.body.snapshot.publicationId, first.publicationId);
});

test('几十版大量改撤场景：两端净差量低于整版 5%，同步资料磁盘占用 ≤2.5 倍', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'sync-scale-'));
  after(() => rm(root, { recursive: true, force: true }));
  const input = path.join(root, 'input');
  const data = path.join(root, 'upstream');
  const downstream = path.join(root, 'downstream');
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(data, { recursive: true });
  await mkdir(downstream, { recursive: true });

  // 200 个接电：仅 2 个（idx=7,11）的窗口落在重跑日 2026-11-03，其余 198 个在 11-02 单日。
  // 24 个版本反复重跑 11-03，只翻动这 2 个接电（改了又撤、撤了又改），
  // 模拟“几十版大量改撤、两端只差少量记录”。
  const N = 200;
  const VERSIONS = 24;
  const CHURN = [7, 11];
  const csvRows: string[] = [];
  const jsonlRows: string[] = [];
  for (let i = 0; i < N; i++) {
    const id = `CONN-S${String(i).padStart(3, '0')}`;
    const churned = CHURN.includes(i);
    const day = churned ? '2026-11-03' : '2026-11-02';
    csvRows.push(`${id},1,测试船${i},M${i},${day} 08:00:00,${day} 18:00:00`);
    const sampleDay = churned ? '2026-11-03' : '2026-11-02';
    jsonlRows.push(JSON.stringify({
      reading_id: `R${i}-A`, revision: 1, meter_id: `M${i}`,
      sampled_at_utc: `${sampleDay}T00:00:00Z`, unit: 'Wh', cumulative: '0',
    }));
    jsonlRows.push(JSON.stringify({
      reading_id: `R${i}-B`, revision: 1, meter_id: `M${i}`,
      sampled_at_utc: `${sampleDay}T10:00:00Z`, unit: 'Wh', cumulative: '10000',
    }));
  }
  await writeFile(
    path.join(input, 'connections', 'base.csv'),
    encodeGb18030(['connection_id,revision,vessel,meter_id,start_beijing,end_beijing', ...csvRows].join('\n') + '\n'),
  );
  await writeFile(path.join(input, 'readings', 'base.jsonl'), jsonlRows.join('\n') + '\n', 'utf-8');

  const first = await runFull(input, data);
  const firstBundle = (await loadPublication(data, first.publicationId))!;

  // 下游先引导
  await materializeSyncedPublication(downstream, buildPublicationDelta(null, firstBundle), null);

  let previous = firstBundle;
  let stepDeltaRows = 0;
  const wireSizes: number[] = [Buffer.byteLength(JSON.stringify(buildPublicationDelta(null, firstBundle)))];

  for (let v = 1; v <= VERSIONS; v++) {
    // 两个接电在“原值/改动值”之间来回翻转：每版都有真实内容变化（产生新发布），
    // 但大部分翻转在后续版本被撤回。
    const patchRows: string[] = [];
    for (const idx of CHURN) {
      const flipped = v % 2 === 1; // 奇数版改、偶数版恢复
      patchRows.push(JSON.stringify({
        reading_id: `R${idx}-B`, revision: v + 1, meter_id: `M${idx}`,
        sampled_at_utc: '2026-11-03T10:00:00Z', unit: 'Wh',
        cumulative: flipped ? '12000' : '10000',
      }));
    }
    await writeFile(
      path.join(input, 'readings', `patch-${String(v).padStart(2, '0')}.jsonl`),
      patchRows.join('\n') + '\n',
      'utf-8',
    );
    const outcome = await rerunDay(input, data, '2026-11-03');
    assert.equal(outcome.skipped, false, '每版翻转均有真实内容变化，应产生新发布');
    const current = (await loadCurrentPublication(data))!;
    const step = buildPublicationDelta(previous, current);
    stepDeltaRows += step.stats.deltaRows;
    wireSizes.push(Buffer.byteLength(JSON.stringify(step)));

    // 逐版应用到下游
    const local = (await loadCurrentPublication(downstream))!;
    await materializeSyncedPublication(downstream, step, local);
    previous = current;
  }

  const target = previous;
  const localFinal = (await loadCurrentPublication(downstream))!;
  assertBundleEqual(localFinal, target);

  // 端到端口径：首版 -> 末版的净差量（中间改撤已相互抵消，只剩最终仍处于改动态的少量记录）
  const endToEnd = buildPublicationDelta(firstBundle, target);
  const fullRowCount = target.accounts.length + target.quarantines.length + target.lineage.length;
  assert.ok(
    endToEnd.stats.deltaRows < fullRowCount * 0.05,
    `端到端差量 ${endToEnd.stats.deltaRows} 行未低于整版 ${fullRowCount} 行的 5%`,
  );

  // 重启后重读：导出读入明细行数与检查文档键数均低于整版行数的 5%（键数与目标版一致、无重影）
  const restarted = (await loadCurrentPublication(downstream))!;
  const checkedKeys =
    restarted.accounts.length + restarted.quarantines.length + restarted.lineage.length;
  assert.equal(checkedKeys, fullRowCount);
  // 重启后重新导出“下游当前 vs 目标版”应为零差量：读入明细 0 行
  assert.equal(buildPublicationDelta(restarted, target).stats.deltaRows, 0);

  // 每版改动量相近、版本数翻倍时：同步资料（引导 + 全部差量）磁盘占用 ≤ 2.5 倍整版快照
  const syncBytes = wireSizes.reduce((a, b) => a + b, 0);
  const fullBytes = Buffer.byteLength(JSON.stringify(target));
  assert.ok(
    syncBytes <= fullBytes * 2.5,
    `同步资料 ${syncBytes}B 超过 2.5 倍整版 ${fullBytes}B`,
  );
  // 逐步差量行数本身也远小于整版（每版只翻动 2 个接电）
  assert.ok(stepDeltaRows < fullRowCount * 0.5);
});

test('CLI sync-pull 端到端：上游服务导出，下游空目录引导后增量一致', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  await copyStage(run2, input);
  await rerunDay(input, data, '2026-11-04');
  const pub2 = (await loadCurrentPublication(data))!;

  const handle: HttpServerHandle = await startHttpServer(data, 0);
  after(() => handle.close());

  const downstream = await mkdtemp(path.join(os.tmpdir(), 'sync-cli-'));
  after(() => rm(downstream, { recursive: true, force: true }));

  const execFileAsync = promisify(execFile);
  // 用异步子进程：同步 execFileSync 会阻塞事件循环，导致同进程内的 HTTP 服务无法响应
  const runCli = (args: string[]) =>
    execFileAsync(
      process.execPath,
      ['--import', 'tsx', path.join(projectRoot, 'src', 'cli.ts'), ...args],
      { cwd: projectRoot },
    ).then((r) => r.stdout);

  // 引导（显式用 127.0.0.1，避免子进程环境 localhost 解析差异）
  const bootOut = await runCli(['sync-pull', '--upstream', `http://127.0.0.1:${handle.port}`, '--data', downstream, '--to', first.publicationId]);
  assert.match(bootOut, /差量同步结果/);
  // 增量
  const incOut = await runCli([
    'sync-pull', '--upstream', `http://127.0.0.1:${handle.port}`, '--data', downstream,
    '--from', first.publicationId, '--to', pub2.manifest.publicationId,
  ]);
  assert.match(incOut, new RegExp(pub2.manifest.publicationId));

  const local = (await loadCurrentPublication(downstream))!;
  assertBundleEqual(local, pub2);
});
