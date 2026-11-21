/**
 * 乱序双源修订冲突的端到端集成测试。
 *
 * 覆盖需求：
 * 1. 同修订冲突只按最高修订判定、与文件顺序无关；等值合并；冲突整笔隔离且保留各方出处；
 * 2. 读数候选换表/换时刻只影响命中候选位置的接电；接口每笔接电只返回一条隔离；
 * 3. 高修订补发后重跑旧日可解除隔离，未涉及接电沿用；
 * 4. 旧原件移走后，按任一候选实际涉及的旧日重跑仍能撤换整笔结果；候选之间的空档不算重叠；
 * 5. 幂等重跑结果稳定。
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { cp, mkdtemp, mkdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runFull, rerunDay } from '../src/pipeline.js';
import { loadCurrentPublication } from '../src/storage.js';
import { startHttpServer, type HttpServerHandle } from '../src/server.js';
import { ingest } from '../src/ingest.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const fixtureRoot = path.join(here, '..', 'fixtures', 'conflict');

async function freshDirs(): Promise<{ input: string; data: string; root: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), 'conflict-test-'));
  const input = path.join(root, 'input');
  const data = path.join(root, 'data');
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(data, { recursive: true });
  return { root, input, data };
}

async function copyStage(stage: string, input: string): Promise<void> {
  await cp(path.join(fixtureRoot, stage, 'connections'), path.join(input, 'connections'), { recursive: true });
  await cp(path.join(fixtureRoot, stage, 'readings'), path.join(input, 'readings'), { recursive: true });
}

test('stage-a 乱序双源：同修订冲突整笔隔离、等值合并、接口每笔仅一条隔离', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage('stage-a', input);

  // 采集层：R804 两个冲突候选位置都带 revisionConflict 与双方出处；R801/R802 等值合并无冲突
  const ingested = await ingest(input);
  const r804 = ingested.readings.filter((r) => r.readingId === 'R804');
  assert.equal(r804.length, 2);
  for (const r of r804) {
    assert.equal(r.revisionConflict, true);
    const files = [...new Set(r.sources.map((s) => s.file))].sort();
    assert.deepEqual(files, [
      'readings/2026-11-05_readings_batch1.jsonl',
      'readings/2026-11-09_readings_batch2.jsonl',
    ]);
  }
  for (const id of ['R801', 'R802', 'R803']) {
    const rr = ingested.readings.filter((x) => x.readingId === id);
    assert.equal(rr.every((x) => x.revisionConflict === undefined), true, `${id} 等值来源不应判冲突`);
  }
  assert.equal(ingested.warnings.some((w) => w.includes('R804')), true);

  const out = await runFull(input, data);
  assert.equal(out.skipped, false);
  // CONN-201 隔离；CONN-202（1 条日账）、CONN-203（1 条日账）正常
  assert.equal(out.accountCount, 2);
  assert.equal(out.quarantineCount, 1);

  const pub = (await loadCurrentPublication(data))!;
  const q = pub.quarantines.find((x) => x.connectionId === 'CONN-201')!;
  assert.ok(q);
  // 同一时刻两份不一致读数同时命中时刻冲突与同修订冲突，两类原因合并到同一条隔离
  assert.deepEqual([...new Set(q.reasons)].sort(), ['CONFLICTING_READING', 'SOURCE_REVISION_CONFLICT']);
  // 整笔隔离：该接电没有任何日账
  assert.equal(pub.accounts.some((a) => a.connectionId === 'CONN-201'), false);
  // 证据保留各方出处（两个文件各一条），任一类证据都应携带双方来源
  const sourceFiles = new Set(q.evidence.flatMap((e) => (e.sources ?? []).map((s) => s.file)));
  assert.ok(sourceFiles.has('readings/2026-11-05_readings_batch1.jsonl'));
  assert.ok(sourceFiles.has('readings/2026-11-09_readings_batch2.jsonl'));
  // 血缘连接到隔离文档且包含冲突读数的双方出处
  const lin = pub.lineage.find((l) => l.subjectDocId === q.docId)!;
  assert.ok(lin);
  const linFiles = new Set(lin.readingSources.map((s) => s.file));
  assert.ok(linFiles.has('readings/2026-11-05_readings_batch1.jsonl'));
  assert.ok(linFiles.has('readings/2026-11-09_readings_batch2.jsonl'));

  // HTTP 接口：每笔接电只返回一条隔离
  const handle: HttpServerHandle = await startHttpServer(data, 0);
  try {
    const base = `http://localhost:${handle.port}`;
    const qrows = await (await fetch(`${base}/quarantines?connectionId=CONN-201`)).json() as any;
    assert.equal(qrows.count, 1);
    const connRows = await (await fetch(`${base}/connection?id=CONN-201`)).json() as any;
    assert.equal(connRows.accounts.length, 0);
    assert.notEqual(connRows.quarantine, null);
    assert.equal(connRows.lineage.length, 1);
  } finally {
    await handle.close();
  }
});

test('stage-b 补发高修订后重跑旧日：解隔；未涉及接电沿用；每笔仍只一条隔离/日账', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage('stage-a', input);
  await runFull(input, data);

  // 切换到 stage-b（追加权威补发 + CONN-203 移位），重跑 R804 候选实际涉及的旧日 11-09
  await rm(path.join(input, 'readings'), { recursive: true, force: true });
  await rm(path.join(input, 'connections'), { recursive: true, force: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await copyStage('stage-b', input);

  const rerun = await rerunDay(input, data, '2026-11-09');
  assert.equal(rerun.skipped, false);
  // 11-09 涉及 CONN-201（窗口重叠 + 旧日）与 CONN-202（窗口重叠）；CONN-203 不在此日
  assert.deepEqual(rerun.recomputedConnectionIds.sort(), ['CONN-201', 'CONN-202']);
  // CONN-203 不涉及重跑日，整笔沿用
  assert.deepEqual(rerun.carriedConnectionIds, ['CONN-203']);

  const pub = (await loadCurrentPublication(data))!;
  // CONN-201 解隔为两条日账；隔离区清空
  assert.equal(pub.quarantines.length, 0);
  const acc201 = pub.accounts.filter((a) => a.connectionId === 'CONN-201').sort((a, b) => a.beijingDay.localeCompare(b.beijingDay));
  assert.deepEqual(acc201.map((a) => a.beijingDay), ['2026-11-09', '2026-11-10']);
  // 用量：午夜 20000 - 起点 1000 = 19000；终点 26000 - 午夜 20000 = 6000
  assert.deepEqual(acc201.map((a) => a.usageWh), [19000, 6000]);
  // 高修订 rev2 进入血缘，旧 rev1 冲突不再决定结果
  const lin201 = pub.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-201-2026-11-09')!;
  assert.ok(lin201.readingSources.some((s) => s.recordId === 'R804' && s.revision === 2));

  // 幂等：相同输入再重跑，跳过发布
  const again = await rerunDay(input, data, '2026-11-09');
  assert.equal(again.skipped, true);
});

test('旧原件移走、候选换位：按候选实际涉及的旧日重跑撤换整笔；空档日不纳入', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage('stage-a', input);
  await runFull(input, data);

  // stage-b：CONN-203 rev2 窗口移到 11-12，旧 M10 读数（11-10）从输入移走，新读数落在 11-12
  await rm(path.join(input, 'readings'), { recursive: true, force: true });
  await rm(path.join(input, 'connections'), { recursive: true, force: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await copyStage('stage-b', input);

  // 重跑旧日 11-10：最新 CONN-203 窗口（11-12）与该日不重叠，
  // 但上一版 CONN-203 实际涉及 11-10 → 仍须纳入并整笔撤换旧结果
  const rerun = await rerunDay(input, data, '2026-11-10');
  assert.ok(rerun.recomputedConnectionIds.includes('CONN-201'), 'CONN-201 窗口跨 11-10 应纳入');
  assert.ok(rerun.recomputedConnectionIds.includes('CONN-203'), '旧结果实际涉及 11-10，必须纳入撤换');
  assert.ok(!rerun.recomputedConnectionIds.includes('CONN-202'));

  const pub = (await loadCurrentPublication(data))!;
  // 旧 11-10 日账已撤下；新结果只在 11-12
  const acc203 = pub.accounts.filter((a) => a.connectionId === 'CONN-203');
  assert.deepEqual(acc203.map((a) => a.beijingDay), ['2026-11-12']);
  assert.equal(acc203[0]!.revision, 2);
  assert.equal(acc203[0]!.usageWh, 7000); // 14000 - 7000
  assert.ok(rerun.withdrawnDocIds.includes('DAILY-CONN-203-2026-11-10'));
  // CONN-202 未纳入，文档沿用（docId 不变）
  assert.ok(rerun.carriedConnectionIds.includes('CONN-202'));

  // 空档日 11-11：没有任何候选/旧结果实际落在该日 → 没有接电纳入，整版沿用、幂等跳过
  const gap = await rerunDay(input, data, '2026-11-11');
  assert.deepEqual(gap.recomputedConnectionIds, []);
  assert.equal(gap.skipped, true);
  const pubAfterGap = (await loadCurrentPublication(data))!;
  assert.equal(pubAfterGap.manifest.publicationId, pub.manifest.publicationId);

  // 重跑新候选日 11-12：最新窗口重叠，正常重算且结果稳定
  const rerunNew = await rerunDay(input, data, '2026-11-12');
  assert.ok(rerunNew.recomputedConnectionIds.includes('CONN-203'));
  const pub2 = (await loadCurrentPublication(data))!;
  assert.deepEqual(
    pub2.accounts.filter((a) => a.connectionId === 'CONN-203').map((a) => [a.beijingDay, a.usageWh]),
    [['2026-11-12', 7000]],
  );
});
