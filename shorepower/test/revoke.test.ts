import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { rerunDay, revokeBatch, runFull, supplement } from '../src/pipeline.js';
import { loadCurrentPublication, loadPublication } from '../src/storage.js';
import { encodeGb18030 } from '../src/encoding.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const run1 = path.join(here, '..', 'fixtures', 'run1');
const run2 = path.join(here, '..', 'fixtures', 'run2');

async function freshDirs(): Promise<{ input: string; data: string; root: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), 'revoke-test-'));
  const input = path.join(root, 'input');
  const data = path.join(root, 'data');
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await mkdir(data, { recursive: true });
  return { input, data, root };
}

async function copyStage(src: string, input: string): Promise<void> {
  await cp(path.join(src, 'connections'), path.join(input, 'connections'), { recursive: true });
  await cp(path.join(src, 'readings'), path.join(input, 'readings'), { recursive: true });
}

const csv = (rows: string[]): string =>
  ['connection_id,revision,vessel,meter_id,start_beijing,end_beijing', ...rows].join('\n') + '\n';

const readingLine = (row: {
  id: string;
  rev?: number;
  meter: string;
  utc: string;
  unit?: 'Wh' | 'kWh';
  value: string;
}): string =>
  JSON.stringify({
    reading_id: row.id,
    revision: row.rev ?? 1,
    meter_id: row.meter,
    sampled_at_utc: row.utc,
    unit: row.unit ?? 'Wh',
    cumulative: row.value,
  }) + '\n';

async function writeCsv(file: string, rows: string[]): Promise<void> {
  await writeFile(file, encodeGb18030(csv(rows)));
}

/** 输入目录里还剩哪些投递文件（用于断言发布成功后已清走） */
async function deliveredFiles(input: string): Promise<string[]> {
  const entries = await readdir(input, { withFileTypes: true, recursive: true });
  return entries
    .filter((e) => e.isFile())
    .map((e) => path.relative(input, path.join(e.parentPath ?? input, e.name)))
    .sort();
}

test('首次导入形成可撤销批次，发布成功后投递文件被清走', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const out = await runFull(input, data);
  assert.equal(out.skipped, false);
  assert.ok(out.batchId, '首次导入应登记批次');
  assert.equal(out.cleanedFiles.length, 2);
  assert.deepEqual(await deliveredFiles(input), []);

  const pub = (await loadCurrentPublication(data))!;
  const batch = pub.batches.find((b) => b.batchId === out.batchId)!;
  assert.equal(batch.kind, 'initial-import');
  assert.equal(batch.status, 'active');
  // 投递文件被清走，但批次仍保留文件指纹
  assert.equal(batch.files.length, 2);
  // 批次内的原始记录随发布落盘，撤销不再依赖输入目录
  assert.ok(pub.batchRecords.length > 0);
  assert.ok(pub.batchRecords.every((r) => r.batchId === out.batchId));
  // 血缘可查，并带批次标记
  const lin = pub.lineage.find((l) => l.subjectDocId === 'DAILY-CONN-101-2026-11-03')!;
  assert.ok(lin.connectionSources.every((s) => s.batchId === out.batchId));
});

test('撤销补发批次：按剩余有效批次回退最高修订，只重算受影响接电', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  assert.equal((await loadCurrentPublication(data))!.accounts.find(
    (a) => a.connectionId === 'CONN-101' && a.beijingDay === '2026-11-03',
  )!.usageWh, 16000);

  await copyStage(run2, input);
  const second = await supplement(input, data);
  assert.equal(second.skipped, false);
  assert.ok(second.batchId);
  const pub2 = (await loadCurrentPublication(data))!;
  assert.equal(pub2.batches.find((b) => b.batchId === second.batchId)!.status, 'active');
  assert.equal(
    pub2.accounts.find((a) => a.connectionId === 'CONN-101' && a.beijingDay === '2026-11-03')!.usageWh,
    16100,
    '补发修订生效',
  );
  assert.ok(pub2.accounts.some((a) => a.connectionId === 'CONN-107'), 'CONN-107 补发后恢复');
  assert.equal((await deliveredFiles(input)).length, 0, '补发投递文件也被清走');

  // 预览撤销：报告影响范围但不发布
  const preview = await revokeBatch(data, { batchId: second.batchId!, dryRun: true });
  assert.equal(preview.dryRun, true);
  assert.equal(preview.publicationId, null);
  assert.ok(preview.affectedConnectionIds.includes('CONN-101'));
  assert.ok(preview.affectedConnectionIds.includes('CONN-107'));
  assert.ok(preview.affectedConnectionIds.includes('CONN-106'));
  assert.equal((await loadCurrentPublication(data))!.manifest.publicationId, pub2.manifest.publicationId);

  const revoked = await revokeBatch(data, { batchId: second.batchId!, note: '补发数据有误' });
  assert.equal(revoked.skipped, false);
  const pub3 = (await loadCurrentPublication(data))!;

  // 受影响集合应精确：CONN-102/103/104/105 的窗口与读数未被该批次触及
  assert.deepEqual(revoked.recomputedConnectionIds, ['CONN-101', 'CONN-106', 'CONN-107']);
  assert.equal(pub3.manifest.mode, 'revoke');
  assert.deepEqual(pub3.manifest.revokedBatchIds, [second.batchId]);

  // 旧修订恢复：CONN-101 回到 16000，CONN-106 rev1 窗口与用量恢复
  const acc = (id: string, day: string) =>
    pub3.accounts.find((a) => a.connectionId === id && a.beijingDay === day);
  assert.equal(acc('CONN-101', '2026-11-03')!.usageWh, 16000);
  assert.equal(acc('CONN-101', '2026-11-03')!.revision, 1);
  assert.equal(acc('CONN-106', '2026-11-04')!.usageWh, 18000);
  assert.equal(acc('CONN-106', '2026-11-04')!.revision, 1);
  // CONN-107 失去补发的午夜读数，重新隔离
  assert.ok(pub3.quarantines.some((q) => q.connectionId === 'CONN-107'));
  assert.ok(!pub3.accounts.some((a) => a.connectionId === 'CONN-107'));
  // 撤下的旧文档进入墓碑，历史仍可查
  assert.ok(revoked.withdrawnDocIds.includes('QUAR-CONN-107'));
  const pub2Again = (await loadPublication(data, pub2.manifest.publicationId))!;
  assert.ok(pub2Again.accounts.some((a) => a.connectionId === 'CONN-107'), '已发布历史仍可查');

  // 未受影响接电沿用：CONN-102 文档未被重算，但血统与发布 id 指向新发布
  assert.ok(revoked.recomputedConnectionIds.every((id) => id !== 'CONN-102'));
  const conn102 = pub3.accounts.filter((a) => a.connectionId === 'CONN-102');
  assert.equal(conn102.length, 1);
  assert.equal(conn102[0]!.publicationId, pub3.manifest.publicationId);

  // 幂等：再次撤销同一批次不产生新发布
  const retry = await revokeBatch(data, { batchId: second.batchId! });
  assert.equal(retry.skipped, true);
  assert.equal(retry.publicationId, pub3.manifest.publicationId);
});

test('后来独立生效的更正不随旧批次撤销而丢失', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  // 首次导入：CONN-A 单日，读数 100 -> 200
  await writeCsv(path.join(input, 'connections', 'imp.csv'), [
    'CONN-A,1,测试轮,M1,2026-11-03 09:00:00,2026-11-03 17:00:00',
  ]);
  await writeFile(
    path.join(input, 'readings', 'imp.jsonl'),
    readingLine({ id: 'RA1', meter: 'M1', utc: '2026-11-03T01:00:00Z', value: '100' }) +
      readingLine({ id: 'RA2', meter: 'M1', utc: '2026-11-03T09:00:00Z', value: '200' }),
  );
  const first = await runFull(input, data);
  const firstBatch = first.batchId!;

  // 投递目录已被清空，补发前重新创建
  await mkdir(path.join(input, 'readings'), { recursive: true });
  // 补发批次：新增一条无关电表读数（不影响 CONN-A），形成后续可撤的旧批次
  await writeFile(
    path.join(input, 'readings', 'supp.jsonl'),
    readingLine({ id: 'RX1', meter: 'M9', utc: '2026-11-03T09:00:00Z', value: '5' }),
  );
  const supp = await supplement(input, data);
  assert.ok(supp.batchId);

  // 后来独立生效的更正：CONN-A 起点读数修订 -> 80，用量变为 120
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await writeFile(
    path.join(input, 'readings', 'corr.jsonl'),
    readingLine({ id: 'RA1', rev: 2, meter: 'M1', utc: '2026-11-03T01:00:00Z', value: '80' }),
  );
  const corr = await supplement(input, data);
  assert.ok(corr.batchId);
  assert.notEqual(corr.batchId, supp.batchId);
  assert.equal(
    (await loadCurrentPublication(data))!.accounts[0]!.usageWh,
    120,
    '独立更正已生效',
  );

  // 撤销无关的补发批次：更正必须保留
  const revoked = await revokeBatch(data, { batchId: supp.batchId! });
  const pub = (await loadCurrentPublication(data))!;
  assert.equal(pub.accounts[0]!.usageWh, 120);
  assert.equal(pub.accounts[0]!.revision, 1);
  // 更正批次仍有效，且血缘指向更正来源
  const corrBatch = pub.batches.find((b) => b.batchId === corr.batchId)!;
  assert.equal(corrBatch.status, 'active');
  assert.ok(
    pub.lineage[0]!.readingSources.some((s) => s.batchId === corr.batchId && s.recordId === 'RA1'),
  );
  // 撤销并不牵动 CONN-A（补发批次只涉及 M9）
  assert.deepEqual(revoked.recomputedConnectionIds, []);
  // 首次导入批次仍在，可继续追溯
  assert.equal(pub.batches.find((b) => b.batchId === firstBatch)!.status, 'active');
});

test('撤销首次导入批次：无剩余来源时账与隔离被撤下，历史发布仍可查', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  const firstPub = (await loadCurrentPublication(data))!;

  const revoked = await revokeBatch(data, { batchId: first.batchId! });
  assert.equal(revoked.skipped, false);
  const pub = (await loadCurrentPublication(data))!;
  assert.equal(pub.accounts.length, 0);
  assert.equal(pub.quarantines.length, 0);
  assert.equal(pub.lineage.length, 0);
  assert.ok(revoked.withdrawnDocIds.length >= 9);
  assert.ok(pub.manifest.withdrawnDocIds.includes('DAILY-CONN-101-2026-11-03'));
  // 历史发布仍可查询，血缘完整
  const old = (await loadPublication(data, firstPub.manifest.publicationId))!;
  assert.equal(old.accounts.length, 5);
  assert.equal(old.lineage.length, 9);
});

test('发布中断后重试：指针不变，重试后撤销生效', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  await runFull(input, data);
  await copyStage(run2, input);
  const supp = await supplement(input, data);
  const beforeRetry = (await loadCurrentPublication(data))!;

  await assert.rejects(
    revokeBatch(
      data,
      { batchId: supp.batchId! },
      {
        commitHooks: {
          afterStaging: () => {
            throw new Error('模拟发布中途退出');
          },
        },
      },
    ),
    /模拟发布中途退出/,
  );

  // 指针未切换：仍是撤销前的完整发布，批次状态仍为 active
  const afterCrash = (await loadCurrentPublication(data))!;
  assert.equal(afterCrash.manifest.publicationId, beforeRetry.manifest.publicationId);
  assert.equal(afterCrash.batches.find((b) => b.batchId === supp.batchId)!.status, 'active');

  // 重试成功，批次状态与账一起切换
  const retry = await revokeBatch(data, { batchId: supp.batchId! });
  assert.equal(retry.skipped, false);
  const afterRetry = (await loadCurrentPublication(data))!;
  assert.equal(afterRetry.batches.find((b) => b.batchId === supp.batchId)!.status, 'revoked');
  assert.equal(
    afterRetry.accounts.find((a) => a.connectionId === 'CONN-101' && a.beijingDay === '2026-11-03')!
      .usageWh,
    16000,
  );
});

test('撤销后仅以剩余有效批次重选修订，无需输入目录', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  await runFull(input, data);
  await copyStage(run2, input);
  const supp = await supplement(input, data);
  // 输入目录已被清空
  assert.deepEqual(await deliveredFiles(input), []);

  const result = await revokeBatch(data, { batchId: supp.batchId! });
  assert.equal(result.skipped, false);
  const pub = (await loadCurrentPublication(data))!;
  assert.equal(pub.manifest.activeBatchIds.length, 1);
  assert.equal(pub.manifest.revokedBatchIds.length, 1);
  assert.equal(
    pub.accounts.find((a) => a.connectionId === 'CONN-101' && a.beijingDay === '2026-11-03')!.usageWh,
    16000,
  );
});

test('投递文件清走后再次 run：从批次台账重建，结果不被清空', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  assert.deepEqual(await deliveredFiles(input), []);

  // 输入目录为空时再次全量运行：仍应得到完整结果（依赖落盘台账，而非投递文件）
  const again = await runFull(input, data);
  assert.equal(again.skipped, true, '相同批次状态应跳过重复发布');
  const pub = (await loadCurrentPublication(data))!;
  assert.equal(pub.accounts.length, 5);
  assert.equal(pub.quarantines.length, 4);
  assert.equal(pub.manifest.publicationId, first.publicationId);
});

test('撤销后重新投递相同文件：形成新批次，不复活已撤销批次', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  await revokeBatch(data, { batchId: first.batchId! });
  assert.equal((await loadCurrentPublication(data))!.accounts.length, 0);

  // 重新投递同一批文件：必须是新批次（id 与已撤销批次不同）并恢复日账
  await copyStage(run1, input);
  const redeliver = await runFull(input, data);
  assert.equal(redeliver.skipped, false);
  assert.ok(redeliver.batchId);
  assert.notEqual(redeliver.batchId, first.batchId);
  const pub = (await loadCurrentPublication(data))!;
  assert.equal(pub.accounts.length, 5);
  assert.equal(pub.batches.filter((b) => b.status === 'active').length, 1);
  assert.equal(pub.batches.filter((b) => b.status === 'revoked').length, 1);
});
