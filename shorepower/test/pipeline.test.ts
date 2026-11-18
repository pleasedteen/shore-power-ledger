import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { cp, mkdtemp, mkdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runFull, rerunDay } from '../src/pipeline.js';
import { loadCurrentPublication } from '../src/storage.js';
import { startHttpServer, type HttpServerHandle } from '../src/server.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const allInOne = path.join(here, '..', 'fixtures', 'all-in-one');
const run1 = path.join(here, '..', 'fixtures', 'run1');
const run2 = path.join(here, '..', 'fixtures', 'run2');

async function freshDirs(): Promise<{ input: string; data: string; root: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), 'pipeline-test-'));
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

test('全量 -> 指定日重跑：纳入重叠接电、替换撤下、其余沿用、幂等', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));

  await copyStage(run1, input);
  const first = await runFull(input, data);
  assert.equal(first.skipped, false);
  const pub1 = (await loadCurrentPublication(data))!;
  assert.equal(pub1.accounts.length, 5);
  assert.equal(pub1.quarantines.length, 4);

  await copyStage(run2, input);
  const second = await rerunDay(input, data, '2026-11-04');
  assert.equal(second.skipped, false);
  const pub2 = (await loadCurrentPublication(data))!;

  // 纳入集合：窗口与 11-04 重叠的全部接电（含上一版被隔离的 CONN-103/104/105/107）
  assert.deepEqual(second.recomputedConnectionIds.sort(), [
    'CONN-101',
    'CONN-103',
    'CONN-104',
    'CONN-105',
    'CONN-106',
    'CONN-107',
  ]);
  // 未重叠接电沿用上一版（CONN-102 为 11-03 单日）
  assert.deepEqual(second.carriedConnectionIds.sort(), ['CONN-102']);
  // 撤下清单只含新发布不再保留的文档：CONN-107 由隔离转为日账，其旧隔离文档被撤下；
  // CONN-106 rev2 重算后 docId 不变（内容原地替换），不属于撤下。
  assert.deepEqual(second.withdrawnDocIds, ['QUAR-CONN-107']);
  assert.ok(!second.withdrawnDocIds.includes('DAILY-CONN-102-2026-11-03'));

  // 补发午夜读数后 CONN-107 恢复
  assert.equal(pub2.quarantines.some((q) => q.connectionId === 'CONN-107'), false);
  assert.ok(pub2.accounts.some((a) => a.connectionId === 'CONN-107'));

  // 血缘全部指向新发布 id，且覆盖每条结果文档
  for (const doc of [...pub2.accounts, ...pub2.quarantines]) {
    const lin = pub2.lineage.find((l) => l.subjectDocId === doc.docId);
    assert.ok(lin, `缺少 ${doc.docId} 的血缘`);
    assert.equal(lin!.publicationId, pub2.manifest.publicationId);
  }

  // 相同输入重跑：幂等跳过，指针不变
  const third = await rerunDay(input, data, '2026-11-04');
  assert.equal(third.skipped, true);
  assert.equal(third.publicationId, pub2.manifest.publicationId);
});

test('全量输入视图（all-in-one）一次跑通，重送文件被识别', async () => {
  const { root, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  const input = allInOne; // 直接复用提交的夹具目录
  const out = await runFull(input, data);
  assert.equal(out.accountCount, 7);
  assert.equal(out.quarantineCount, 3);
  const pub = (await loadCurrentPublication(data))!;
  assert.ok(pub.manifest.inputFiles.some((f) => f.duplicateOf !== undefined));
});

test('HTTP 接口返回同一发布快照，且可指定历史发布', async () => {
  const { root, input, data } = await freshDirs();
  after(() => rm(root, { recursive: true, force: true }));
  await copyStage(run1, input);
  const first = await runFull(input, data);
  await copyStage(run2, input);
  await rerunDay(input, data, '2026-11-04');

  const handle: HttpServerHandle = await startHttpServer(data, 0);
  after(() => handle.close());
  const base = `http://localhost:${handle.port}`;
  const get = async (p: string) => (await fetch(`${base}${p}`)).json() as any;

  const current = await get('/health');
  const accounts = await get('/accounts?day=2026-11-04');
  // 同一请求内快照一致
  assert.equal(accounts.snapshot.publicationId, current.snapshot.publicationId);
  // 11-04 日账：CONN-101/106/107 各一条（CONN-103/104/105 仍被隔离，CONN-102 为 11-03 单日）
  assert.equal(accounts.count, 3);

  // 指定历史发布：看到的是首版快照（5 日账 4 隔离）
  const old = await get(`/publications?publicationId=${first.publicationId}`);
  assert.equal(old.manifest.accountCount, 5);
  assert.equal(old.manifest.quarantineCount, 4);

  const notFound = await get('/health?publicationId=pub-does-not-exist');
  assert.equal(notFound.error.includes('不存在'), true);
});
