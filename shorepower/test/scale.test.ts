/**
 * 规模与读写范围验证：90 天夹具、反复更正一笔跨日接电。
 *
 * 断言目标（各自低于整版明细的 10%）：
 * 1. 冷启动（新进程）后按天查询新版单日明细的读取条数；
 * 2. 每次重跑读取旧明细的条数；
 * 3. 每次重跑写入新明细的条数。
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { encodeGb18030 } from '../src/encoding.js';
import { rerunDay, runFull } from '../src/pipeline.js';
import { openCurrentPublication } from '../src/storage.js';
import { startHttpServer } from '../src/server.js';
import type { DetailIoSnapshot } from '../src/metrics.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(here, '..');
const DAYS = 90;
const START_DAY = '2026-08-01';
/** 反复更正的同一笔跨日接电（每天 0 号，02:00 起跨日 26 小时） */
const TARGET_CONN = 'CONN-2026-10-29-00';
/** 目标接电跨入的次日：按该日重跑才会纳入这笔跨日接电 */
const CORRECTION_DAY = '2026-10-30';

async function generate(
  dir: string,
  options: { revision?: number } = {},
): Promise<void> {
  const revision = options.revision ?? 1;
  execFileSync(
    process.execPath,
    [
      '--import',
      'tsx',
      path.join(projectRoot, 'scripts', 'generate-scale-fixtures.ts'),
      dir,
      '--days',
      String(DAYS),
      '--per-day',
      '3',
      '--start',
      START_DAY,
      '--target',
      TARGET_CONN,
      '--revision',
      String(revision),
    ],
    { cwd: projectRoot, stdio: 'pipe' },
  );
}

test('90 天规模：按天查询与按日重跑只触碰少量明细（<10%）', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'scale-test-'));
  after(() => rm(root, { recursive: true, force: true }));
  const baseInput = path.join(root, 'input-base');
  const correctedInput = path.join(root, 'input-corrected');
  const data = path.join(root, 'data');

  await generate(baseInput);
  await generate(correctedInput, { revision: 2 });

  const first = await runFull(baseInput, data);
  assert.equal(first.skipped, false);
  // 90 天 × 3 接电，跨日接电拆两天：450 日账 + 450 血缘 = 900 条明细
  assert.equal(first.accountCount, 450);
  assert.equal(first.detailDocCount, first.accountCount + first.quarantineCount + first.lineageDocCount);
  assert.ok(first.detailDocCount > 500, `整版明细应达到规模，实际 ${first.detailDocCount}`);

  // 整版明细基准：每次更正只应触碰远小于该值的明细
  const totalDetail = first.detailDocCount;
  const budget = Math.floor(totalDetail * 0.1);

  // —— 场景 1：深入眼，冷启动后按天查询新版单日明细 ——
  const reads: DetailIoSnapshot[] = [];
  const handle = await startHttpServer(data, 0, {
    onDetailIo: (snap, pathName) => {
      if (pathName === '/accounts' || pathName === '/connection') reads.push(snap);
    },
  });
  after(() => handle.close());
  const base = `http://localhost:${handle.port}`;

  // 冷启动：直接起服务查询最新发布，不依赖任何进程内缓存
  const dayResp = (await (await fetch(`${base}/accounts?day=${CORRECTION_DAY}`)).json()) as any;
  assert.ok(dayResp.count > 0, '目标日应有日账');
  assert.equal(reads.length, 1);
  const queryRead = reads.at(-1)!;
  assert.ok(
    queryRead.detailDocsRead > 0 && queryRead.detailDocsRead < budget,
    `按天查询读取明细 ${queryRead.detailDocsRead} 条，应低于整版 ${totalDetail} 条的 10%`,
  );

  // 单条接电查询同样只读少量明细
  const firstConn = dayResp.accounts[0].connectionId as string;
  const connResp = (await (await fetch(`${base}/connection?id=${firstConn}`)).json()) as any;
  assert.ok(connResp.accounts.length > 0);
  assert.equal(reads.length, 2);
  assert.ok(
    reads[1]!.detailDocsRead > 0 && reads[1]!.detailDocsRead < budget,
    `按接电查询读取明细 ${reads[1]!.detailDocsRead} 条，应低于 10%`,
  );

  // 旧发布 id 查询仍可用（请求内快照一致）
  const oldHealth = (await (await fetch(`${base}/health?publicationId=${first.publicationId}`)).json()) as any;
  assert.equal(oldHealth.snapshot.publicationId, first.publicationId);
  const currentHealth = (await (await fetch(`${base}/health`)).json()) as any;
  assert.equal(currentHealth.snapshot.publicationId, first.publicationId);

  let lastPublicationId = first.publicationId;

  // —— 场景 2：反复更正同一笔跨日接电（rev2 → rev5），逐次核对读/写范围 ——
  for (let revision = 2; revision <= 5; revision++) {
    // 每次更正只带来一份新的输入视图，模拟运营逐次提交修订
    await generate(correctedInput, { revision });
    const out = await rerunDay(correctedInput, data, CORRECTION_DAY);
    assert.equal(out.skipped, false, `修订 ${revision} 应产生新发布`);
    assert.ok(
      out.recomputedConnectionIds.includes(TARGET_CONN),
      `修订 ${revision} 应重算目标跨日接电，实际 ${out.recomputedConnectionIds.join(',')}`,
    );
    assert.ok(
      out.detailIo.detailDocsRead < budget,
      `修订 ${revision} 读取旧明细 ${out.detailIo.detailDocsRead} 条，应低于 ${budget}`,
    );
    assert.ok(
      out.detailIo.detailDocsWritten < budget,
      `修订 ${revision} 写入新明细 ${out.detailIo.detailDocsWritten} 条，应低于 ${budget}`,
    );
    assert.notEqual(out.publicationId, lastPublicationId);
    lastPublicationId = out.publicationId;

    // 每次重跑的结果都可查询，沿用接电的快照完整（日账/隔离/血缘一一对应）
    const reader = await openCurrentPublication(data);
    assert.ok(reader);
    const bundle = await reader!.toBundle();
    assert.equal(bundle.accounts.length, bundle.manifest.accountCount);
    assert.equal(bundle.lineage.length, bundle.accounts.length + bundle.quarantines.length);
    assert.equal(
      bundle.accounts.find((a) => a.connectionId === TARGET_CONN)?.revision,
      revision,
    );

    // 同一份输入重复重跑：幂等跳过（不产生新发布，也不写明细）
    const again = await rerunDay(correctedInput, data, CORRECTION_DAY);
    assert.equal(again.skipped, true);
    assert.equal(again.publicationId, lastPublicationId);
    assert.equal(again.detailIo.detailDocsWritten, 0);
  }

  // 更正后的目标发布：按天查询可以读到目标接电的新版明细
  const correctedDay = (await (await fetch(`${base}/accounts?day=${CORRECTION_DAY}`)).json()) as any;
  assert.ok(correctedDay.count > 0);
  const targetRow = correctedDay.accounts.find((a: any) => a.connectionId === TARGET_CONN);
  assert.ok(targetRow, '目标接电应出现在次日日账中');
  assert.equal(targetRow.revision, 5);

  // 整版清单依然记录了正确的撤下集合与继承集合
  const finalReader = await openCurrentPublication(data);
  const finalManifest = finalReader!.manifest;
  assert.ok(finalManifest.carriedConnectionIds.length > DAYS, '未涉及的接电应大量沿用');
  assert.ok(finalManifest.recomputedConnectionIds.length < 10, '每次重跑只重算少量接电');
});

/**
 * 旧版整版发布：手写一份无 index.json 的整版目录，验证
 * 1) HTTP 查询兼容旧发布 ID；2) 以其为当前发布重跑时能一次性迁移为分片，结果不变、旧文档进入墓碑。
 */
test('旧版整版发布：查询兼容旧发布 ID，重跑可迁移为分片', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'legacy-test-'));
  after(() => rm(root, { recursive: true, force: true }));
  const data = path.join(root, 'data');
  const pubs = path.join(data, 'publications');
  const legacyId = 'pub-full-20260101000000-legacy';
  const legacyDir = path.join(pubs, legacyId);
  const { mkdir, writeFile } = await import('node:fs/promises');
  await mkdir(legacyDir, { recursive: true });

  // 与旧版整版落盘一致：日账/隔离把 publicationId 放在末尾，血缘放在 docId 之后
  const account = {
    docType: 'daily-account',
    docId: 'DAILY-CONN-101-2026-11-03',
    connectionId: 'CONN-101',
    revision: 1,
    vessel: '远洋之星',
    meterId: 'M1',
    beijingDay: '2026-11-03',
    windowStartMs: 1,
    windowEndMs: 2,
    usageWh: 16000,
    usageKwh: 16,
    startBoundary: { kind: 'CONNECTION_START', sampledAtMs: 1, cumulativeWh: 12000, source: { file: 'a', fileSha256: 'x', line: 2, recordId: 'CONN-101', revision: 1 } },
    endBoundary: { kind: 'BEIJING_MIDNIGHT', day: '2026-11-03', sampledAtMs: 2, cumulativeWh: 28000, source: { file: 'a', fileSha256: 'x', line: 3, recordId: 'R102', revision: 1 } },
    readingsUsed: [],
    publicationId: legacyId,
  };
  const quarantine = {
    docType: 'quarantine',
    docId: 'QUAR-CONN-103',
    connectionId: 'CONN-103',
    revision: 1,
    vessel: '蓝鲸号',
    meterId: 'M3',
    startBeijingMs: 10,
    endBeijingMs: 20,
    reasons: ['MISSING_MIDNIGHT_READING'],
    messages: ['缺午夜'],
    evidence: [],
    publicationId: legacyId,
  };
  const lineage = {
    docType: 'lineage',
    docId: 'LIN-DAILY-CONN-101-2026-11-03',
    publicationId: legacyId,
    subjectDocId: 'DAILY-CONN-101-2026-11-03',
    subjectKind: 'daily-account',
    connectionId: 'CONN-101',
    connectionSources: [],
    readingSources: [],
    inputFiles: [],
  };
  const manifest = {
    docType: 'manifest',
    publicationId: legacyId,
    createdAtMs: Date.now(),
    mode: 'full',
    inputFiles: [],
    accountCount: 1,
    quarantineCount: 1,
    recomputedConnectionIds: ['CONN-101', 'CONN-103'],
    carriedConnectionIds: [],
    withdrawnDocIds: [],
    warnings: [],
    fingerprint: 'legacy',
  };
  await writeFile(path.join(legacyDir, 'manifest.json'), JSON.stringify(manifest, null, 2));
  await writeFile(path.join(legacyDir, 'daily-accounts.jsonl'), JSON.stringify(account) + '\n');
  await writeFile(path.join(legacyDir, 'quarantines.jsonl'), JSON.stringify(quarantine) + '\n');
  await writeFile(path.join(legacyDir, 'lineage.jsonl'), JSON.stringify(lineage) + '\n');
  await writeFile(
    path.join(pubs, 'current.json'),
    JSON.stringify({ publicationId: legacyId, tombstoneDocIds: [] }),
  );

  const handle = await startHttpServer(data, 0);
  after(() => handle.close());
  const base = `http://localhost:${handle.port}`;
  const day = (await (await fetch(`${base}/accounts?day=2026-11-03`)).json()) as any;
  assert.equal(day.count, 1);
  assert.equal(day.accounts[0].publicationId, legacyId);
  assert.equal(day.accounts[0].usageWh, 16000);
  const byId = (await (await fetch(`${base}/lineage?docId=DAILY-CONN-101-2026-11-03`)).json()) as any;
  assert.equal(byId.count, 1);
  assert.equal(byId.lineage[0].publicationId, legacyId);

  // 目录内确实只有旧格式文件（没有 index.json）
  assert.ok(!(await readdir(legacyDir)).includes('index.json'));

  // 用同一份输入重跑一个无关日期：CONN-101/103 沿用，迁移为分片发布
  const input = path.join(root, 'input');
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  await writeFile(
    path.join(input, 'connections', '2026-11-03_connections.csv'),
    encodeGb18030(
      'connection_id,revision,vessel,meter_id,start_beijing,end_beijing\n' +
        'CONN-101,1,远洋之星,M1,2026-11-03 08:00:00,2026-11-04 08:00:00\n',
    ),
  );
  const out = await rerunDay(input, data, '2026-11-15');
  assert.equal(out.skipped, false);
  assert.deepEqual(out.recomputedConnectionIds, []);
  assert.deepEqual(out.carriedConnectionIds, ['CONN-101', 'CONN-103']);

  // 迁移后的发布为分片格式，且旧文档 id 全部保留（不误入墓碑）
  const migrated = await openCurrentPublication(data);
  assert.ok(migrated!.isSharded);
  assert.deepEqual(out.withdrawnDocIds, []);
  const newDay = (await (await fetch(`${base}/accounts?day=2026-11-03`)).json()) as any;
  assert.equal(newDay.count, 1);
  assert.equal(newDay.snapshot.publicationId, out.publicationId);
  assert.equal(newDay.accounts[0].usageWh, 16000);
  const newQuar = (await (await fetch(`${base}/quarantines`)).json()) as any;
  assert.equal(newQuar.count, 1);

  // 再次重跑同一天：幂等，且分片发布不再读取任何旧明细
  const again = await rerunDay(input, data, '2026-11-15');
  assert.equal(again.skipped, true);
  assert.equal(again.detailIo.detailDocsRead, 0);
});

test('规模夹具文件按天落盘、可重复生成', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'fixture-shape-'));
  after(() => rm(root, { recursive: true, force: true }));
  await generate(root);
  const conns = await readdir(path.join(root, 'connections'));
  const reads = await readdir(path.join(root, 'readings'));
  assert.equal(conns.length, DAYS);
  assert.equal(reads.length, DAYS);
  const size = (await stat(path.join(root, 'readings', reads[0]!))).size;
  assert.ok(size > 0);
  const raw = await readFile(path.join(root, 'readings', reads[0]!), 'utf-8');
  assert.ok(raw.includes('sampled_at_utc'));
});
