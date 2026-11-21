/**
 * 历史回补（按北京时间日期范围一次发布）测试。
 *
 * 覆盖：
 * 1. 整笔跨日接电选择：最新输入窗口 ∪ 上一版发布窗口，一次发布出整批结果；
 * 2. 进程中断与断点续跑：钩子注入中断 + 子进程真实退出（exitCode=2）两种方式，
 *    中断后即使投递目录整体更换也能续跑，已持久化接电不重算；
 * 3. 成功发布后的重试返回原发布号，历史发布列表不新增重复版本；
 * 4. 提交前当前/历史查询只能看到上一完整发布，提交后同一发布号下可查整批结果；
 * 5. 暂存量只随纳入接电笔数变化：无关读数增至十倍，续跑暂存不得翻倍。
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  cp,
  mkdtemp,
  mkdir,
  rm,
  writeFile,
  readdir,
  readFile,
} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { runFull, rerunRange, type BackfillHooks } from '../src/pipeline.js';
import {
  listPublicationIds,
  loadCurrentPublication,
} from '../src/storage.js';
import {
  backfillJobKey,
  stagingTotalBytes,
} from '../src/backfill-store.js';
import { startHttpServer, type HttpServerHandle } from '../src/server.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(here, '..');
const run1 = path.join(projectRoot, 'fixtures', 'run1');
const run2 = path.join(projectRoot, 'fixtures', 'run2');
const cliPath = path.join(projectRoot, 'src', 'cli.ts');

async function freshRoot(): Promise<string> {
  return mkdtemp(path.join(os.tmpdir(), 'backfill-test-'));
}

async function makeInput(root: string, name = 'input'): Promise<string> {
  const input = path.join(root, name);
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  return input;
}

async function copyStage(src: string, input: string): Promise<void> {
  await cp(path.join(src, 'connections'), path.join(input, 'connections'), { recursive: true });
  await cp(path.join(src, 'readings'), path.join(input, 'readings'), { recursive: true });
}

function runCli(args: string[], env: NodeJS.ProcessEnv = {}): Promise<{ code: number | null; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', cliPath, ...args], {
      cwd: projectRoot,
      env: { ...process.env, ...env },
    });
    let stderr = '';
    child.stderr.on('data', (b) => (stderr += b.toString()));
    child.stdout.on('data', () => {});
    child.on('error', reject);
    child.on('close', (code) => resolve({ code, stderr }));
  });
}

test('日期范围回补：整笔纳入跨日接电，一次发布整批结果，单日与范围语义一致', async () => {
  const root = await freshRoot();
  after(() => rm(root, { recursive: true, force: true }));
  const input = await makeInput(root);
  const data = path.join(root, 'data');
  await mkdir(data, { recursive: true });

  await copyStage(run1, input);
  const base = await runFull(input, data);
  assert.equal(base.skipped, false);

  // 范围 11-03 ~ 11-04：覆盖全部接电（含单日 CONN-102/105），整笔发布
  const out = await rerunRange(input, data, '2026-11-03', '2026-11-04');
  assert.equal(out.skipped, false);
  assert.equal(out.publicationId, 'pub-backfill-20261103-20261104');
  assert.deepEqual(
    out.recomputedConnectionIds.sort(),
    ['CONN-101', 'CONN-102', 'CONN-103', 'CONN-104', 'CONN-105', 'CONN-106', 'CONN-107'],
  );
  assert.deepEqual(out.carriedConnectionIds, []);
  // 与首次全量业务内容一致的部分：日账 5、隔离 4
  assert.equal(out.accountCount, 5);
  assert.equal(out.quarantineCount, 4);

  const pub = (await loadCurrentPublication(data))!;
  assert.equal(pub.manifest.mode, 'backfill');
  assert.equal(pub.manifest.backfillStartDay, '2026-11-03');
  assert.equal(pub.manifest.backfillEndDay, '2026-11-04');

  // 单日范围等价于按天重叠选择
  const root2 = await freshRoot();
  after(() => rm(root2, { recursive: true, force: true }));
  const input2 = await makeInput(root2);
  const data2 = path.join(root2, 'data');
  await mkdir(data2, { recursive: true });
  await copyStage(run1, input2);
  await runFull(input2, data2);
  const dayOnly = await rerunRange(input2, data2, '2026-11-04', '2026-11-04');
  // 11-04 重叠：除单日 11-03 的 CONN-102 外全部纳入；CONN-102 沿用
  assert.ok(!dayOnly.recomputedConnectionIds.includes('CONN-102'));
  assert.deepEqual(dayOnly.carriedConnectionIds, ['CONN-102']);
});

test('进程内中断后续跑：已暂存接电不重算，重试返回原发布号且历史无重复版本', async () => {
  const root = await freshRoot();
  after(() => rm(root, { recursive: true, force: true }));
  const input = await makeInput(root);
  const data = path.join(root, 'data');
  await mkdir(data, { recursive: true });
  await copyStage(run1, input);
  const base = await runFull(input, data);

  // 实时结算计数：onConnectionComputed 仅在真正计算时触发，从暂存读回不触发
  const computedIds: string[] = [];
  const crashAfterTwo: BackfillHooks = {
    onConnectionComputed: ({ connectionId }) => computedIds.push(connectionId),
    onConnectionStaged: ({ completedCount }) => {
      if (completedCount === 2) {
        throw new Error('模拟中断');
      }
    },
  };

  await assert.rejects(
    () => rerunRange(input, data, '2026-11-03', '2026-11-04', crashAfterTwo),
    /模拟中断/,
  );

  // 中断后：当前指针仍指向上一完整发布，历史列表没有回补半成品
  const midCurrent = await loadCurrentPublication(data);
  assert.equal(midCurrent!.manifest.publicationId, base.publicationId);
  const midIds = await listPublicationIds(data);
  assert.deepEqual(midIds, [base.publicationId]);

  // 暂存里恰好两笔接电
  const jobKey = backfillJobKey('2026-11-03', '2026-11-04');
  const stagedFiles = await readdir(path.join(data, 'backfills', jobKey, 'staging'));
  assert.equal(stagedFiles.length, 2);

  // 续跑（不再注入中断）
  const resumedComputed: string[] = [];
  const resumed = await rerunRange(input, data, '2026-11-03', '2026-11-04', {
    onConnectionComputed: ({ connectionId }) => resumedComputed.push(connectionId),
  });
  assert.equal(resumed.skipped, false);
  assert.equal(resumed.resumed, true);
  assert.equal(resumed.publicationId, 'pub-backfill-20261103-20261104');

  // 中断前实时计算了前两笔（按 id 排序：CONN-101、CONN-102）
  assert.deepEqual(computedIds, ['CONN-101', 'CONN-102']);
  // 续跑只实时计算剩余 5 笔，已持久化的两笔绝不重算
  assert.equal(resumedComputed.length, 5);
  assert.ok(!resumedComputed.includes('CONN-101'));
  assert.ok(!resumedComputed.includes('CONN-102'));

  // 成功后的重试：返回原发布号，历史发布列表不新增重复版本
  const idsBefore = await listPublicationIds(data);
  const retry = await rerunRange(input, data, '2026-11-03', '2026-11-04');
  assert.equal(retry.skipped, true);
  assert.equal(retry.publicationId, resumed.publicationId);
  const idsAfter = await listPublicationIds(data);
  assert.deepEqual(idsAfter, idsBefore);
  assert.equal(idsAfter.filter((id) => id === resumed.publicationId).length, 1);
});

test('子进程真实中断 + 投递目录更换后续跑：HTTP 当前/历史只看到整批发布结果', async () => {
  const root = await freshRoot();
  after(() => rm(root, { recursive: true, force: true }));
  const data = path.join(root, 'data');
  await mkdir(data, { recursive: true });

  // 首次投递目录 input-a（run1），先建立全量基线发布
  const inputA = await makeInput(root, 'input-a');
  await copyStage(run1, inputA);
  const base = await runFull(inputA, data);

  const rangeArgs = (input: string) => [
    'rerun-range',
    '--start',
    '2026-11-03',
    '--end',
    '2026-11-04',
    '--input',
    input,
    '--data',
    data,
  ];

  // 子进程：暂存 3 笔后真实退出（exitCode=2）
  const crashed = await runCli(rangeArgs(inputA), { BACKFILL_CRASH_AFTER: '3' });
  assert.equal(crashed.code, 2);
  assert.match(crashed.stderr, /中断注入/);

  // 中断期间 HTTP 查询只能看到上一完整发布
  const handleMid: HttpServerHandle = await startHttpServer(data, 0);
  const baseMid = `http://localhost:${handleMid.port}`;
  const midHealth = await (await fetch(`${baseMid}/health`)).json() as any;
  assert.equal(midHealth.snapshot.publicationId, base.publicationId);
  const midPubs = await (await fetch(`${baseMid}/publications`)).json() as any;
  assert.deepEqual(midPubs.allPublicationIds, [base.publicationId]);
  // 回补发布号尚不可查
  const midBackfill = await (
    await fetch(`${baseMid}/health?publicationId=pub-backfill-20261103-20261104`)
  ).json() as any;
  assert.match(midBackfill.error, /不存在/);
  await handleMid.close();

  // 投递目录整体更换为 input-b（run1 + run2 的合并视图），旧目录不再参与
  const inputB = await makeInput(root, 'input-b');
  await copyStage(run1, inputB);
  await copyStage(run2, inputB);
  await rm(inputA, { recursive: true, force: true });

  // 续跑：前 3 笔从暂存读回（即使输入目录已换），其余完成整批发布
  const resumedRun = await runCli(rangeArgs(inputB));
  assert.equal(resumedRun.code, 0);

  const finalPub = (await loadCurrentPublication(data))!;
  assert.equal(finalPub.manifest.publicationId, 'pub-backfill-20261103-20261104');
  assert.equal(finalPub.manifest.mode, 'backfill');

  // 提交后 HTTP：当前与按发布号的历史查询都指向同一发布号、同一份整批结果
  const handle: HttpServerHandle = await startHttpServer(data, 0);
  after(() => handle.close());
  const baseUrl = `http://localhost:${handle.port}`;
  const get = async (p: string) => (await fetch(`${baseUrl}${p}`)).json() as Promise<any>;

  const health = await get('/health');
  assert.equal(health.snapshot.publicationId, finalPub.manifest.publicationId);

  const accountsCurrent = await get('/accounts?day=2026-11-04');
  const accountsHistory = await get(
    '/accounts?day=2026-11-04&publicationId=pub-backfill-20261103-20261104',
  );
  assert.equal(accountsCurrent.snapshot.publicationId, finalPub.manifest.publicationId);
  assert.equal(accountsHistory.snapshot.publicationId, finalPub.manifest.publicationId);
  assert.equal(accountsCurrent.count, accountsHistory.count);
  assert.deepEqual(
    accountsCurrent.accounts.map((a: any) => a.docId).sort(),
    accountsHistory.accounts.map((a: any) => a.docId).sort(),
  );
  // run2 补发后 CONN-107 解除隔离、CONN-106 升 rev2：整批结果里可见
  assert.ok(accountsCurrent.accounts.some((a: any) => a.connectionId === 'CONN-107'));
  assert.ok(accountsCurrent.accounts.some((a: any) => a.connectionId === 'CONN-106' && a.revision === 2));

  // 历史发布列表：基线 + 回补各一个，无重复/幽灵版本（按 id 词序排列）
  const pubs = await get('/publications');
  assert.deepEqual([...pubs.allPublicationIds].sort(), [
    'pub-backfill-20261103-20261104',
    base.publicationId,
  ].sort());

  // 基线发布仍可按 id 查询（快照不可变）
  const oldBundle = await get(`/publications?publicationId=${base.publicationId}`);
  assert.equal(oldBundle.manifest.publicationId, base.publicationId);
  assert.equal(oldBundle.manifest.accountCount, 5);

  // 再跑一次同范围：幂等，历史列表不变
  const retryRun = await runCli(rangeArgs(inputB));
  assert.equal(retryRun.code, 0);
  const pubsRetry = await get('/publications');
  assert.deepEqual(pubsRetry.allPublicationIds.sort(), pubs.allPublicationIds.sort());
});

test('无关读数增至十倍：续跑暂存字节数不翻倍（暂存只随纳入接电笔数增长）', async () => {
  // 构造两份仅“无关读数数量”不同的输入：读数属于未纳入范围的电表/日期
  async function buildInput(input: string, extraReadings: number): Promise<void> {
    await copyStage(run1, input);
    const lines: string[] = [];
    for (let i = 0; i < extraReadings; i++) {
      lines.push(
        JSON.stringify({
          reading_id: `IRRELEVANT-${i}`,
          revision: 1,
          // 电表 M99 不被任何接电使用；采样时间也在全部接电窗口之外
          meter_id: 'M99',
          sampled_at_utc: '2026-10-01T00:00:00Z',
          unit: 'Wh',
          cumulative: String(1000 + i),
        }),
      );
    }
    await writeFile(path.join(input, 'readings', '2026-10-01_irrelevant.jsonl'), lines.join('\n') + '\n');
  }

  async function scenario(extraReadings: number): Promise<{ bytes: number; stagedCount: number }> {
    const root = await freshRoot();
    const input = await makeInput(root);
    const data = path.join(root, 'data');
    await mkdir(data, { recursive: true });
    await buildInput(input, extraReadings);
    await runFull(input, data);

    // 在第 3 笔后中断，制造同等进度的续跑暂存
    await assert.rejects(
      () =>
        rerunRange(input, data, '2026-11-03', '2026-11-04', {
          onConnectionStaged: ({ completedCount }) => {
            if (completedCount === 3) throw new Error('模拟中断');
          },
        }),
      /模拟中断/,
    );
    const jobKey = backfillJobKey('2026-11-03', '2026-11-04');
    const bytes = await stagingTotalBytes(data, jobKey);
    const stagedCount = (
      await readdir(path.join(data, 'backfills', jobKey, 'staging'))
    ).filter((f) => f.endsWith('.json')).length;
    await rm(root, { recursive: true, force: true });
    return { bytes, stagedCount };
  }

  const a = await scenario(10);
  const b = await scenario(100); // 无关读数十倍
  assert.equal(a.stagedCount, 3);
  assert.equal(b.stagedCount, 3);
  // 暂存只存逐接电结算文档与文件元数据，无关读数不进暂存：字节数不得翻倍
  assert.ok(
    b.bytes < a.bytes * 2,
    `无关读数十倍后暂存不应翻倍：base=${a.bytes} bytes, inflated=${b.bytes} bytes`,
  );
  // 输入文件元数据只多一个文件条目，增量应很小（远小于一份接电暂存）
  assert.ok(b.bytes - a.bytes < a.bytes * 0.5);
});

test('回补范围参数校验：非法日期与倒置区间被拒绝', async () => {
  const root = await freshRoot();
  after(() => rm(root, { recursive: true, force: true }));
  const input = await makeInput(root);
  const data = path.join(root, 'data');
  await mkdir(data, { recursive: true });
  await copyStage(run1, input);
  await runFull(input, data);

  await assert.rejects(
    () => rerunRange(input, data, '2026-11-05', '2026-11-03'),
    /日期范围非法/,
  );
  await assert.rejects(
    () => rerunRange(input, data, '11-05', '2026-11-06'),
    /YYYY-MM-DD/,
  );
  void readFile;
});
