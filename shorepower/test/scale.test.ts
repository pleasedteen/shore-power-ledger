/**
 * 规模与索引相关测试（node:test）。
 *
 * 覆盖验收要点：
 * 1. 「每版只增一个文件并回补一笔」时，单版新增的清单/索引字节随规模翻倍
 *    最多增至 1.5 倍（内容寻址树只改一条路径）；
 * 2. 重启后按发布号点查旧血缘，不逐版回放；
 * 3. 中断后上一完整发布仍可查；
 * 4. 沿用账仍返回原结算时清单；
 * 5. 首发规模（数千文件）在受限堆内完成。
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import { planTree } from '../src/objectstore.js';
import {
  expandFileList,
  listPublicationIds,
  openCurrentPublication,
  openPublication,
  readAccounts,
  readLineageBySubject,
} from '../src/storage.js';
import { runFull, rerunDay } from '../src/pipeline.js';
import { encodeGb18030 } from '../src/encoding.js';
import type { InputFileInfo } from '../src/types.js';

const execFileAsync = promisify(execFile);
const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(here, '..');

async function tempRoot(prefix: string): Promise<string> {
  return mkdtemp(path.join(os.tmpdir(), prefix));
}

/** 构造 n 个文件清单条目（键为相对路径） */
function fileInfos(n: number, extra: string[] = []): InputFileInfo[] {
  const rows: InputFileInfo[] = [];
  for (let i = 0; i < n; i++) {
    rows.push({
      file: `connections/${String(i).padStart(6, '0')}.csv`,
      encoding: 'GB18030',
      sha256: `h${i}`,
      sizeBytes: 100,
    });
  }
  for (const name of extra) {
    rows.push({ file: name, encoding: 'UTF-8', sha256: `x-${name}`, sizeBytes: 120 });
  }
  return rows;
}

/**
 * 模拟“每版只增一个文件并回补一笔”：以 n 为基线建立树，再连续新增 6 个文件，
 * 统计每个版本新增的节点字节总量。
 */
function incrementalBytesPerVersion(n: number): number {
  let files = fileInfos(n);
  const seen = new Set<string>();
  const seed = planTree(files.map((f) => ({ key: f.file, value: f })));
  for (const id of seed.nodes.keys()) seen.add(id);

  const perVersion: number[] = [];
  for (let v = 0; v < 6; v++) {
    files = [...files, ...fileInfos(0, [`backfill/round-${v}.jsonl`])];
    const plan = planTree(files.map((f) => ({ key: f.file, value: f })));
    let added = 0;
    for (const [id, text] of plan.nodes) {
      if (!seen.has(id)) {
        added += Buffer.byteLength(text, 'utf-8');
        seen.add(id);
      }
    }
    perVersion.push(added);
    files = files.slice(0, n + v + 1);
  }
  return perVersion.reduce((s, x) => s + x, 0) / perVersion.length;
}

test('每版只增一个文件时，单版新增清单字节随规模翻倍最多增至 1.5 倍', () => {
  const small = incrementalBytesPerVersion(600);
  const large = incrementalBytesPerVersion(1200);
  assert.ok(small > 0, '基线应有新增字节');
  const ratio = large / small;
  assert.ok(
    ratio <= 1.5,
    `清单新增字节比 ${ratio.toFixed(3)} 超过 1.5（small=${small.toFixed(0)}, large=${large.toFixed(0)}）`,
  );
});

/** 递归统计目录总字节（发布新增量的度量） */
async function dirBytes(dir: string): Promise<number> {
  let total = 0;
  const stack = [dir];
  while (stack.length > 0) {
    const cur = stack.pop()!;
    let entries;
    try {
      entries = await readdir(cur, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const p = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(p);
      else total += (await stat(p)).size;
    }
  }
  return total;
}

/**
 * 端到端验证「每版只增一个文件并回补一笔」：
 * 稳定状态下，规模翻倍后单版新增磁盘字节平均最多增至 1.5 倍。
 * 用较小规模以保持测试快速；首两版用于预热（新建树命名空间）。
 */
async function measureRerunBytesPerVersion(connections: number): Promise<number> {
  const root = await tempRoot('growth-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await mkdir(path.join(input, 'connections'), { recursive: true });
    await mkdir(path.join(input, 'readings'), { recursive: true });

    const dayMs = 86_400_000;
    const anchor = Date.UTC(2026, 10, 2) - 8 * 3_600_000; // 北京 2026-11-03 00:00
    const beijing = (ms: number) => {
      const s = new Date(ms + 8 * 3_600_000);
      const p = (n: number) => String(n).padStart(2, '0');
      return `${s.getUTCFullYear()}-${p(s.getUTCMonth() + 1)}-${p(s.getUTCDate())} ${p(s.getUTCHours())}:${p(s.getUTCMinutes())}:${p(s.getUTCSeconds())}`;
    };
    const start = anchor + 9 * 3_600_000;
    const end = anchor + 17 * 3_600_000;
    const connLine = (i: number) =>
      `CONN-${String(i).padStart(6, '0')},1,船-${i},M-${String(i).padStart(6, '0')},${beijing(start)},${beijing(end)}`;
    const readingLines = (i: number) => {
      const m = `M-${String(i).padStart(6, '0')}`;
      const r = (id: string, at: number, v: number) =>
        JSON.stringify({
          reading_id: id,
          revision: 1,
          meter_id: m,
          sampled_at_utc: new Date(at).toISOString().replace('.000Z', 'Z'),
          unit: 'Wh',
          cumulative: String(v),
        });
      return [r(`R-${i}-S`, start, 1000), r(`R-${i}-E`, end, 9000)];
    };

    // 每笔接电单独一个小文件，模拟“多个小文件”的首发形态
    const seedFiles = connections;
    for (let i = 0; i < seedFiles; i++) {
      await writeFile(
        path.join(input, 'connections', `c${String(i).padStart(4, '0')}.csv`),
        encodeGb18030(
          'connection_id,revision,vessel,meter_id,start_beijing,end_beijing\n' + connLine(i) + '\n',
        ),
      );
      await writeFile(
        path.join(input, 'readings', `r${String(i).padStart(4, '0')}.jsonl`),
        readingLines(i).join('\n') + '\n',
      );
    }
    await runFull(input, data);
    let prev = await dirBytes(data);

    const warm: number[] = [];
    const steady: number[] = [];
    for (let v = 0; v < 8; v++) {
      const i = connections + v;
      await writeFile(
        path.join(input, 'connections', `backfill-${v}.csv`),
        encodeGb18030(
          'connection_id,revision,vessel,meter_id,start_beijing,end_beijing\n' + connLine(i) + '\n',
        ),
      );
      await writeFile(
        path.join(input, 'readings', `backfill-${v}.jsonl`),
        readingLines(i).join('\n') + '\n',
      );
      await rerunDay(input, data, '2026-11-03');
      const now = await dirBytes(data);
      if (v < 2) warm.push(now - prev);
      else steady.push(now - prev);
      prev = now;
    }
    void dayMs;
    return steady.reduce((s, x) => s + x, 0) / steady.length;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test('端到端：每版只增一个文件并回补一笔，规模翻倍后单版新增字节平均不超过 1.5 倍', async () => {
  const small = await measureRerunBytesPerVersion(200);
  const large = await measureRerunBytesPerVersion(400);
  assert.ok(small > 0 && large > 0);
  const ratio = large / small;
  assert.ok(
    ratio <= 1.5,
    `单版新增字节比 ${ratio.toFixed(3)} 超过 1.5（small=${small.toFixed(0)}, large=${large.toFixed(0)}）`,
  );
});

test('索引树同样满足增量字节上限（每版回补一笔 → 索引只改一条路径）', () => {
  const measure = (base: number): number => {
    let pairs = [] as Array<{ key: string; value: string }>;
    for (let i = 0; i < base; i++) {
      pairs.push({ key: `DAILY-CONN-${i}-2026-11-03`, value: 'daily-account' });
      pairs.push({ key: `DAILY-CONN-${i}-2026-11-04`, value: 'daily-account' });
    }
    const seen = new Set<string>();
    const seed = planTree(pairs);
    for (const id of seed.nodes.keys()) seen.add(id);
    const perVersion: number[] = [];
    for (let v = 0; v < 6; v++) {
      const idx = base + v;
      pairs = [
        ...pairs,
        { key: `DAILY-CONN-${idx}-2026-11-03`, value: 'daily-account' },
        { key: `LIN-DAILY-CONN-${idx}-2026-11-03`, value: 'lineage' },
      ];
      const plan = planTree(pairs);
      let added = 0;
      for (const [id, text] of plan.nodes) {
        if (!seen.has(id)) {
          added += Buffer.byteLength(text, 'utf-8');
          seen.add(id);
        }
      }
      perVersion.push(added);
    }
    return perVersion.reduce((s, x) => s + x, 0) / perVersion.length;
  };
  const small = measure(600);
  const large = measure(1200);
  assert.ok(large / small <= 1.5, `索引新增字节比 ${(large / small).toFixed(3)} 超过 1.5`);
});

test('血缘不再内联整批清单：发布清单只记录清单树根', async () => {
  const root = await tempRoot('scale-inline-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await writeStage(input, {
      connections: [
        ['CONN-1', 1, '船舶A', 'M1', '2026-11-03 08:00:00', '2026-11-04 08:00:00'],
      ],
      readings: [
        ['R1', 'M1', '2026-11-03T00:00:00Z', 1000],
        ['R2', 'M1', '2026-11-03T16:00:00Z', 6000],
        ['R3', 'M1', '2026-11-04T00:00:00Z', 11000],
      ],
    });
    await runFull(input, data);
    const ref = (await openCurrentPublication(data))!;
    assert.equal(ref.legacy, false);
    assert.ok(ref.manifest.fileListRoot, '发布清单应引用清单树根');
    const lineage = await readLineageBySubject(
      data,
      ref,
      'DAILY-CONN-1-2026-11-03',
    );
    assert.ok(lineage);
    assert.ok(lineage!.fileListRoot);
    // 查询时展开为完整清单，保持旧响应形状
    assert.ok((lineage!.inputFiles?.length ?? 0) >= 2);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('沿用账返回其原结算时清单，而非本次发布的清单', async () => {
  const root = await tempRoot('carry-list-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await writeStage(input, {
      connections: [
        ['CONN-A', 1, '船A', 'MA', '2026-11-03 09:00:00', '2026-11-03 17:00:00'],
        ['CONN-B', 1, '船B', 'MB', '2026-11-05 09:00:00', '2026-11-05 17:00:00'],
      ],
      readings: [
        ['RA1', 'MA', '2026-11-03T01:00:00Z', 0],
        ['RA2', 'MA', '2026-11-03T09:00:00Z', 5000],
        ['RB1', 'MB', '2026-11-05T01:00:00Z', 0],
        ['RB2', 'MB', '2026-11-05T09:00:00Z', 4000],
      ],
    });
    await runFull(input, data);
    const pub1 = (await openCurrentPublication(data))!;
    const lin1 = await readLineageBySubject(data, pub1, 'DAILY-CONN-B-2026-11-05');
    assert.ok(lin1?.fileListRoot);
    const originalRoot = lin1!.fileListRoot;
    const originalList = await expandFileList(data, originalRoot!);

    // 增加一个新文件，重跑 11-03（只覆盖 CONN-A）：CONN-B 不在纳入集合，被沿用
    await writeFile(path.join(input, 'readings', 'extra.jsonl'), '', 'utf-8');
    await rerunDay(input, data, '2026-11-03');
    const pub2 = (await openCurrentPublication(data))!;
    const lin2 = await readLineageBySubject(data, pub2, 'DAILY-CONN-B-2026-11-05');
    assert.ok(lin2);
    // 沿用的血缘保留原清单根，内容与首版一致（不因新文件变化）
    assert.equal(lin2!.fileListRoot, originalRoot);
    assert.deepEqual(lin2!.inputFiles, originalList);
    // 本次重算的 CONN-A 血缘指向新清单根
    const linA = await readLineageBySubject(data, pub2, 'DAILY-CONN-A-2026-11-03');
    assert.notEqual(linA!.fileListRoot, originalRoot);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('重启后按发布号点查旧血缘：只按索引下钻，不加载/回放其它版本', async () => {
  const root = await tempRoot('point-query-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await writeStage(input, {
      connections: [
        ['CONN-X', 1, '船X', 'MX', '2026-11-03 08:00:00', '2026-11-04 08:00:00'],
      ],
      readings: [
        ['RX1', 'MX', '2026-11-03T00:00:00Z', 1000],
        ['RX2', 'MX', '2026-11-03T16:00:00Z', 4000],
        ['RX3', 'MX', '2026-11-04T00:00:00Z', 9000],
      ],
    });
    const first = await runFull(input, data);

    // 反复重跑产生多版，模拟历史发布堆积
    for (let i = 0; i < 3; i++) {
      await rerunDay(input, data, '2026-11-04');
    }
    const ids = await listPublicationIds(data);
    assert.ok(ids.length >= 1);

    // 打开历史发布并点查血缘：通过清单里的 lineageIndexRoot 直接下钻
    const oldRef = (await openPublication(data, first.publicationId))!;
    assert.ok(oldRef.manifest.lineageIndexRoot);
    const hit = await readLineageBySubject(data, oldRef, 'DAILY-CONN-X-2026-11-04');
    assert.ok(hit);
    assert.equal(hit!.publicationId, first.publicationId);
    // 该点查不会读取整棵历史树：结果与整包载入一致
    const accounts = await readAccounts(data, oldRef, { connectionId: 'CONN-X' });
    assert.equal(accounts.length, 2);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('点查血缘只依赖 lineage/清单树：accounts 树缺失也能命中指定发布的旧血缘', async () => {
  const root = await tempRoot('point-isolation-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await writeStage(input, {
      connections: [['CONN-Y', 1, '船Y', 'MY', '2026-11-03 09:00:00', '2026-11-03 17:00:00']],
      readings: [
        ['RY1', 'MY', '2026-11-03T01:00:00Z', 0],
        ['RY2', 'MY', '2026-11-03T09:00:00Z', 5000],
      ],
    });
    const first = await runFull(input, data);
    const ref = (await openPublication(data, first.publicationId))!;

    // 破坏 accounts 索引树节点：点查血缘不应触碰它（证明不逐版/整包回放）
    const accountsRoot = ref.manifest.accountsIndexRoot!;
    const accountsPrefix = accountsRoot.slice(0, 2);
    const accountsDir = path.join(data, 'store', 'trees', 'accounts', 'nodes', accountsPrefix);
    const names = await readdir(accountsDir);
    for (const n of names) await rm(path.join(accountsDir, n), { force: true });

    const hit = await readLineageBySubject(data, ref, 'DAILY-CONN-Y-2026-11-03');
    assert.ok(hit);
    assert.equal(hit!.publicationId, first.publicationId);
    // 反证：读日账需要 accounts 树，此时读不到（节点缺失会抛错）
    await assert.rejects(readAccounts(data, ref, { connectionId: 'CONN-Y' }));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('旧版 JSONL 发布仍可查询（兼容旧数据）', async () => {
  const root = await tempRoot('legacy-read-');
  try {
    const data = path.join(root, 'data');
    const pubDir = path.join(data, 'publications', 'pub-full-legacy-1');
    await mkdir(pubDir, { recursive: true });
    const account = {
      docType: 'daily-account',
      docId: 'DAILY-OLD-2026-11-03',
      publicationId: 'pub-full-legacy-1',
      connectionId: 'OLD',
      revision: 1,
      vessel: '旧船',
      meterId: 'MOLD',
      beijingDay: '2026-11-03',
      windowStartMs: 0,
      windowEndMs: 1000,
      usageWh: 10,
      usageKwh: 0.01,
      startBoundary: { kind: 'CONNECTION_START', sampledAtMs: 0, cumulativeWh: 0, source: { file: 'a', fileSha256: 'x', line: 1, recordId: 'r', revision: 1 } },
      endBoundary: { kind: 'CONNECTION_END', sampledAtMs: 1000, cumulativeWh: 10, source: { file: 'a', fileSha256: 'x', line: 2, recordId: 'r2', revision: 1 } },
      readingsUsed: [],
    };
    const lineage = {
      docType: 'lineage',
      docId: 'LIN-DAILY-OLD-2026-11-03',
      publicationId: 'pub-full-legacy-1',
      subjectDocId: 'DAILY-OLD-2026-11-03',
      subjectKind: 'daily-account',
      connectionId: 'OLD',
      connectionSources: [],
      readingSources: [],
      inputFiles: [{ file: 'a.csv', encoding: 'GB18030', sha256: 'x', sizeBytes: 10 }],
    };
    await writeFile(
      path.join(pubDir, 'daily-accounts.jsonl'),
      JSON.stringify(account) + '\n',
      'utf-8',
    );
    await writeFile(path.join(pubDir, 'quarantines.jsonl'), '', 'utf-8');
    await writeFile(path.join(pubDir, 'lineage.jsonl'), JSON.stringify(lineage) + '\n', 'utf-8');
    await writeFile(
      path.join(pubDir, 'manifest.json'),
      JSON.stringify({
        docType: 'manifest',
        publicationId: 'pub-full-legacy-1',
        createdAtMs: 1,
        mode: 'full',
        inputFiles: [{ file: 'a.csv', encoding: 'GB18030', sha256: 'x', sizeBytes: 10 }],
        accountCount: 1,
        quarantineCount: 0,
        recomputedConnectionIds: ['OLD'],
        carriedConnectionIds: [],
        withdrawnDocIds: [],
        warnings: [],
        fingerprint: 'legacy',
      }) + '\n',
      'utf-8',
    );
    await writeFile(
      path.join(data, 'publications', 'current.json'),
      JSON.stringify({ publicationId: 'pub-full-legacy-1', tombstoneDocIds: [] }) + '\n',
      'utf-8',
    );

    const ref = (await openCurrentPublication(data))!;
    assert.equal(ref.legacy, true);
    const rows = await readAccounts(data, ref, { day: '2026-11-03' });
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.publicationId, 'pub-full-legacy-1');
    const lin = await readLineageBySubject(data, ref, 'DAILY-OLD-2026-11-03');
    assert.equal(lin!.inputFiles?.length, 1);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('旧版发布可被重跑沿用并升级为新格式：不残留旧发布 id，重复重跑幂等', async () => {
  const root = await tempRoot('legacy-upgrade-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    // 旧发布中只有 CONN-K；输入里另有一个不相关且不重叠的 CONN-Z，
    // 让重跑 2026-11-03 时 CONN-Z 被沿用（其文档来自旧 JSONL）。
    await writeStage(input, {
      connections: [
        ['CONN-Z', 1, '船Z', 'MZ', '2026-11-10 09:00:00', '2026-11-10 17:00:00'],
      ],
      readings: [
        ['RZ1', 'MZ', '2026-11-10T01:00:00Z', 0],
        ['RZ2', 'MZ', '2026-11-10T09:00:00Z', 5000],
      ],
    });

    const pubDir = path.join(data, 'publications', 'pub-full-legacy-1');
    await mkdir(pubDir, { recursive: true });
    const srcRef = { file: 'old.csv', fileSha256: 'x', line: 1, recordId: 'K', revision: 1 };
    const account = {
      docType: 'daily-account',
      docId: 'DAILY-CONN-K-2026-11-05',
      publicationId: 'pub-full-legacy-1',
      connectionId: 'CONN-K',
      revision: 1,
      vessel: '旧船K',
      meterId: 'MK',
      beijingDay: '2026-11-05',
      windowStartMs: 0,
      windowEndMs: 1000,
      usageWh: 10,
      usageKwh: 0.01,
      startBoundary: { kind: 'CONNECTION_START', sampledAtMs: 0, cumulativeWh: 0, source: srcRef },
      endBoundary: { kind: 'CONNECTION_END', sampledAtMs: 1000, cumulativeWh: 10, source: srcRef },
      readingsUsed: [],
    };
    const lineage = {
      docType: 'lineage',
      docId: 'LIN-DAILY-CONN-K-2026-11-05',
      publicationId: 'pub-full-legacy-1',
      subjectDocId: 'DAILY-CONN-K-2026-11-05',
      subjectKind: 'daily-account',
      connectionId: 'CONN-K',
      connectionSources: [],
      readingSources: [],
      inputFiles: [{ file: 'old.csv', encoding: 'GB18030', sha256: 'x', sizeBytes: 10 }],
    };
    await writeFile(path.join(pubDir, 'daily-accounts.jsonl'), JSON.stringify(account) + '\n');
    await writeFile(path.join(pubDir, 'quarantines.jsonl'), '');
    await writeFile(path.join(pubDir, 'lineage.jsonl'), JSON.stringify(lineage) + '\n');
    await writeFile(
      path.join(pubDir, 'manifest.json'),
      JSON.stringify({
        docType: 'manifest',
        publicationId: 'pub-full-legacy-1',
        createdAtMs: 1,
        mode: 'full',
        inputFiles: [{ file: 'old.csv', encoding: 'GB18030', sha256: 'x', sizeBytes: 10 }],
        accountCount: 1,
        quarantineCount: 0,
        recomputedConnectionIds: ['CONN-K'],
        carriedConnectionIds: [],
        withdrawnDocIds: [],
        warnings: [],
        fingerprint: 'legacy',
      }) + '\n',
    );
    await writeFile(
      path.join(data, 'publications', 'current.json'),
      JSON.stringify({ publicationId: 'pub-full-legacy-1', tombstoneDocIds: [] }) + '\n',
    );

    // 重跑 2026-11-03：仅纳入 CONN-Z，CONN-K 沿用并升级
    const out = await rerunDay(input, data, '2026-11-03');
    assert.equal(out.skipped, false);
    const ref = (await openCurrentPublication(data))!;
    assert.equal(ref.legacy, false, '重跑后应为新格式发布');
    const carried = (await readAccounts(data, ref, { connectionId: 'CONN-K' }))[0];
    assert.ok(carried);
    assert.equal(carried!.publicationId, ref.publicationId);
    // 沿用血缘保留原清单内容（来自旧发布内联清单），并转换为清单树根
    const lin = await readLineageBySubject(data, ref, 'DAILY-CONN-K-2026-11-05');
    assert.ok(lin!.fileListRoot);
    assert.deepEqual(lin!.inputFiles?.map((f) => f.file), ['old.csv']);

    // 相同输入再次重跑：幂等跳过
    const again = await rerunDay(input, data, '2026-11-03');
    assert.equal(again.skipped, true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('中断后上一完整发布仍可查（发布目录残缺不影响指针）', async () => {
  const root = await tempRoot('interrupt-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await writeStage(input, {
      connections: [['CONN-1', 1, '船', 'M1', '2026-11-03 09:00:00', '2026-11-03 17:00:00']],
      readings: [
        ['R1', 'M1', '2026-11-03T01:00:00Z', 0],
        ['R2', 'M1', '2026-11-03T09:00:00Z', 5000],
      ],
    });
    await runFull(input, data);
    const pub1 = (await openCurrentPublication(data))!;

    // 模拟下一次发布中断：残留临时目录 + 无人引用的残缺对象
    const tmp = path.join(data, 'publications', `pub-full-20990101000000-dead.tmp-1-x`);
    await mkdir(tmp, { recursive: true });
    await writeFile(path.join(tmp, 'manifest.json'), '{broken');
    await mkdir(path.join(data, 'store', 'blobs', 'aa'), { recursive: true });
    await writeFile(path.join(data, 'store', 'blobs', 'aa', 'orphan.json'), '{}', 'utf-8');

    const after = (await openCurrentPublication(data))!;
    assert.equal(after.publicationId, pub1.publicationId);
    const rows = await readAccounts(data, after, { connectionId: 'CONN-1' });
    assert.equal(rows.length, 1);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('首发规模：2000 笔接电 / 2000 个小文件在 192 MiB 堆内完成', async () => {
  const root = await tempRoot('scale-run-');
  try {
    const input = path.join(root, 'input');
    const data = path.join(root, 'data');
    await execFileAsync(
      process.execPath,
      [
        '--import',
        'tsx',
        path.join(projectRoot, 'scripts', 'generate-scale-fixtures.ts'),
        '--out',
        input,
        '--count',
        '2000',
        '--files',
        '2000',
      ],
      { cwd: projectRoot },
    );
    const { stdout } = await execFileAsync(
      process.execPath,
      [
        '--max-old-space-size=192',
        '--import',
        'tsx',
        path.join(projectRoot, 'src', 'cli.ts'),
        'run',
        '--input',
        input,
        '--data',
        data,
      ],
      { cwd: projectRoot, maxBuffer: 64 * 1024 * 1024 },
    );
    assert.match(stdout, /日账文档：4000 条/);
    assert.match(stdout, /隔离文档：0 条/);

    // 发布对象按内容寻址：文档不重复落盘
    const blobPrefixes = await readdir(path.join(data, 'store', 'blobs'));
    let blobCount = 0;
    for (const p of blobPrefixes) {
      blobCount += (await readdir(path.join(data, 'store', 'blobs', p))).length;
    }
    // 4000 日账 + 4000 血缘 = 8000 个唯一文档
    assert.equal(blobCount, 8000);

    // 发布目录本身只有 manifest（不再有整包 JSONL）
    const ref = (await openCurrentPublication(data))!;
    const files = await readdir(path.join(data, 'publications', ref.publicationId));
    assert.deepEqual(files, ['manifest.json']);
    const manifestSize = (await stat(path.join(data, 'publications', ref.publicationId, 'manifest.json'))).size;
    // 清单本体不再含整批文件清单，保持很小
    assert.ok(manifestSize < 200_000, `manifest 大小 ${manifestSize} 过大`);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

// ───────────────────────── 测试夹具写入 ─────────────────────────

interface Stage {
  connections: Array<[string, number, string, string, string, string]>;
  readings: Array<[string, string, string, number]>;
}

async function writeStage(input: string, stage: Stage): Promise<void> {
  await mkdir(path.join(input, 'connections'), { recursive: true });
  await mkdir(path.join(input, 'readings'), { recursive: true });
  const csv = [
    'connection_id,revision,vessel,meter_id,start_beijing,end_beijing',
    ...stage.connections.map((r) => r.join(',')),
  ].join('\n');
  await writeFile(
    path.join(input, 'connections', '2026-11-03_connections.csv'),
    encodeGb18030(csv + '\n'),
  );
  const jsonl = stage.readings
    .map(([id, meter, at, value]) =>
      JSON.stringify({
        reading_id: id,
        revision: 1,
        meter_id: meter,
        sampled_at_utc: at,
        unit: 'Wh',
        cumulative: String(value),
      }),
    )
    .join('\n');
  await writeFile(path.join(input, 'readings', 'readings.jsonl'), jsonl + '\n', 'utf-8');
}
