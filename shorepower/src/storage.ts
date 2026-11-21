/**
 * 本地存储层。
 *
 * 目录结构（v2，内容寻址增量存储）：
 *   <data>/store/blobs/<前2位>/<sha256>.json        不可变文档（日账/隔离/血缘）
 *   <data>/store/trees/<命名空间>/nodes/<前2位>/<id>.json
 *        accounts / quarantines / lineage / filelists 四棵持久化索引树
 *   <data>/publications/
 *       pub-<mode>-<时间戳>-<随机>/manifest.json    发布清单：只记录各树根 id
 *       current.json                               指向最近一次完整发布的指针（原子 rename）
 *       pub-...tmp-xxx/                            发布中途退出的残留目录（启动时清理）
 *
 * 为什么这样改：
 * - 旧版把「本次发布扫描到的整批输入文件清单」逐条复制进每条血缘，文件数上千时
 *   发布体积与内存按「文档数 × 文件数」膨胀，首发即可能撑爆堆内存；
 * - 现在文档按内容哈希只存一份，索引与文件清单改用内容寻址的前缀树，
 *   每次发布仅新增「本次变化的那条路径」的节点，单版新增字节近似恒定；
 * - 每个发布清单自带各索引树根 id，查询某发布只需按根 id 下钻，
 *   重启后按发布号点查血缘无需逐版回放；
 * - 发布仍先写对象、再原子 rename 发布目录、最后切换指针，中断不影响上一完整发布。
 *
 * 兼容性：旧版发布目录内的 daily-accounts.jsonl / quarantines.jsonl / lineage.jsonl
 * 仍然可直接读取（storageVersion 缺省即旧格式）。
 */

import { mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import {
  cleanupObjectStoreStaging,
  getBlob,
  getTreeEntries,
  getTreeValue,
  hashValue,
  mapLimit,
  persistBlob,
  persistTree,
  planTree,
  type TreePlan,
} from './objectstore.js';
import type {
  DailyAccount,
  InputFileInfo,
  LineageRecord,
  PublicationManifest,
  QuarantineEntry,
  StoredDailyAccount,
  StoredLineageRecord,
  StoredQuarantineEntry,
} from './types.js';

export interface PublicationBundle {
  manifest: PublicationManifest;
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
}

/** 管线内部使用：不含 publicationId、不展开清单的存储态文档 */
export interface StoredPublication {
  manifest: PublicationManifest;
  accounts: StoredDailyAccount[];
  quarantines: StoredQuarantineEntry[];
  lineage: StoredLineageRecord[];
}

/** 一次发布在存储中的定位信息（查询入口） */
export interface PublicationRef {
  publicationId: string;
  manifest: PublicationManifest;
  /** true = 旧版 JSONL 发布 */
  legacy: boolean;
}

export interface AccountFilter {
  day?: string;
  connectionId?: string;
  vessel?: string;
  meterId?: string;
}

const PUBLICATIONS_DIR = 'publications';
const CURRENT_POINTER = 'current.json';
const STORAGE_VERSION = 2;

export const TREE_ACCOUNTS = 'accounts';
export const TREE_QUARANTINES = 'quarantines';
export const TREE_LINEAGE = 'lineage';
export const TREE_FILELISTS = 'filelists';
/**
 * 发布清单里若干「id 集合」也落内容寻址树，避免 manifest 自身随规模线性膨胀。
 * 按业务含义分命名空间，便于按名诊断，节点仍按内容哈希去重。
 */
export const TREE_RECOMPUTED = 'manifest-recomputed';
export const TREE_CARRIED = 'manifest-carried';
export const TREE_WITHDRAWN = 'manifest-withdrawn';

interface CurrentPointer {
  publicationId: string;
  /**
   * 跨发布累积的文档墓碑：曾经被撤下的结果文档 id。
   * 重跑时某文档可能在“上一版”里已经不存在，单靠相邻两版对比会让撤下清单
   * 在幂等重跑间抖动；持久墓碑保证撤下记录只增不减、重跑确定。
   */
  tombstoneDocIds: string[];
}

interface AccountIndexValue {
  b: string;
  d: string;
  c: string;
  v: string;
  m: string;
}

interface ConnectionIndexValue {
  b: string;
  c: string;
}

function publicationsRoot(dataDir: string): string {
  return path.join(dataDir, PUBLICATIONS_DIR);
}

function publicationDir(dataDir: string, publicationId: string): string {
  return path.join(publicationsRoot(dataDir), publicationId);
}

function pointerPath(dataDir: string): string {
  return path.join(publicationsRoot(dataDir), CURRENT_POINTER);
}

async function readManifestFile(file: string): Promise<PublicationManifest | null> {
  try {
    return JSON.parse(await readFile(file, 'utf-8')) as PublicationManifest;
  } catch {
    return null;
  }
}

/** 读取当前指针；无发布时返回 null */
async function readPointer(dataDir: string): Promise<CurrentPointer | null> {
  try {
    const raw = JSON.parse(await readFile(pointerPath(dataDir), 'utf-8')) as CurrentPointer;
    return { publicationId: raw.publicationId, tombstoneDocIds: raw.tombstoneDocIds ?? [] };
  } catch {
    return null;
  }
}

/** 读取历史墓碑（无发布时为空集） */
export async function loadTombstones(dataDir: string): Promise<string[]> {
  const pointer = await readPointer(dataDir);
  return pointer ? [...pointer.tombstoneDocIds].sort() : [];
}

async function readJsonl<T>(file: string): Promise<T[]> {
  let text: string;
  try {
    text = await readFile(file, 'utf-8');
  } catch {
    return [];
  }
  return text
    .split(/\r?\n/)
    .filter((l) => l.trim().length > 0)
    .map((l) => JSON.parse(l) as T);
}

/** 清理上次未完成的临时发布目录与对象存储临时文件 */
export async function cleanupStaging(dataDir: string): Promise<void> {
  await cleanupObjectStoreStaging(dataDir);
  const root = publicationsRoot(dataDir);
  let entries: string[];
  try {
    entries = await readdir(root);
  } catch {
    return;
  }
  await Promise.all(
    entries
      .filter((e) => e.includes('.tmp-'))
      .map((e) => rm(path.join(root, e), { recursive: true, force: true })),
  );
}

export async function listPublicationIds(dataDir: string): Promise<string[]> {
  try {
    const entries = await readdir(publicationsRoot(dataDir));
    return entries
      .filter((e) => e.startsWith('pub-') && !e.includes('.tmp-'))
      .sort();
  } catch {
    return [];
  }
}

/**
 * 文件清单快照的展开缓存（按根 id 内容寻址，天然不可变）。
 * 同一进程内多次查询同一份清单时复用；沿用的旧结果指向原清单根，
 * 因此同一条目可同时服务多个发布而不必重复解析。
 * 设上限避免长时间运行的服务缓存无限增长。
 */
const FILE_LIST_CACHE_MAX = 64;
const fileListCache = new Map<string, InputFileInfo[]>();

export async function expandFileList(dataDir: string, rootId: string): Promise<InputFileInfo[]> {
  const cached = fileListCache.get(rootId);
  if (cached) return cached;
  const rows = (await getTreeEntries(dataDir, TREE_FILELISTS, rootId)).map(
    ([, value]) => value as InputFileInfo,
  );
  rows.sort((a, b) => (a.file < b.file ? -1 : a.file > b.file ? 1 : 0));
  cachePut(fileListCache, rootId, rows);
  return rows;
}

function cachePut<T>(cache: Map<string, T>, key: string, value: T): void {
  if (cache.size >= FILE_LIST_CACHE_MAX) {
    const oldest = cache.keys().next();
    if (!oldest.done) cache.delete(oldest.value);
  }
  cache.set(key, value);
}

/** id 集合的展开缓存（按根 id 内容寻址，天然不可变） */
const idSetCache = new Map<string, string[]>();

async function expandIdSet(
  dataDir: string,
  namespace: string,
  rootId: string,
): Promise<string[]> {
  const key = `${namespace}:${rootId}`;
  const cached = idSetCache.get(key);
  if (cached) return cached;
  const rows = (await getTreeEntries(dataDir, namespace, rootId)).map(([k]) => k);
  rows.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  cachePut(idSetCache, key, rows);
  return rows;
}

/** 打开指定发布并展开清单与 id 集合（不读取任何结果文档） */
export async function openPublication(
  dataDir: string,
  publicationId: string,
): Promise<PublicationRef | null> {
  const raw = await readManifestFile(path.join(publicationDir(dataDir, publicationId), 'manifest.json'));
  if (!raw) return null;
  const legacy = raw.storageVersion !== STORAGE_VERSION;
  let manifest: PublicationManifest = raw;
  if (!legacy && raw.fileListRoot) {
    const inputFiles = await expandFileList(dataDir, raw.fileListRoot);
    manifest = { ...raw, inputFiles };
  } else if (!Array.isArray(raw.inputFiles)) {
    manifest = { ...raw, inputFiles: [] };
  }
  if (!legacy) {
    const [recomputed, carried, withdrawn] = await Promise.all([
      raw.recomputedConnectionIdsRoot
        ? expandIdSet(dataDir, TREE_RECOMPUTED, raw.recomputedConnectionIdsRoot)
        : Promise.resolve(raw.recomputedConnectionIds ?? []),
      raw.carriedConnectionIdsRoot
        ? expandIdSet(dataDir, TREE_CARRIED, raw.carriedConnectionIdsRoot)
        : Promise.resolve(raw.carriedConnectionIds ?? []),
      raw.withdrawnDocIdsRoot
        ? expandIdSet(dataDir, TREE_WITHDRAWN, raw.withdrawnDocIdsRoot)
        : Promise.resolve(raw.withdrawnDocIds ?? []),
    ]);
    manifest = {
      ...manifest,
      recomputedConnectionIds: recomputed,
      carriedConnectionIds: carried,
      withdrawnDocIds: withdrawn,
    };
  }
  return { publicationId, manifest, legacy };
}

export async function openCurrentPublication(dataDir: string): Promise<PublicationRef | null> {
  const pointer = await readPointer(dataDir);
  if (!pointer) return null;
  return openPublication(dataDir, pointer.publicationId);
}

function stampAccount(a: StoredDailyAccount, publicationId: string): DailyAccount {
  return { ...a, publicationId };
}

function stampQuarantine(q: StoredQuarantineEntry, publicationId: string): QuarantineEntry {
  return { ...q, publicationId };
}

function stampLineage(l: StoredLineageRecord, publicationId: string): LineageRecord {
  const { inputFiles, ...rest } = l;
  const base = { ...rest, publicationId } as LineageRecord;
  if (inputFiles) base.inputFiles = inputFiles;
  return base;
}

async function readAccountBlob(dataDir: string, hash: string): Promise<StoredDailyAccount> {
  return getBlob<StoredDailyAccount>(dataDir, hash);
}

async function readQuarantineBlob(
  dataDir: string,
  hash: string,
): Promise<StoredQuarantineEntry> {
  return getBlob<StoredQuarantineEntry>(dataDir, hash);
}

async function readLineageBlob(dataDir: string, hash: string): Promise<StoredLineageRecord> {
  return getBlob<StoredLineageRecord>(dataDir, hash);
}

/** 读取整个发布的全部文档（旧格式走 JSONL；新格式走索引树） */
export async function loadPublication(
  dataDir: string,
  publicationId: string,
): Promise<PublicationBundle | null> {
  const ref = await openPublication(dataDir, publicationId);
  if (!ref) return null;
  const { manifest } = ref;
  if (ref.legacy) {
    const dir = publicationDir(dataDir, publicationId);
    try {
      const [accounts, quarantines, lineage] = await Promise.all([
        readJsonl<DailyAccount>(path.join(dir, 'daily-accounts.jsonl')),
        readJsonl<QuarantineEntry>(path.join(dir, 'quarantines.jsonl')),
        readJsonl<LineageRecord>(path.join(dir, 'lineage.jsonl')),
      ]);
      return { manifest, accounts, quarantines, lineage };
    } catch {
      return null;
    }
  }
  try {
    const [accounts, quarantines, lineage] = await Promise.all([
      materializeAccounts(dataDir, ref),
      materializeQuarantines(dataDir, ref),
      materializeLineage(dataDir, ref),
    ]);
    return { manifest, accounts, quarantines, lineage };
  } catch {
    return null;
  }
}

export async function loadCurrentPublication(dataDir: string): Promise<PublicationBundle | null> {
  const pointer = await readPointer(dataDir);
  if (!pointer) return null;
  return loadPublication(dataDir, pointer.publicationId);
}

/**
 * 旧版 JSONL 文档内联了 publicationId。沿用进新版内容寻址存储前必须剔除，
 * 否则同一业务内容会因发布 id 不同而无法去重（也会污染沿用语义）。
 */
function stripPublicationId<T>(doc: T): T {
  const { publicationId: _pub, ...rest } = doc as T & { publicationId?: unknown };
  return rest as T;
}

/**
 * 读取发布的存储态文档（不含 publicationId、不展开清单）。
 * 管线重跑沿用旧结果时使用：避免为每条血缘复制整批清单。
 */
export async function loadStoredPublication(
  dataDir: string,
  publicationId: string,
): Promise<StoredPublication | null> {
  const ref = await openPublication(dataDir, publicationId);
  if (!ref) return null;
  if (ref.legacy) {
    const dir = publicationDir(dataDir, publicationId);
    try {
      const [accounts, quarantines, lineage] = await Promise.all([
        readJsonl<StoredDailyAccount>(path.join(dir, 'daily-accounts.jsonl')),
        readJsonl<StoredQuarantineEntry>(path.join(dir, 'quarantines.jsonl')),
        readJsonl<StoredLineageRecord>(path.join(dir, 'lineage.jsonl')),
      ]);
      return {
        manifest: ref.manifest,
        accounts: accounts.map(stripPublicationId),
        quarantines: quarantines.map(stripPublicationId),
        lineage: lineage.map(stripPublicationId),
      };
    } catch {
      return null;
    }
  }
  const [accounts, quarantines, lineage] = await Promise.all([
    materializeAccountsRaw(dataDir, ref),
    materializeQuarantinesRaw(dataDir, ref),
    materializeLineageRaw(dataDir, ref),
  ]);
  return { manifest: ref.manifest, accounts, quarantines, lineage };
}

async function materializeAccountsRaw(
  dataDir: string,
  ref: PublicationRef,
): Promise<StoredDailyAccount[]> {
  if (!ref.manifest.accountsIndexRoot) return [];
  const entries = await getTreeEntries(dataDir, TREE_ACCOUNTS, ref.manifest.accountsIndexRoot);
  const rows = await mapLimit(entries, IO_LIMIT, ([, value]) =>
    readAccountBlob(dataDir, (value as AccountIndexValue).b),
  );
  rows.sort((a, b) => (a.docId < b.docId ? -1 : a.docId > b.docId ? 1 : 0));
  return rows;
}

async function materializeQuarantinesRaw(
  dataDir: string,
  ref: PublicationRef,
): Promise<StoredQuarantineEntry[]> {
  if (!ref.manifest.quarantinesIndexRoot) return [];
  const entries = await getTreeEntries(
    dataDir,
    TREE_QUARANTINES,
    ref.manifest.quarantinesIndexRoot,
  );
  const rows = await mapLimit(entries, IO_LIMIT, ([, value]) =>
    readQuarantineBlob(dataDir, (value as ConnectionIndexValue).b),
  );
  rows.sort((a, b) => (a.docId < b.docId ? -1 : a.docId > b.docId ? 1 : 0));
  return rows;
}

async function materializeLineageRaw(
  dataDir: string,
  ref: PublicationRef,
): Promise<StoredLineageRecord[]> {
  if (!ref.manifest.lineageIndexRoot) return [];
  const entries = await getTreeEntries(dataDir, TREE_LINEAGE, ref.manifest.lineageIndexRoot);
  const rows = await mapLimit(entries, IO_LIMIT, ([, value]) =>
    readLineageBlob(dataDir, (value as ConnectionIndexValue).b),
  );
  rows.sort((a, b) => (a.docId < b.docId ? -1 : a.docId > b.docId ? 1 : 0));
  return rows;
}

async function materializeAccounts(
  dataDir: string,
  ref: PublicationRef,
): Promise<DailyAccount[]> {
  return (await materializeAccountsRaw(dataDir, ref)).map((a) =>
    stampAccount(a, ref.publicationId),
  );
}

async function materializeQuarantines(
  dataDir: string,
  ref: PublicationRef,
): Promise<QuarantineEntry[]> {
  return (await materializeQuarantinesRaw(dataDir, ref)).map((q) =>
    stampQuarantine(q, ref.publicationId),
  );
}

async function materializeLineage(
  dataDir: string,
  ref: PublicationRef,
): Promise<LineageRecord[]> {
  const rows = await materializeLineageRaw(dataDir, ref);
  return mapLimit(rows, IO_LIMIT, async (l) => {
    const stamped = stampLineage(l, ref.publicationId);
    if (!stamped.inputFiles && stamped.fileListRoot) {
      stamped.inputFiles = await expandFileList(dataDir, stamped.fileListRoot);
    }
    return stamped;
  });
}

// ───────────────────────── 点查 / 过滤查询 ─────────────────────────

/** 按结果文档 id 点查血缘：新格式只下钻一条索引路径，绝不逐版回放 */
export async function readLineageBySubject(
  dataDir: string,
  ref: PublicationRef,
  subjectDocId: string,
): Promise<LineageRecord | undefined> {
  if (ref.legacy) {
    const bundle = await loadPublication(dataDir, ref.publicationId);
    return bundle?.lineage.find((l) => l.subjectDocId === subjectDocId);
  }
  if (!ref.manifest.lineageIndexRoot) return undefined;
  const value = (await getTreeValue(
    dataDir,
    TREE_LINEAGE,
    ref.manifest.lineageIndexRoot,
    subjectDocId,
  )) as ConnectionIndexValue | undefined;
  if (!value) return undefined;
  const doc = await readLineageBlob(dataDir, value.b);
  const stamped = stampLineage(doc, ref.publicationId);
  if (!stamped.inputFiles && stamped.fileListRoot) {
    stamped.inputFiles = await expandFileList(dataDir, stamped.fileListRoot);
  }
  return stamped;
}

export async function readAccounts(
  dataDir: string,
  ref: PublicationRef,
  filter: AccountFilter,
): Promise<DailyAccount[]> {
  if (ref.legacy) {
    const bundle = await loadPublication(dataDir, ref.publicationId);
    return (bundle?.accounts ?? []).filter((a) => matchesAccount(a, filter));
  }
  if (!ref.manifest.accountsIndexRoot) return [];
  const entries = await getTreeEntries(dataDir, TREE_ACCOUNTS, ref.manifest.accountsIndexRoot);
  const matched: string[] = [];
  for (const [, value] of entries) {
    const v = value as AccountIndexValue;
    if (
      (filter.day === undefined || v.d === filter.day) &&
      (filter.connectionId === undefined || v.c === filter.connectionId) &&
      (filter.vessel === undefined || v.v === filter.vessel) &&
      (filter.meterId === undefined || v.m === filter.meterId)
    ) {
      matched.push(v.b);
    }
  }
  const rows = await mapLimit(matched, IO_LIMIT, (hash) => readAccountBlob(dataDir, hash));
  rows.sort((a, b) => (a.docId < b.docId ? -1 : a.docId > b.docId ? 1 : 0));
  return rows.map((a) => stampAccount(a, ref.publicationId));
}

function matchesAccount(
  a: StoredDailyAccount,
  filter: AccountFilter,
): boolean {
  return (
    (filter.day === undefined || a.beijingDay === filter.day) &&
    (filter.connectionId === undefined || a.connectionId === filter.connectionId) &&
    (filter.vessel === undefined || a.vessel === filter.vessel) &&
    (filter.meterId === undefined || a.meterId === filter.meterId)
  );
}

export async function readQuarantines(
  dataDir: string,
  ref: PublicationRef,
  connectionId?: string,
): Promise<QuarantineEntry[]> {
  if (ref.legacy) {
    const bundle = await loadPublication(dataDir, ref.publicationId);
    return (bundle?.quarantines ?? []).filter(
      (q) => connectionId === undefined || q.connectionId === connectionId,
    );
  }
  if (!ref.manifest.quarantinesIndexRoot) return [];
  const entries = await getTreeEntries(
    dataDir,
    TREE_QUARANTINES,
    ref.manifest.quarantinesIndexRoot,
  );
  const matched: string[] = [];
  for (const [, value] of entries) {
    const v = value as ConnectionIndexValue;
    if (connectionId === undefined || v.c === connectionId) matched.push(v.b);
  }
  const rows = await mapLimit(matched, IO_LIMIT, (hash) =>
    readQuarantineBlob(dataDir, hash),
  );
  rows.sort((a, b) => (a.docId < b.docId ? -1 : a.docId > b.docId ? 1 : 0));
  return rows.map((q) => stampQuarantine(q, ref.publicationId));
}

export async function readLineageByConnection(
  dataDir: string,
  ref: PublicationRef,
  connectionId: string,
): Promise<LineageRecord[]> {
  if (ref.legacy) {
    const bundle = await loadPublication(dataDir, ref.publicationId);
    return (bundle?.lineage ?? []).filter((l) => l.connectionId === connectionId);
  }
  if (!ref.manifest.lineageIndexRoot) return [];
  const entries = await getTreeEntries(dataDir, TREE_LINEAGE, ref.manifest.lineageIndexRoot);
  const matched: string[] = [];
  for (const [, value] of entries) {
    const v = value as ConnectionIndexValue;
    if (v.c === connectionId) matched.push(v.b);
  }
  const docs = await mapLimit(matched, IO_LIMIT, (hash) => readLineageBlob(dataDir, hash));
  const rows = await mapLimit(docs, IO_LIMIT, async (l) => {
    const stamped = stampLineage(l, ref.publicationId);
    if (!stamped.inputFiles && stamped.fileListRoot) {
      stamped.inputFiles = await expandFileList(dataDir, stamped.fileListRoot);
    }
    return stamped;
  });
  rows.sort((a, b) => (a.docId < b.docId ? -1 : a.docId > b.docId ? 1 : 0));
  return rows;
}

export async function readAllLineage(
  dataDir: string,
  ref: PublicationRef,
): Promise<LineageRecord[]> {
  if (ref.legacy) {
    return (await loadPublication(dataDir, ref.publicationId))?.lineage ?? [];
  }
  return materializeLineage(dataDir, ref);
}

/** 只列出发布中全部结果文档 id（不读取任何文档内容），供撤下清单计算 */
export async function listResultDocIds(
  dataDir: string,
  ref: PublicationRef,
): Promise<{ accountDocIds: string[]; quarantineDocIds: string[] }> {
  if (ref.legacy) {
    const dir = publicationDir(dataDir, ref.publicationId);
    const [accounts, quarantines] = await Promise.all([
      readJsonl<{ docId: string }>(path.join(dir, 'daily-accounts.jsonl')),
      readJsonl<{ docId: string }>(path.join(dir, 'quarantines.jsonl')),
    ]);
    return {
      accountDocIds: accounts.map((d) => d.docId),
      quarantineDocIds: quarantines.map((d) => d.docId),
    };
  }
  const [accounts, quarantines] = await Promise.all([
    ref.manifest.accountsIndexRoot
      ? getTreeEntries(dataDir, TREE_ACCOUNTS, ref.manifest.accountsIndexRoot)
      : Promise.resolve([]),
    ref.manifest.quarantinesIndexRoot
      ? getTreeEntries(dataDir, TREE_QUARANTINES, ref.manifest.quarantinesIndexRoot)
      : Promise.resolve([]),
  ]);
  return {
    accountDocIds: accounts.map(([key]) => key),
    quarantineDocIds: quarantines.map(([key]) => key),
  };
}

// ───────────────────────── 写入 ─────────────────────────

export interface PublicationDocs {
  accounts: StoredDailyAccount[];
  quarantines: StoredQuarantineEntry[];
  lineage: StoredLineageRecord[];
}

export interface IndexRoots {
  accountsIndexRoot: string;
  quarantinesIndexRoot: string;
  lineageIndexRoot: string;
}

/** 文档 + 索引树的完整落盘计划：先算根 id/内容，再统一决定是否写入 */
export interface PublicationPlan extends IndexRoots {
  /** [内容哈希, 文档]，内容相同只写一次 */
  blobs: Map<string, unknown>;
  /** [命名空间, 树计划] */
  trees: Array<[string, TreePlan]>;
}

/** 有并发上限的 IO 调度，避免数千对象同时打开文件描述符 */
const IO_LIMIT = 32;

/**
 * 规划本次发布的文档与索引树（纯内存，不落盘）。
 * 文档按内容哈希去重：沿用/未变化的文档不会产生新字节；
 * 索引树同样只写变化路径上的节点。
 */
export function planPublicationDocs(docs: PublicationDocs): PublicationPlan {
  const blobs = new Map<string, unknown>();
  const addBlob = (value: unknown): string => {
    const h = hashValue(value);
    if (!blobs.has(h)) blobs.set(h, value);
    return h;
  };

  const accountPairs: Array<{ key: string; value: AccountIndexValue }> = [];
  for (const a of docs.accounts) {
    accountPairs.push({
      key: a.docId,
      value: { b: addBlob(a), d: a.beijingDay, c: a.connectionId, v: a.vessel, m: a.meterId },
    });
  }
  const quarantinePairs: Array<{ key: string; value: ConnectionIndexValue }> = [];
  for (const q of docs.quarantines) {
    quarantinePairs.push({ key: q.docId, value: { b: addBlob(q), c: q.connectionId } });
  }
  const lineagePairs: Array<{ key: string; value: ConnectionIndexValue }> = [];
  for (const l of docs.lineage) {
    lineagePairs.push({ key: l.subjectDocId, value: { b: addBlob(l), c: l.connectionId } });
  }

  const accountsTree = planTree(accountPairs);
  const quarantinesTree = planTree(quarantinePairs);
  const lineageTree = planTree(lineagePairs);

  return {
    accountsIndexRoot: accountsTree.rootId,
    quarantinesIndexRoot: quarantinesTree.rootId,
    lineageIndexRoot: lineageTree.rootId,
    blobs,
    trees: [
      [TREE_ACCOUNTS, accountsTree],
      [TREE_QUARANTINES, quarantinesTree],
      [TREE_LINEAGE, lineageTree],
    ],
  };
}

/** 落盘一份发布计划：文档按内容哈希写入，索引树按节点存在性写入 */
export async function persistPublicationPlan(
  dataDir: string,
  plan: PublicationPlan,
): Promise<void> {
  await mapLimit([...plan.blobs.entries()], IO_LIMIT, ([hash, value]) =>
    persistBlob(dataDir, hash, value),
  );
  for (const [namespace, tree] of plan.trees) {
    await persistTree(dataDir, namespace, tree);
  }
}

/** 发布内容指纹：稳定字段 + 各内容寻址树根，等价于旧版整包规范化哈希 */
export function manifestFingerprint(
  manifest: Omit<PublicationManifest, 'fingerprint' | 'publicationId' | 'createdAtMs' | 'inputFiles'>,
): string {
  const stable = {
    docType: manifest.docType,
    mode: manifest.mode,
    rerunDay: manifest.rerunDay,
    fileListRoot: manifest.fileListRoot,
    accountCount: manifest.accountCount,
    quarantineCount: manifest.quarantineCount,
    // 用 id 集合树的根 id 参与指纹：根 id 由集合内容唯一决定，等价于直接哈希集合
    recomputedConnectionIdsRoot: manifest.recomputedConnectionIdsRoot,
    carriedConnectionIdsRoot: manifest.carriedConnectionIdsRoot,
    withdrawnDocIdsRoot: manifest.withdrawnDocIdsRoot,
    warnings: manifest.warnings,
    accountsIndexRoot: manifest.accountsIndexRoot,
    quarantinesIndexRoot: manifest.quarantinesIndexRoot,
    lineageIndexRoot: manifest.lineageIndexRoot,
  };
  return hashValue(stable);
}

export interface ManifestIdSetPlan {
  recomputedConnectionIdsRoot: string;
  carriedConnectionIdsRoot: string;
  withdrawnDocIdsRoot: string;
  trees: Array<[string, TreePlan]>;
}

/**
 * 规划发布清单里的三组 id 集合树。
 * 这些集合随规模线性增长，若直接内联进 manifest，manifest 单版新增字节会翻倍；
 * 改走内容寻址树后，manifest 只剩固定大小的元数据 + 树根。
 */
export function planManifestIdSets(
  recomputedConnectionIds: string[],
  carriedConnectionIds: string[],
  withdrawnDocIds: string[],
): ManifestIdSetPlan {
  const toPairs = (ids: string[]) =>
    ids.map((id) => ({ key: id, value: true as const }));
  const recomputed = planTree(toPairs(recomputedConnectionIds));
  const carried = planTree(toPairs(carriedConnectionIds));
  const withdrawn = planTree(toPairs(withdrawnDocIds));
  return {
    recomputedConnectionIdsRoot: recomputed.rootId,
    carriedConnectionIdsRoot: carried.rootId,
    withdrawnDocIdsRoot: withdrawn.rootId,
    trees: [
      [TREE_RECOMPUTED, recomputed],
      [TREE_CARRIED, carried],
      [TREE_WITHDRAWN, withdrawn],
    ],
  };
}

/** 落盘清单 id 集合树（已存在节点按内容哈希跳过） */
export async function persistManifestIdSets(
  dataDir: string,
  plan: ManifestIdSetPlan,
): Promise<void> {
  for (const [namespace, tree] of plan.trees) {
    await persistTree(dataDir, namespace, tree);
  }
}

/**
 * 原子发布：写发布清单到临时目录，rename 为正式目录，再切换指针。
 * 任一步失败/进程退出，current.json 都保持指向上一完整发布。
 *
 * 落盘清单只保留树根引用：整批文件清单、三组 id 集合都以根 id 记录，
 * 避免 manifest 自身随规模膨胀。
 */
export async function commitPublication(
  dataDir: string,
  manifest: PublicationManifest,
  tombstoneDocIds: string[],
): Promise<{ publicationId: string; skipped: false }> {
  const root = publicationsRoot(dataDir);
  await mkdir(root, { recursive: true });
  const publicationId = manifest.publicationId;
  const tmpDir = path.join(
    root,
    `${publicationId}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`,
  );
  const finalDir = publicationDir(dataDir, publicationId);
  await mkdir(tmpDir, { recursive: true });

  const {
    inputFiles: _expanded,
    recomputedConnectionIds: _recomputed,
    carriedConnectionIds: _carried,
    withdrawnDocIds: _withdrawn,
    ...onDisk
  } = manifest;
  await writeFile(path.join(tmpDir, 'manifest.json'), JSON.stringify(onDisk, null, 2) + '\n');

  await rename(tmpDir, finalDir);
  const pointerTmp = path.join(root, `${CURRENT_POINTER}.tmp-${process.pid}`);
  const pointer: CurrentPointer = {
    publicationId,
    tombstoneDocIds: [...tombstoneDocIds].sort(),
  };
  await writeFile(pointerTmp, JSON.stringify(pointer, null, 2) + '\n');
  await rename(pointerTmp, pointerPath(dataDir));
  return { publicationId, skipped: false };
}
