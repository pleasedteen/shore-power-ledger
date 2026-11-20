/**
 * 本地存储层（分片版）。
 *
 * 目录结构：
 *   <data>/publications/pub-<模式>-<时间戳>-<随机>/
 *       index.json                    发布索引：按日/按接电定位明细分片，无需读明细
 *       accounts/<day>/<conn>.jsonl   日账分片（一次接电 × 一个北京自然日）
 *       quarantine/<conn>.jsonl       隔离分片（一次接电至多一条）
 *       lineage/<conn>.jsonl          血缘分片（一次接电的全部结果文档血缘）
 *       manifest.json                 发布清单（模式、计数、撤下、告警、指纹）
 *   <data>/publications/pub-...tmp-xxx/  发布中途退出的残留目录（启动时清理）
 *   <data>/publications/current.json     指向最近一次完整发布的指针（原子 rename）
 *
 * 分片只保存“业务内容”，不写入 publicationId；读取时按所在发布目录注入。
 * 因此沿用上一版的分片可以用硬链接直接进入新发布，既保证新发布是完整快照，
 * 又不必读取或重写未涉及的明细。旧版发布（无 index.json 的整版 JSONL）仍可读取。
 *
 * 单次查询只打开一个发布目录，天然实现“同一次完整发布”的快照隔离。
 */

import { createHash } from 'node:crypto';
import { copyFile, link, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import type { DetailIoCounter, DetailIoSnapshot } from './metrics.js';
import type {
  DailyAccount,
  LineageRecord,
  PublicationManifest,
  QuarantineEntry,
} from './types.js';

/** 分片内的日账文档：不含发布 id（读取时注入） */
export type StoredAccount = Omit<DailyAccount, 'publicationId'>;
/** 分片内的隔离文档：不含发布 id */
export type StoredQuarantine = Omit<QuarantineEntry, 'publicationId'>;
/** 分片内的血缘文档：不含发布 id */
export type StoredLineage = Omit<LineageRecord, 'publicationId'>;
export type StoredDetail = StoredAccount | StoredQuarantine | StoredLineage;

/** 明细分片元数据，写入 index.json */
export interface ShardInfo {
  /** 发布目录内相对路径 */
  file: string;
  /** 分片内容 SHA-256（不含发布 id，跨发布稳定） */
  sha256: string;
  /** 明细文档条数 */
  docCount: number;
  sizeBytes: number;
}

/** 单次接电在索引中的摘要，用于按天筛选、重叠判断与撤下计算 */
export interface ConnectionIndexEntry {
  connectionId: string;
  /** 本次结算采用的接电窗口（绝对时间 ms） */
  startMs: number;
  endMs: number;
  /** 产出的日账覆盖的北京自然日 */
  days: string[];
  accountDocIds: string[];
  quarantineDocId?: string;
  /** 该接电的日账分片（按日） */
  accountShards: string[];
  quarantineShard?: string;
  /** 该接电的血缘分片 */
  lineageShard: string;
  /** 该接电的血缘文档 id（LIN-<结果文档 id>） */
  lineageDocIds: string[];
}

/** 发布索引：把“按天/按接电/按文档”查询定位到少量分片 */
export interface PublicationIndex {
  docType: 'index';
  formatVersion: 2;
  /** 北京自然日 -> 该日有日账的接电 id（升序） */
  days: Record<string, string[]>;
  /** 接电 id -> 索引摘要 */
  connections: Record<string, ConnectionIndexEntry>;
  /** 全部明细分片（按路径升序） */
  shards: ShardInfo[];
}

/** 整版读取时的聚合视图（测试与演示使用；服务端按需读取分片） */
export interface PublicationBundle {
  manifest: PublicationManifest;
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
}

const PUBLICATIONS_DIR = 'publications';
const CURRENT_POINTER = 'current.json';
const INDEX_FILE = 'index.json';
const FORMAT_VERSION = 2;

interface CurrentPointer {
  publicationId: string;
  /**
   * 跨发布累积的文档墓碑：曾经被撤下的结果文档 id。
   * 重跑时某文档可能在“上一版”里已经不存在，单靠相邻两版对比会让撤下清单
   * 在幂等重跑间抖动；持久墓碑保证撤下记录只增不减、重跑确定。
   */
  tombstoneDocIds: string[];
}

function publicationsRoot(dataDir: string): string {
  return path.join(dataDir, PUBLICATIONS_DIR);
}

function pointerPath(dataDir: string): string {
  return path.join(publicationsRoot(dataDir), CURRENT_POINTER);
}

/** 读取当前指针；无发布时返回 null（含空墓碑的初始态由 commitPublication 建立） */
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

function sha256(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

/**
 * 接电 id 转文件键：保留可读字符，追加短哈希避免不同 id 归一化后碰撞。
 * 结果确定，便于人工排查与稳定复现。
 */
export function connectionKey(connectionId: string): string {
  const slug = connectionId.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 48);
  const hash = createHash('sha256').update(connectionId).digest('hex').slice(0, 8);
  return `${slug}-${hash}`;
}

export function accountShardFile(day: string, connectionId: string): string {
  return `accounts/${day}/${connectionKey(connectionId)}.jsonl`;
}

export function quarantineShardFile(connectionId: string): string {
  return `quarantine/${connectionKey(connectionId)}.jsonl`;
}

export function lineageShardFile(connectionId: string): string {
  return `lineage/${connectionKey(connectionId)}.jsonl`;
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

function toJsonl(rows: unknown[]): string {
  return rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length > 0 ? '\n' : '');
}

/** 清理上次未完成的临时发布目录 */
export async function cleanupStaging(dataDir: string): Promise<void> {
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
    return entries.filter((e) => e.startsWith('pub-')).sort();
  } catch {
    return [];
  }
}

/**
 * 读取分片索引，返回值语义：
 * - PublicationIndex：分片发布，按索引定位读取；
 * - null：文件不存在，说明是旧版整版发布，走兼容读取；
 * - undefined：文件存在但无法解析或版本不符，视为发布损坏（不能静默当成旧版）。
 */
async function readIndex(dir: string): Promise<PublicationIndex | null | undefined> {
  let text: string;
  try {
    text = await readFile(path.join(dir, INDEX_FILE), 'utf-8');
  } catch {
    return null;
  }
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return undefined;
  }
  const idx = raw as Partial<PublicationIndex>;
  if (
    idx?.docType !== 'index' ||
    idx.formatVersion !== FORMAT_VERSION ||
    typeof idx.days !== 'object' ||
    idx.days === null ||
    typeof idx.connections !== 'object' ||
    idx.connections === null ||
    !Array.isArray(idx.shards)
  ) {
    return undefined;
  }
  return idx as PublicationIndex;
}

/**
 * 从整版文档推导索引（仅用于兼容旧版发布，缺少分片列表）。
 * 分片字段留空：旧版读取器永远走整版读取分支，不会据此定位分片。
 */
export function deriveIndexFromBundle(bundle: PublicationBundle): PublicationIndex {
  const accountDocs = new Map<string, DailyAccount[]>();
  for (const a of bundle.accounts) {
    const list = accountDocs.get(a.connectionId) ?? [];
    list.push(a);
    accountDocs.set(a.connectionId, list);
  }
  const quarantines = new Map(bundle.quarantines.map((q) => [q.connectionId, q] as const));
  const lineage = new Map<string, LineageRecord[]>();
  for (const l of bundle.lineage) {
    const list = lineage.get(l.connectionId) ?? [];
    list.push(l);
    lineage.set(l.connectionId, list);
  }

  const ids = new Set<string>([
    ...accountDocs.keys(),
    ...quarantines.keys(),
    ...lineage.keys(),
  ]);
  const connections: Record<string, ConnectionIndexEntry> = {};
  const days: Record<string, string[]> = {};
  for (const connectionId of [...ids].sort()) {
    const accs = (accountDocs.get(connectionId) ?? []).slice().sort((a, b) =>
      a.beijingDay.localeCompare(b.beijingDay),
    );
    const q = quarantines.get(connectionId);
    const lins = lineage.get(connectionId) ?? [];
    let startMs: number;
    let endMs: number;
    if (q) {
      startMs = q.startBeijingMs;
      endMs = q.endBeijingMs;
    } else if (accs.length > 0) {
      startMs = Math.min(...accs.map((a) => a.windowStartMs));
      endMs = Math.max(...accs.map((a) => a.windowEndMs));
    } else {
      startMs = 0;
      endMs = 0;
    }
    const connDays = [...new Set(accs.map((a) => a.beijingDay))].sort();
    for (const d of connDays) {
      (days[d] ??= []).push(connectionId);
    }
    connections[connectionId] = {
      connectionId,
      startMs,
      endMs,
      days: connDays,
      accountDocIds: accs.map((a) => a.docId),
      ...(q ? { quarantineDocId: q.docId } : {}),
      accountShards: [],
      ...(q ? { quarantineShard: quarantineShardFile(connectionId) } : {}),
      lineageShard: lineageShardFile(connectionId),
      lineageDocIds: lins.map((l) => l.docId).sort(),
    };
  }
  for (const d of Object.keys(days)) days[d]!.sort();
  return { docType: 'index', formatVersion: FORMAT_VERSION, days, connections, shards: [] };
}

/**
 * 去掉文档里的发布 id（落盘分片只存业务内容）。
 * 读取时按文档类型以旧版字段顺序注入回去，见 PublicationReader 的注入方法。
 */
export function stripPublicationId<T extends { publicationId?: string }>(
  doc: T,
): Omit<T, 'publicationId'> {
  const { publicationId: _publicationId, ...rest } = doc;
  return rest;
}

/**
 * 单次请求（或单次重跑）的发布读取器。
 *
 * - 每个 reader 固定一个发布目录与清单，读取时注入该发布的 publicationId；
 * - 分片解析结果在 reader 内缓存，避免同一请求重复读取同一分片；
 * - reader 不跨请求复用，保证快照隔离；
 * - 旧版整版发布自动走兼容读取。
 */
export class PublicationReader {
  readonly dir: string;
  readonly manifest: PublicationManifest;
  /** 磁盘上的分片索引；null 表示旧版整版发布 */
  readonly shardIndex: PublicationIndex | null;
  private counter?: DetailIoCounter;
  private shardCache = new Map<string, StoredDetail[]>();
  private legacyBundle?: PublicationBundle;
  private derived?: PublicationIndex;
  private subjectDocIndex?: Map<string, string>;

  constructor(dir: string, manifest: PublicationManifest, shardIndex: PublicationIndex | null) {
    this.dir = dir;
    this.manifest = manifest;
    this.shardIndex = shardIndex;
  }

  get publicationId(): string {
    return this.manifest.publicationId;
  }

  get isSharded(): boolean {
    return this.shardIndex !== null;
  }

  useCounter(counter?: DetailIoCounter): this {
    this.counter = counter;
    return this;
  }

  /** 索引视图：分片发布直接返回，旧版发布读取整版后推导 */
  async indexView(): Promise<PublicationIndex> {
    if (this.shardIndex) return this.shardIndex;
    if (this.derived) return this.derived;
    const bundle = await this.loadLegacy();
    this.derived = deriveIndexFromBundle(bundle);
    return this.derived;
  }

  /** 旧版整版发布：一次读入并计数 */
  private async loadLegacy(): Promise<PublicationBundle> {
    if (this.legacyBundle) return this.legacyBundle;
    const [accounts, quarantines, lineage] = await Promise.all([
      readJsonl<DailyAccount>(path.join(this.dir, 'daily-accounts.jsonl')),
      readJsonl<QuarantineEntry>(path.join(this.dir, 'quarantines.jsonl')),
      readJsonl<LineageRecord>(path.join(this.dir, 'lineage.jsonl')),
    ]);
    this.counter?.addShardRead(accounts.length, Buffer.byteLength(JSON.stringify(accounts)));
    this.counter?.addShardRead(quarantines.length, Buffer.byteLength(JSON.stringify(quarantines)));
    this.counter?.addShardRead(lineage.length, Buffer.byteLength(JSON.stringify(lineage)));
    this.legacyBundle = {
      manifest: this.manifest,
      accounts,
      quarantines,
      lineage,
    };
    return this.legacyBundle;
  }

  /** 旧版整版文档（管线迁移旧版发布时使用） */
  async legacyDocuments(): Promise<PublicationBundle> {
    return this.loadLegacy();
  }

  private async readShard(file: string): Promise<StoredDetail[]> {
    const cached = this.shardCache.get(file);
    if (cached) return cached;
    let text: string;
    try {
      text = await readFile(path.join(this.dir, file), 'utf-8');
    } catch {
      return [];
    }
    const docs = text
      .split(/\r?\n/)
      .filter((l) => l.trim().length > 0)
      .map((l) => JSON.parse(l) as StoredDetail);
    this.counter?.addShardRead(docs.length, Buffer.byteLength(text));
    this.shardCache.set(file, docs);
    return docs;
  }

  /**
   * 把发布 id 注入回明细文档，并保持旧版整版落盘的字段顺序：
   * - 日账 / 隔离：旧版以 `{ ...doc, publicationId }` 构造，发布 id 位于末尾；
   * - 血缘：旧版字面量把 publicationId 紧跟在 docId 之后。
   * 这样 HTTP 响应与改造前逐字节一致（JSON 键顺序不变）。
   */
  private injectAccount(doc: StoredDetail): DailyAccount {
    return {
      ...(doc as StoredAccount),
      publicationId: this.manifest.publicationId,
    };
  }

  private injectQuarantine(doc: StoredDetail): QuarantineEntry {
    return {
      ...(doc as StoredQuarantine),
      publicationId: this.manifest.publicationId,
    };
  }

  private injectLineage(doc: StoredDetail): LineageRecord {
    const { docType, docId, ...rest } = doc as StoredLineage;
    return {
      docType,
      docId,
      publicationId: this.manifest.publicationId,
      ...rest,
    };
  }

  async accountsForDay(day: string): Promise<DailyAccount[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return bundle.accounts.filter((a) => a.beijingDay === day);
    }
    const ids = this.shardIndex.days[day] ?? [];
    const out: DailyAccount[] = [];
    for (const id of ids) {
      const entry = this.shardIndex.connections[id];
      const file = accountShardFile(day, id);
      if (!entry || !entry.accountShards.includes(file)) continue;
      for (const d of await this.readShard(file)) out.push(this.injectAccount(d));
    }
    return sortAccounts(out);
  }

  async accountsForConnection(connectionId: string): Promise<DailyAccount[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return sortAccounts(bundle.accounts.filter((a) => a.connectionId === connectionId));
    }
    const entry = this.shardIndex.connections[connectionId];
    if (!entry) return [];
    const out: DailyAccount[] = [];
    for (const file of entry.accountShards) {
      for (const d of await this.readShard(file)) out.push(this.injectAccount(d));
    }
    return sortAccounts(out);
  }

  async allAccounts(): Promise<DailyAccount[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return sortAccounts([...bundle.accounts]);
    }
    const out: DailyAccount[] = [];
    for (const shard of this.shardIndex.shards) {
      if (!shard.file.startsWith('accounts/')) continue;
      for (const d of await this.readShard(shard.file)) out.push(this.injectAccount(d));
    }
    return sortAccounts(out);
  }

  async quarantineForConnection(connectionId: string): Promise<QuarantineEntry | null> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return bundle.quarantines.find((q) => q.connectionId === connectionId) ?? null;
    }
    const entry = this.shardIndex.connections[connectionId];
    if (!entry?.quarantineShard) return null;
    const docs = await this.readShard(entry.quarantineShard);
    return docs.length > 0 ? this.injectQuarantine(docs[0]!) : null;
  }

  async allQuarantines(): Promise<QuarantineEntry[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return [...bundle.quarantines].sort((a, b) =>
        a.connectionId.localeCompare(b.connectionId),
      );
    }
    const out: QuarantineEntry[] = [];
    for (const id of Object.keys(this.shardIndex.connections).sort()) {
      const q = await this.quarantineForConnection(id);
      if (q) out.push(q);
    }
    return out;
  }

  async lineageForConnection(connectionId: string): Promise<LineageRecord[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return sortLineage(bundle.lineage.filter((l) => l.connectionId === connectionId));
    }
    const entry = this.shardIndex.connections[connectionId];
    if (!entry) return [];
    const out: LineageRecord[] = [];
    for (const d of await this.readShard(entry.lineageShard)) out.push(this.injectLineage(d));
    return sortLineage(out);
  }

  private subjectDocMap(): Map<string, string> {
    if (this.subjectDocIndex) return this.subjectDocIndex;
    const map = new Map<string, string>();
    if (this.shardIndex) {
      for (const entry of Object.values(this.shardIndex.connections)) {
        for (const id of entry.accountDocIds) map.set(id, entry.connectionId);
        if (entry.quarantineDocId) map.set(entry.quarantineDocId, entry.connectionId);
      }
    }
    this.subjectDocIndex = map;
    return map;
  }

  async lineageForDoc(subjectDocId: string): Promise<LineageRecord[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return bundle.lineage.filter((l) => l.subjectDocId === subjectDocId);
    }
    const conn = this.subjectDocMap().get(subjectDocId);
    if (!conn) return [];
    const rows = await this.lineageForConnection(conn);
    return rows.filter((l) => l.subjectDocId === subjectDocId);
  }

  async allLineage(): Promise<LineageRecord[]> {
    if (!this.shardIndex) {
      const bundle = await this.loadLegacy();
      return sortLineage([...bundle.lineage]);
    }
    const out: LineageRecord[] = [];
    for (const id of Object.keys(this.shardIndex.connections).sort()) {
      out.push(...(await this.lineageForConnection(id)));
    }
    return sortLineage(out);
  }

  /** 整版读取（测试/演示/迁移使用） */
  async toBundle(): Promise<PublicationBundle> {
    const [accounts, quarantines, lineage] = await Promise.all([
      this.allAccounts(),
      this.allQuarantines(),
      this.allLineage(),
    ]);
    return { manifest: this.manifest, accounts, quarantines, lineage };
  }

  /** 明细读写快照（旧版整版读取也计入明细读取） */
  detailIo(): DetailIoSnapshot {
    return (
      this.counter?.snapshot() ?? {
        detailDocsRead: 0,
        detailDocsWritten: 0,
        shardBytesRead: 0,
        shardBytesWritten: 0,
        shardsRead: 0,
        shardsWritten: 0,
        shardsLinked: 0,
      }
    );
  }
}

function sortAccounts(rows: DailyAccount[]): DailyAccount[] {
  return rows.sort((a, b) =>
    a.connectionId === b.connectionId
      ? a.beijingDay.localeCompare(b.beijingDay)
      : a.connectionId.localeCompare(b.connectionId),
  );
}

function sortLineage(rows: LineageRecord[]): LineageRecord[] {
  return rows.sort((a, b) => a.docId.localeCompare(b.docId));
}

/** 打开指定发布；不存在或清单损坏时返回 null */
export async function openPublication(
  dataDir: string,
  publicationId: string,
): Promise<PublicationReader | null> {
  const dir = path.join(publicationsRoot(dataDir), publicationId);
  let manifest: PublicationManifest;
  try {
    manifest = JSON.parse(await readFile(path.join(dir, 'manifest.json'), 'utf-8')) as PublicationManifest;
  } catch {
    return null;
  }
  const index = await readIndex(dir);
  if (index === undefined) return null; // index.json 损坏：不当作旧版空发布
  return new PublicationReader(dir, manifest, index);
}

/** 打开当前发布；从未发布过时返回 null */
export async function openCurrentPublication(dataDir: string): Promise<PublicationReader | null> {
  const pointer = await readPointer(dataDir);
  if (!pointer) return null;
  const reader = await openPublication(dataDir, pointer.publicationId);
  return reader;
}

/** 读取当前（最近一次完整）发布的整版内容；从未发布过时返回 null */
export async function loadCurrentPublication(dataDir: string): Promise<PublicationBundle | null> {
  const reader = await openCurrentPublication(dataDir);
  return reader ? reader.toBundle() : null;
}

/** 读取指定发布的整版内容 */
export async function loadPublication(
  dataDir: string,
  publicationId: string,
): Promise<PublicationBundle | null> {
  const reader = await openPublication(dataDir, publicationId);
  return reader ? reader.toBundle() : null;
}

/** 已序列化、待写入的分片 */
export interface PendingShard {
  file: string;
  text: string;
  info: ShardInfo;
}

/** 从上一版硬链接复用的分片 */
export interface LinkedShard {
  file: string;
  /** 上一版发布目录内该分片的绝对路径 */
  sourceFile: string;
  info: ShardInfo;
}

/** 序列化分片并计算元数据（不落盘） */
export function prepareShard(file: string, docs: StoredDetail[]): PendingShard {
  const text = toJsonl(docs);
  const buf = Buffer.from(text, 'utf-8');
  return {
    file,
    text,
    info: {
      file,
      sha256: sha256(buf),
      docCount: docs.length,
      sizeBytes: buf.length,
    },
  };
}

/** 合并新写与复用分片的元数据，按路径排序保证指纹稳定 */
export function buildShardInfos(
  pending: PendingShard[],
  linked: LinkedShard[],
): ShardInfo[] {
  return [...pending.map((p) => p.info), ...linked.map((l) => l.info)].sort((a, b) =>
    a.file.localeCompare(b.file),
  );
}

/**
 * 原子发布（分片版）：先写临时目录（新分片实写、沿用分片硬链接），
 * 再写 index.json / manifest.json，最后 rename 目录并切换指针。
 */
export async function commitPublication(
  dataDir: string,
  bundle: {
    manifest: PublicationManifest;
    index: PublicationIndex;
    pending: PendingShard[];
    linked: LinkedShard[];
  },
  tombstoneDocIds: string[],
  counter?: DetailIoCounter,
): Promise<{ publicationId: string; skipped: false }> {
  const root = publicationsRoot(dataDir);
  await mkdir(root, { recursive: true });
  const publicationId = bundle.manifest.publicationId;
  const tmpDir = path.join(
    root,
    `${publicationId}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`,
  );
  const finalDir = path.join(root, publicationId);
  await mkdir(tmpDir, { recursive: true });

  for (const shard of bundle.pending) {
    const dest = path.join(tmpDir, shard.file);
    await mkdir(path.dirname(dest), { recursive: true });
    await writeFile(dest, shard.text);
    counter?.addShardWrite(shard.info.docCount, shard.info.sizeBytes);
  }
  for (const shard of bundle.linked) {
    const dest = path.join(tmpDir, shard.file);
    await mkdir(path.dirname(dest), { recursive: true });
    try {
      await link(shard.sourceFile, dest);
      counter?.addLinkedShard();
    } catch {
      // 文件系统不支持硬链接时退化为复制；复制属于实写，如实计入明细写入
      await copyFile(shard.sourceFile, dest);
      counter?.addShardWrite(shard.info.docCount, shard.info.sizeBytes);
    }
  }

  await writeFile(path.join(tmpDir, INDEX_FILE), JSON.stringify(bundle.index) + '\n');
  await writeFile(path.join(tmpDir, 'manifest.json'), JSON.stringify(bundle.manifest, null, 2) + '\n');

  await rename(tmpDir, finalDir);
  // 指针写入同目录临时文件后原子替换；墓碑随指针一并持久化
  const pointerTmp = path.join(root, `${CURRENT_POINTER}.tmp-${process.pid}`);
  const pointer: CurrentPointer = {
    publicationId,
    tombstoneDocIds: [...tombstoneDocIds].sort(),
  };
  await writeFile(pointerTmp, JSON.stringify(pointer, null, 2) + '\n');
  await rename(pointerTmp, pointerPath(dataDir));
  return { publicationId, skipped: false };
}

/**
 * 幂等指纹：清单稳定字段 + 索引（含全部分片哈希）的规范化哈希。
 * 分片哈希覆盖了全部明细内容，因此无需读取明细即可判断两次发布是否一致；
 * publicationId/createdAt 等易变字段被剔除。
 */
export function contentFingerprint(
  manifest: Omit<PublicationManifest, 'publicationId' | 'createdAtMs' | 'fingerprint'>,
  index: PublicationIndex,
): string {
  // 清单运行时仍带 publicationId/createdAtMs（类型上 Omit 仅为编译期约束），
  // 这里显式剔除，避免易变字段污染指纹。
  const {
    publicationId: _pubId,
    createdAtMs: _createdAt,
    fingerprint: _fp,
    ...manifestStable
  } = manifest as PublicationManifest;
  const stable = {
    manifest: stripVolatile(manifestStable),
    index: stripVolatile(index),
  };
  return createHash('sha256').update(canonicalJson(stable)).digest('hex');
}

/**
 * 递归规范化为“JSON 落盘形态”并剔除易变字段：
 * - 删除值为 undefined 的可选字段（JSON.stringify 落盘时本就会丢弃），
 *   保证内存态文档与写盘回读后的文档规范化结果一致；
 * - 删除任意层级的 publicationId，使相同业务内容不会仅因发布 id 不同而指纹不同。
 */
function stripVolatile<T>(obj: T): T {
  if (Array.isArray(obj)) {
    return obj.map((v) => stripVolatile(v)) as T;
  }
  if (obj !== null && typeof obj === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      if (k === 'publicationId' || v === undefined) continue;
      out[k] = stripVolatile(v);
    }
    return out as T;
  }
  return obj;
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => a.localeCompare(b));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

