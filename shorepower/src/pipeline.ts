/**
 * 管线编排：全量运行与指定日期重跑（分片版）。
 *
 * 明细按“接电 × 自然日”切分成分片，并用 index.json 建立“按天 / 按接电 / 按文档”
 * 的定位信息（窗口、文档 id、分片哈希）。因此：
 *
 * - 按天查询只需读取该日的日账分片；
 * - 指定日重跑只需读取索引判断纳入集合，重算涉及接电并写回其分片；
 *   未涉及的接电以硬链接复用上一版分片，不读取、不重写明细；
 * - 撤下与指纹计算只依赖索引中的文档 id 与分片哈希。
 *
 * 重跑规则（运营人员指定北京日期 D）：
 * - 纳入：最新接电时段与 D 重叠的接电，或上一版发布中接电时段与 D 重叠的接电；
 * - 对纳入的接电重新结算，在所有相关日期替换其日账/隔离/血缘；
 * - 上一版中已失效（最新输入里被更高修订取代、或不再出现）的旧接电结果随替换撤下；
 * - 其余接电沿用各自已经发布的结果（分片硬链接进入新发布，保持快照完整）。
 *
 * 旧版整版发布（无 index.json）仍可查询；若以其为当前发布重跑，会一次性读取整版
 * 明细完成迁移，之后的重跑恢复分片读写。
 */

import path from 'node:path';
import { ingest } from './ingest.js';
import { settleConnection, type ConnectionSettlement } from './settlement.js';
import { DetailIoCounter, type DetailIoSnapshot } from './metrics.js';
import {
  accountShardFile,
  buildShardInfos,
  commitPublication,
  contentFingerprint,
  lineageShardFile,
  loadTombstones,
  openCurrentPublication,
  prepareShard,
  quarantineShardFile,
  stripPublicationId,
  type ConnectionIndexEntry,
  type LinkedShard,
  type PendingShard,
  type PublicationIndex,
  type PublicationReader,
  type ShardInfo,
  type StoredAccount,
  type StoredLineage,
  type StoredQuarantine,
} from './storage.js';
import { overlapsDay } from './time.js';
import type {
  ConnectionRecord,
  DailyAccount,
  InputFileInfo,
  LineageRecord,
  PublicationManifest,
  QuarantineEntry,
  SourceRef,
} from './types.js';

export interface RunOutcome {
  publicationId: string;
  /** skipped=true 表示内容与当前发布一致，未产生新发布 */
  skipped: boolean;
  reason?: string;
  accountCount: number;
  quarantineCount: number;
  /** 血缘文档数 */
  lineageDocCount: number;
  /** 整版明细文档数（日账 + 隔离 + 血缘），用于衡量读写占比 */
  detailDocCount: number;
  /** 本次运行的明细读写计量 */
  detailIo: DetailIoSnapshot;
  recomputedConnectionIds: string[];
  carriedConnectionIds: string[];
  withdrawnDocIds: string[];
  warnings: string[];
}

function newPublicationId(mode: 'full' | 'rerun'): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
  return `pub-${mode}-${ts}-${Math.random().toString(36).slice(2, 8)}`;
}

/** 为单个结果文档构建血缘（分片内不写 publicationId，读取时注入） */
function buildLineage(
  subjectDocId: string,
  subjectKind: StoredLineage['subjectKind'],
  conn: ConnectionRecord,
  readingSources: SourceRef[],
  inputFiles: InputFileInfo[],
): StoredLineage {
  // 出处按 (文件, 行号, 记录 id) 去重，保留全部修订来源
  const keyOf = (s: SourceRef) => `${s.file}#${s.line}#${s.recordId}#${s.revision}`;
  const dedup = (srcs: SourceRef[]) => {
    const seen = new Set<string>();
    return srcs.filter((s) => {
      const k = keyOf(s);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  };
  return {
    docType: 'lineage',
    docId: `LIN-${subjectDocId}`,
    subjectDocId,
    subjectKind,
    connectionId: conn.connectionId,
    connectionSources: dedup(conn.sources),
    readingSources: dedup(readingSources),
    inputFiles: inputFiles.map((f) => ({ ...f })),
  };
}

interface BuiltConnection {
  entry: ConnectionIndexEntry;
  pending: PendingShard[];
}

/** 同主键同修订冲突：在结算隔离结果上追加 SOURCE_REVISION_CONFLICT */
function withRevisionConflict(
  conn: ConnectionRecord,
  settled: ConnectionSettlement,
): Omit<QuarantineEntry, 'publicationId'> {
  const q: Omit<QuarantineEntry, 'publicationId'> = settled.quarantine
    ? { ...settled.quarantine }
    : {
        docType: 'quarantine',
        docId: `QUAR-${conn.connectionId}`,
        connectionId: conn.connectionId,
        revision: conn.revision,
        vessel: conn.vessel,
        meterId: conn.meterId,
        startBeijingMs: conn.startBeijingMs,
        endBeijingMs: conn.endBeijingMs,
        reasons: [],
        messages: [],
        evidence: [],
      };
  if (!q.reasons.includes('SOURCE_REVISION_CONFLICT')) {
    q.reasons = [...q.reasons, 'SOURCE_REVISION_CONFLICT'];
    q.messages.push('同一来源记录的相同修订版本存在内容冲突，无法确定权威版本');
  }
  return q;
}

/** 把一次接电的结算结果切成分片，并生成索引摘要 */
function buildConnection(
  conn: ConnectionRecord,
  settled: ConnectionSettlement,
  revisionConflict: boolean,
  inputFiles: InputFileInfo[],
): BuiltConnection {
  const pending: PendingShard[] = [];
  const accountDocIds: string[] = [];
  const accountShards: string[] = [];
  const lineageDocs: StoredLineage[] = [];
  const lineageDocIds: string[] = [];
  let quarantineDocId: string | undefined;
  let quarantineShard: string | undefined;
  let days: string[] = [];

  const addLineage = (docId: string, kind: StoredLineage['subjectKind']) => {
    const lin = buildLineage(docId, kind, conn, settled.readingSources, inputFiles);
    lineageDocs.push(lin);
    lineageDocIds.push(lin.docId);
  };

  if (revisionConflict || settled.quarantine) {
    const q = revisionConflict
      ? withRevisionConflict(conn, settled)
      : settled.quarantine!;
    quarantineDocId = q.docId;
    quarantineShard = quarantineShardFile(conn.connectionId);
    pending.push(prepareShard(quarantineShard, [q as StoredQuarantine]));
    addLineage(q.docId, 'quarantine');
  } else {
    for (const acc of settled.accounts) {
      const shard = accountShardFile(acc.beijingDay, conn.connectionId);
      accountShards.push(shard);
      accountDocIds.push(acc.docId);
      pending.push(prepareShard(shard, [acc as StoredAccount]));
      addLineage(acc.docId, 'daily-account');
    }
    days = [...new Set(settled.accounts.map((a) => a.beijingDay))].sort();
  }

  const lineageShard = lineageShardFile(conn.connectionId);
  pending.push(prepareShard(lineageShard, lineageDocs));

  return {
    entry: {
      connectionId: conn.connectionId,
      startMs: conn.startBeijingMs,
      endMs: conn.endBeijingMs,
      days,
      accountDocIds,
      ...(quarantineDocId ? { quarantineDocId } : {}),
      accountShards,
      ...(quarantineShard ? { quarantineShard } : {}),
      lineageShard,
      lineageDocIds: lineageDocIds.sort(),
    },
    pending,
  };
}

/** 从告警中提取“同修订冲突”的接电主键 */
function extractRevisionConflictIds(warnings: string[]): Set<string> {
  const ids = new Set<string>();
  for (const w of warnings) {
    const m = w.match(/接电记录\s+(\S+)\s+rev=/);
    if (m) ids.add(m[1]!);
  }
  return ids;
}

/** 合并多个接电的分片，若路径重复则报错（不同接电不应共享分片路径） */
function collectPending(built: BuiltConnection[]): PendingShard[] {
  const byFile = new Map<string, PendingShard>();
  for (const b of built) {
    for (const p of b.pending) {
      if (byFile.has(p.file)) {
        throw new Error(`分片路径冲突：${p.file}`);
      }
      byFile.set(p.file, p);
    }
  }
  return [...byFile.values()];
}

/**
 * 与上一版内容相同的分片改为硬链接复用，避免重写未变化的明细。
 * 只比较索引中的 SHA-256，不读取任何明细。
 */
function linkUnchanged(
  pending: PendingShard[],
  previousShards: Map<string, ShardInfo>,
  previousDir: string | null,
): { pending: PendingShard[]; linked: LinkedShard[] } {
  if (!previousDir) return { pending, linked: [] };
  const rewritten: PendingShard[] = [];
  const linked: LinkedShard[] = [];
  for (const p of pending) {
    const info = previousShards.get(p.file);
    if (info && info.sha256 === p.info.sha256) {
      linked.push({
        file: p.file,
        sourceFile: path.join(previousDir, p.file),
        info,
      });
    } else {
      rewritten.push(p);
    }
  }
  return { pending: rewritten, linked };
}

/** 构建索引：按天反查接电，按接电定位分片与文档 id */
function buildIndex(
  entries: ConnectionIndexEntry[],
  shardInfos: ShardInfo[],
): PublicationIndex {
  const days: Record<string, string[]> = {};
  const connections: Record<string, ConnectionIndexEntry> = {};
  for (const entry of [...entries].sort((a, b) => a.connectionId.localeCompare(b.connectionId))) {
    connections[entry.connectionId] = entry;
    for (const day of entry.days) {
      (days[day] ??= []).push(entry.connectionId);
    }
  }
  for (const day of Object.keys(days)) days[day]!.sort();
  return {
    docType: 'index',
    formatVersion: 2,
    days,
    connections,
    shards: shardInfos,
  };
}

function indexDocIds(index: PublicationIndex): { accounts: string[]; quarantines: string[] } {
  const accounts: string[] = [];
  const quarantines: string[] = [];
  for (const id of Object.keys(index.connections)) {
    const entry = index.connections[id]!;
    accounts.push(...entry.accountDocIds);
    if (entry.quarantineDocId) quarantines.push(entry.quarantineDocId);
  }
  return { accounts, quarantines };
}

function countDetailDocs(shardInfos: ShardInfo[]): number {
  return shardInfos.reduce((sum, s) => sum + s.docCount, 0);
}

/** 旧版整版发布中沿用的接电：materialize 成待写分片（一次性迁移） */
function carryFromLegacy(
  legacyDocs: { accounts: DailyAccount[]; quarantines: QuarantineEntry[]; lineage: LineageRecord[] },
  ids: string[],
): BuiltConnection[] {
  const built: BuiltConnection[] = [];
  for (const id of ids) {
    const accounts = legacyDocs.accounts
      .filter((a) => a.connectionId === id)
      .sort((a, b) => a.beijingDay.localeCompare(b.beijingDay));
    const quarantine = legacyDocs.quarantines.find((q) => q.connectionId === id);
    const lineage = legacyDocs.lineage.filter((l) => l.connectionId === id);

    const pending: PendingShard[] = [];
    const accountDocIds: string[] = [];
    const accountShards: string[] = [];
    for (const acc of accounts) {
      const shard = accountShardFile(acc.beijingDay, id);
      accountShards.push(shard);
      accountDocIds.push(acc.docId);
      pending.push(prepareShard(shard, [stripPublicationId(acc) as StoredAccount]));
    }
    if (quarantine) {
      pending.push(
        prepareShard(quarantineShardFile(id), [stripPublicationId(quarantine) as StoredQuarantine]),
      );
    }
    const storedLineage = lineage.map((l) => stripPublicationId(l) as StoredLineage);
    const lineageShard = lineageShardFile(id);
    pending.push(prepareShard(lineageShard, storedLineage));

    const startMs = quarantine
      ? quarantine.startBeijingMs
      : accounts.length > 0
        ? Math.min(...accounts.map((a) => a.windowStartMs))
        : 0;
    const endMs = quarantine
      ? quarantine.endBeijingMs
      : accounts.length > 0
        ? Math.max(...accounts.map((a) => a.windowEndMs))
        : 0;
    const days = [...new Set(accounts.map((a) => a.beijingDay))].sort();
    built.push({
      entry: {
        connectionId: id,
        startMs,
        endMs,
        days,
        accountDocIds,
        ...(quarantine ? { quarantineDocId: quarantine.docId } : {}),
        accountShards,
        ...(quarantine ? { quarantineShard: quarantineShardFile(id) } : {}),
        lineageShard,
        lineageDocIds: storedLineage.map((l) => l.docId).sort(),
      },
      pending,
    });
  }
  return built;
}

interface PublishInput {
  mode: 'full' | 'rerun';
  rerunDay: string | undefined;
  inputFiles: InputFileInfo[];
  built: BuiltConnection[];
  carried: ConnectionIndexEntry[];
  /** 沿用接电的分片：分片发布走硬链接，旧版迁移走实写 */
  carriedShards: { pending: PendingShard[]; linked: LinkedShard[] };
  previous: PublicationReader | null;
  historyTombstones: string[];
  warnings: string[];
  counter: DetailIoCounter;
}

async function publish(dataDir: string, input: PublishInput): Promise<RunOutcome> {
  const publicationId = newPublicationId(input.mode);
  const previousShards = new Map<string, ShardInfo>(
    (input.previous?.shardIndex?.shards ?? []).map((s) => [s.file, s]),
  );

  const allPending = collectPending(input.built);
  const { pending, linked } = linkUnchanged(
    allPending,
    previousShards,
    input.previous?.dir ?? null,
  );
  const pendingAll = [...pending, ...input.carriedShards.pending];
  const linkedAll = [...input.carriedShards.linked, ...linked];
  const shardInfos = buildShardInfos(pendingAll, linkedAll);

  const index = buildIndex(
    [
      ...input.built.map((b) => b.entry),
      ...input.carried.map((e) => JSON.parse(JSON.stringify(e)) as ConnectionIndexEntry),
    ],
    shardInfos,
  );

  const accountCount = Object.values(index.connections).reduce(
    (n, e) => n + e.accountDocIds.length,
    0,
  );
  const quarantineCount = Object.values(index.connections).filter(
    (e) => e.quarantineDocId,
  ).length;
  const detailDocCount = countDetailDocs(shardInfos);
  const lineageDocCount = shardInfos
    .filter((s) => s.file.startsWith('lineage/'))
    .reduce((n, s) => n + s.docCount, 0);

  const recomputedConnectionIds = input.built.map((b) => b.entry.connectionId).sort();
  const carriedConnectionIds = input.carried.map((e) => e.connectionId).sort();

  const currentDocIds = indexDocIds(index);
  const retainedDocIds = new Set<string>([
    ...currentDocIds.accounts,
    ...currentDocIds.quarantines,
  ]);
  const previousDocIds = input.previous
    ? indexDocIds(await input.previous.indexView())
    : { accounts: [], quarantines: [] };
  const newWithdrawals = [
    ...previousDocIds.accounts,
    ...previousDocIds.quarantines,
  ].filter((id) => !retainedDocIds.has(id));
  const withdrawnDocIds = [
    ...new Set([...input.historyTombstones, ...newWithdrawals]),
  ].sort();

  const manifestWithoutFingerprint: Omit<PublicationManifest, 'fingerprint'> = {
    docType: 'manifest',
    publicationId,
    createdAtMs: Date.now(),
    mode: input.mode,
    rerunDay: input.rerunDay,
    inputFiles: input.inputFiles,
    accountCount,
    quarantineCount,
    recomputedConnectionIds,
    carriedConnectionIds,
    withdrawnDocIds,
    warnings: input.warnings,
  };

  const fingerprint = contentFingerprint(manifestWithoutFingerprint, index);
  const manifest: PublicationManifest = { ...manifestWithoutFingerprint, fingerprint };

  // 幂等：与当前发布业务内容完全一致时不产生新发布（也不写任何明细）
  if (input.previous && input.previous.manifest.fingerprint === fingerprint) {
    return {
      publicationId: input.previous.manifest.publicationId,
      skipped: true,
      reason: '相同输入与结算结果，内容指纹一致，跳过重复发布',
      accountCount: input.previous.manifest.accountCount,
      quarantineCount: input.previous.manifest.quarantineCount,
      lineageDocCount,
      detailDocCount,
      detailIo: input.counter.snapshot(),
      recomputedConnectionIds,
      carriedConnectionIds,
      withdrawnDocIds,
      warnings: input.warnings,
    };
  }

  await commitPublication(
    dataDir,
    { manifest, index, pending: pendingAll, linked: linkedAll },
    withdrawnDocIds,
    input.counter,
  );
  return {
    publicationId,
    skipped: false,
    accountCount,
    quarantineCount,
    lineageDocCount,
    detailDocCount,
    detailIo: input.counter.snapshot(),
    recomputedConnectionIds,
    carriedConnectionIds,
    withdrawnDocIds,
    warnings: input.warnings,
  };
}

/** 全量运行：重新结算输入中的全部接电 */
export async function runFull(inputDir: string, dataDir: string): Promise<RunOutcome> {
  const counter = new DetailIoCounter();
  const [{ connections, readings, files, warnings }, previous, historyTombstones] =
    await Promise.all([ingest(inputDir), openCurrentPublication(dataDir), loadTombstones(dataDir)]);
  const conflictIds = extractRevisionConflictIds(warnings);

  const built = connections.map((conn) =>
    buildConnection(
      conn,
      settleConnection(conn, readings),
      conflictIds.has(conn.connectionId),
      files,
    ),
  );

  return publish(dataDir, {
    mode: 'full',
    rerunDay: undefined,
    inputFiles: files,
    built,
    carried: [],
    carriedShards: { pending: [], linked: [] },
    previous,
    historyTombstones,
    warnings,
    counter,
  });
}

/** 指定北京时间日期重跑 */
export async function rerunDay(
  inputDir: string,
  dataDir: string,
  day: string,
): Promise<RunOutcome> {
  const counter = new DetailIoCounter();
  const [{ connections, readings, files, warnings }, previous, historyTombstones] =
    await Promise.all([ingest(inputDir), openCurrentPublication(dataDir), loadTombstones(dataDir)]);

  if (!previous) {
    // 没有历史发布可沿用，退化为全量运行
    return runFull(inputDir, dataDir);
  }

  const conflictIds = extractRevisionConflictIds(warnings);
  const latestById = new Map(connections.map((c) => [c.connectionId, c]));
  const previousIndex = await previous.indexView();

  // 纳入集合：最新接电时段与该日重叠，或上一版发布接电时段与该日重叠
  const includedIds = new Set<string>();
  for (const c of connections) {
    if (overlapsDay(c.startBeijingMs, c.endBeijingMs, day)) includedIds.add(c.connectionId);
  }
  for (const entry of Object.values(previousIndex.connections)) {
    if (overlapsDay(entry.startMs, entry.endMs, day)) includedIds.add(entry.connectionId);
  }

  const includedConnections: ConnectionRecord[] = [];
  const missingIncluded: string[] = [];
  for (const id of includedIds) {
    const latest = latestById.get(id);
    if (latest) includedConnections.push(latest);
    else missingIncluded.push(id); // 最新输入已无此接电：其旧结果将被撤下
  }

  const built = includedConnections.map((conn) =>
    buildConnection(
      conn,
      settleConnection(conn, readings),
      conflictIds.has(conn.connectionId),
      files,
    ),
  );

  // 沿用：上一版中不属于纳入集合的接电
  const carriedIds = Object.keys(previousIndex.connections)
    .filter((id) => !includedIds.has(id))
    .sort();

  let carried: ConnectionIndexEntry[];
  let carriedShards: { pending: PendingShard[]; linked: LinkedShard[] };

  if (previous.isSharded) {
    // 分片发布：索引里已有窗口、文档 id 与分片哈希，无需读取旧明细
    const shardInfos = new Map(previousIndex.shards.map((s) => [s.file, s]));
    carried = carriedIds.map(
      (id) => JSON.parse(JSON.stringify(previousIndex.connections[id])) as ConnectionIndexEntry,
    );
    const linked: LinkedShard[] = [];
    for (const entry of carried) {
      const files = [
        ...entry.accountShards,
        ...(entry.quarantineShard ? [entry.quarantineShard] : []),
        entry.lineageShard,
      ];
      for (const file of files) {
        const info = shardInfos.get(file);
        if (!info) throw new Error(`上一版索引缺少分片 ${file}，发布可能已损坏`);
        linked.push({
          file,
          sourceFile: path.join(previous.dir, file),
          info,
        });
      }
    }
    carriedShards = { pending: [], linked };
  } else {
    // 旧版整版发布：一次性读取旧明细并迁移为分片（仅此一次）
    const legacyDocs = await previous.legacyDocuments();
    const legacyBuilt = carryFromLegacy(legacyDocs, carriedIds);
    carried = legacyBuilt.map((b) => b.entry);
    carriedShards = { pending: collectPending(legacyBuilt), linked: [] };
  }

  if (missingIncluded.length > 0) {
    warnings.push(
      `以下接电在上一版发布中与 ${day} 重叠，但最新输入已不存在，旧结果已撤下：${missingIncluded.join(', ')}`,
    );
  }

  return publish(dataDir, {
    mode: 'rerun',
    rerunDay: day,
    inputFiles: files,
    built,
    carried,
    carriedShards,
    previous,
    historyTombstones,
    warnings,
    counter,
  });}
