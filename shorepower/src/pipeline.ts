/**
 * 管线编排：首次导入、补发、指定日期重跑、撤销来源批次。
 *
 * 核心模型：输入按“可撤销来源批次”组织。批次台账与批次内的原始记录随发布一起
 * 落盘（batches.jsonl / batch-records.jsonl），因此投递文件在发布成功后可被清走，
 * 而撤销、重试仍能仅凭台账完成：
 *
 * - 每次纳入新投递文件都会登记一个批次（首次导入 / 补发 / 更正）；
 * - 结算永远在“全部有效批次”上按主键选取最高修订，后到批次自然覆盖先到批次；
 * - 撤销批次时不扫描输入目录，只读被撤批次涉及的主键版本与相关接电的结算输入，
 *   按剩余有效批次重新选修订，只重算真正受影响的接电；
 * - 批次状态与日账、隔离、血缘写入同一个不可变发布目录，随指针一次切换，
 *   因此撤销要么整体生效，要么完全不生效，中断后重试即可。
 */

import { rmdir, rm } from 'node:fs/promises';
import path from 'node:path';
import {
  activeFiles,
  affectedConnectionIds,
  deriveBatchId,
  isActive,
  makeBatch,
  revokeScope,
  selectEffective,
  sortBatchRecords,
  sortBatches,
  type SelectionScope,
} from './batches.js';
import { ingest, type IngestResult } from './ingest.js';
import { settleConnection } from './settlement.js';
import {
  commitPublication,
  contentFingerprint,
  loadCurrentPublication,
  loadTombstones,
  type CommitHooks,
  type PublicationBundle,
} from './storage.js';
import { overlapsDay } from './time.js';
import type {
  BatchRecord,
  ConnectionRecord,
  DailyAccount,
  InputFileInfo,
  LineageRecord,
  MeterReading,
  PublicationManifest,
  QuarantineEntry,
  RevokeRequest,
  RevokeOutcome,
  SourceBatch,
  SourceBatchKind,
  SourceRef,
} from './types.js';

export interface RunOutcome {
  publicationId: string;
  /** skipped=true 表示内容与当前发布一致，未产生新发布 */
  skipped: boolean;
  reason?: string;
  accountCount: number;
  quarantineCount: number;
  recomputedConnectionIds: string[];
  carriedConnectionIds: string[];
  withdrawnDocIds: string[];
  activeBatchIds: string[];
  revokedBatchIds: string[];
  /** 本次登记（或复用）的批次 */
  batchId?: string;
  /** 发布成功后清走的投递文件 */
  cleanedFiles: string[];
  warnings: string[];
}

/** 发布模式（补发与更正使用同一套原子发布） */
type PublishMode = PublicationManifest['mode'];

const nowMs = (): number => Date.now();

function newPublicationId(mode: PublishMode): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
  return `pub-${mode}-${ts}-${Math.random().toString(36).slice(2, 8)}`;
}

// ────────────────────────────────────────────────────────────────────────────
// 批次台账
// ────────────────────────────────────────────────────────────────────────────

interface Ledger {
  batches: SourceBatch[];
  records: BatchRecord[];
}

function ledgerOf(previous: PublicationBundle | null): Ledger {
  return {
    batches: previous ? previous.batches.map((b) => ({ ...b, files: b.files.map((f) => ({ ...f })) })) : [],
    records: previous ? previous.batchRecords.map((r) => r) : [],
  };
}

function activeRecordKeys(ledger: Ledger): { connections: Set<string>; readings: Set<string> } {
  const activeIds = new Set(ledger.batches.filter(isActive).map((b) => b.batchId));
  const connections = new Set<string>();
  const readings = new Set<string>();
  for (const r of ledger.records) {
    if (!activeIds.has(r.batchId)) continue;
    if (r.kind === 'connection') connections.add((r.record as ConnectionRecord).connectionId);
    else readings.add((r.record as MeterReading).readingId);
  }
  return { connections, readings };
}

/** 若同一组投递文件已登记过，复用原批次（保证同样输入重跑不产生新批次） */
function findBatchByDelivery(batches: SourceBatch[], files: InputFileInfo[]): SourceBatch | undefined {
  const signature = (fs: InputFileInfo[]) =>
    fs.map((f) => f.sha256).sort().join(',');
  const target = signature(files);
  return batches.find((b) => signature(b.files) === target);
}

/**
 * 批次 id 由内容派生；若相同内容的批次此前已被撤销，则追加确定性序号区分，避免撤销后
 * 重新投递同一批文件时与已撤销批次 id 冲突（那样会“复活”旧批次），同时保证重试稳定。
 */
function uniqueBatchId(kind: SourceBatchKind, files: InputFileInfo[], existing: SourceBatch[]): string {
  const base = deriveBatchId(kind, files);
  let candidate = base;
  let seq = 1;
  while (existing.some((b) => b.batchId === candidate)) {
    candidate = `${base}-${seq}`;
    seq++;
  }
  return candidate;
}

function stampSources(sources: SourceRef[], batchId: string): SourceRef[] {
  return sources.map((s) => ({ ...s, batchId }));
}

function stampConnection(conn: ConnectionRecord, batchId: string): ConnectionRecord {
  return { ...conn, sources: stampSources(conn.sources, batchId) };
}

function stampReading(reading: MeterReading, batchId: string): MeterReading {
  return { ...reading, sources: stampSources(reading.sources, batchId) };
}

interface DeliveryRegistration {
  ledger: Ledger;
  /** 本次实际新登记的批次；全为重复投递时为 undefined */
  batch?: SourceBatch;
  /** 本次投递的文件（用于发布成功后清走） */
  deliveredFiles: string[];
  /** 是否因为新批次改变了台账 */
  changed: boolean;
}

function contentKey(kind: 'connection' | 'reading', record: ConnectionRecord | MeterReading): string {
  if (kind === 'connection') {
    const c = record as ConnectionRecord;
    return [
      'connection',
      c.connectionId,
      c.revision,
      c.vessel,
      c.meterId,
      c.startBeijingMs,
      c.endBeijingMs,
    ].join('|');
  }
  const r = record as MeterReading;
  return ['reading', r.readingId, r.revision, r.meterId, r.sampledAtMs, r.cumulativeWh, r.originalUnit].join('|');
}

/** 有效批次中已存在的记录内容（用于识别重送，避免登记纯重复批次） */
function existingContentKeys(ledger: Ledger): Set<string> {
  const active = new Set(ledger.batches.filter(isActive).map((b) => b.batchId));
  const keys = new Set<string>();
  for (const r of ledger.records) {
    if (!active.has(r.batchId)) continue;
    keys.add(contentKey(r.kind, r.record));
  }
  return keys;
}

function activeFileHashes(ledger: Ledger): Set<string> {
  const hashes = new Set<string>();
  for (const b of ledger.batches) {
    if (!isActive(b)) continue;
    for (const f of b.files) hashes.add(f.sha256);
  }
  return hashes;
}

/**
 * 把本批投递文件登记为可撤销来源批次。
 *
 * - 文件内容（SHA-256）已在有效批次中出现：视为重送，不再登记；
 * - 单条记录内容已在有效批次中存在：忽略该条，避免撤销时留下无意义的重复来源；
 * - 所有投递内容都是重复时，不创建批次、不改变台账。
 */
function registerDelivery(ingestResult: IngestResult, previous: Ledger, at: number): DeliveryRegistration {
  const deliveredFiles = ingestResult.files.map((f) => f.file);

  if (ingestResult.files.length === 0) {
    return { ledger: previous, deliveredFiles, changed: false };
  }

  const knownHashes = activeFileHashes(previous);
  const knownContent = existingContentKeys(previous);

  // 字节级重送但换了文件名：作为别名登记到拥有该内容的有效批次，清单里保留
  // `duplicateOf` 以便审计“重送文件被识别”，但不重复产生记录。
  let aliasLedger = previous;
  let aliasChanged = false;
  const knownPaths = new Set<string>();
  for (const b of previous.batches) {
    if (isActive(b)) for (const f of b.files) knownPaths.add(f.file);
  }
  const aliasFiles = ingestResult.files.filter(
    (f) => knownHashes.has(f.sha256) && !knownPaths.has(f.file),
  );
  if (aliasFiles.length > 0) {
    const batches = previous.batches.map((b) => ({ ...b, files: b.files.map((f) => ({ ...f })) }));
    for (const alias of aliasFiles) {
      const owner = batches.find(
        (b) => isActive(b) && b.files.some((f) => f.sha256 === alias.sha256 && f.duplicateOf === undefined),
      );
      if (!owner) continue;
      const original = owner.files.find((f) => f.sha256 === alias.sha256 && f.duplicateOf === undefined)!;
      owner.files = [
        ...owner.files,
        { ...alias, batchId: owner.batchId, duplicateOf: original.file },
      ].sort((x, y) => x.file.localeCompare(y.file));
      aliasChanged = true;
    }
    aliasLedger = { batches: sortBatches(batches), records: previous.records };
  }

  // 只登记内容尚未出现过的文件
  const newGroups = ingestResult.fileGroups.filter((g) => !knownHashes.has(g.file.sha256));
  if (newGroups.length === 0) {
    const existing = findBatchByDelivery(aliasLedger.batches, ingestResult.files);
    return { ledger: aliasLedger, batch: existing, deliveredFiles, changed: aliasChanged };
  }

  // 只保留内容真正新增的记录
  const freshGroups = newGroups.map((g) => ({
    file: g.file,
    connections: g.connections.filter((c) => !knownContent.has(contentKey('connection', c))),
    readings: g.readings.filter((r) => !knownContent.has(contentKey('reading', r))),
  }));
  const hasFreshRecords = freshGroups.some(
    (g) => g.connections.length > 0 || g.readings.length > 0,
  );
  if (!hasFreshRecords) {
    const existing = findBatchByDelivery(aliasLedger.batches, ingestResult.files);
    return { ledger: aliasLedger, batch: existing, deliveredFiles, changed: aliasChanged };
  }

  const freshFiles = freshGroups.map((g) => g.file);
  const { connections: knownConnections, readings: knownReadings } = activeRecordKeys(aliasLedger);
  const hasActive = previous.batches.some(isActive);
  let kind: SourceBatchKind;
  if (!hasActive) {
    kind = 'initial-import';
  } else {
    const touchesKnown =
      freshGroups.some(
        (g) =>
          g.connections.some((c) => knownConnections.has(c.connectionId)) ||
          g.readings.some((r) => knownReadings.has(r.readingId)),
      );
    kind = touchesKnown ? 'correction' : 'supplement';
  }

  const batchId = uniqueBatchId(kind, freshFiles, aliasLedger.batches);
  const batch = makeBatch(batchId, kind, freshFiles, at);

  // 同一次投递内的字节级重送：内容由首个文件代表，重送文件作为批次内别名登记，
  // 让清单仍能显示“重送文件被识别”，但不重复携带记录。
  const primaryByHash = new Map(freshFiles.map((f) => [f.sha256, f.file]));
  const intraAliases = ingestResult.files.filter(
    (f) => f.duplicateOf !== undefined && primaryByHash.get(f.sha256) !== f.file && primaryByHash.has(f.sha256),
  );
  if (intraAliases.length > 0) {
    batch.files = [
      ...batch.files,
      ...intraAliases.map((f) => ({
        ...f,
        batchId,
        duplicateOf: primaryByHash.get(f.sha256)!,
      })),
    ].sort((x, y) => x.file.localeCompare(y.file));
  }

  const records: BatchRecord[] = [];
  for (const group of freshGroups) {
    for (const c of group.connections) {
      records.push({ batchId, kind: 'connection', record: stampConnection(c, batchId) });
    }
    for (const r of group.readings) {
      records.push({ batchId, kind: 'reading', record: stampReading(r, batchId) });
    }
  }

  return {
    ledger: {
      batches: sortBatches([...aliasLedger.batches, batch]),
      records: sortBatchRecords([...aliasLedger.records, ...records]),
    },
    batch,
    deliveredFiles,
    changed: true,
  };
}

// ────────────────────────────────────────────────────────────────────────────
// 结算与血缘
// ────────────────────────────────────────────────────────────────────────────

/** 为单个接电构建血缘文档（日账与隔离各一条） */
function buildLineage(
  publicationId: string,
  subjectDocId: string,
  subjectKind: LineageRecord['subjectKind'],
  conn: ConnectionRecord,
  readingSources: SourceRef[],
  inputFiles: InputFileInfo[],
): LineageRecord {
  const keyOf = (s: SourceRef) => `${s.file}#${s.line}#${s.recordId}#${s.revision}#${s.batchId ?? ''}`;
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
    publicationId,
    subjectDocId,
    subjectKind,
    connectionId: conn.connectionId,
    connectionSources: dedup(conn.sources),
    readingSources: dedup(readingSources),
    inputFiles: inputFiles.map((f) => ({ ...f })),
  };
}

interface Recomputed {
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
  connectionIds: string[];
}

function recompute(
  connections: ConnectionRecord[],
  readings: MeterReading[],
  inputFiles: InputFileInfo[],
  publicationId: string,
  revisionConflictIds: Set<string>,
): Recomputed {
  const out: Recomputed = { accounts: [], quarantines: [], lineage: [], connectionIds: [] };

  for (const conn of connections) {
    out.connectionIds.push(conn.connectionId);
    const settled = settleConnection(conn, readings);

    if (revisionConflictIds.has(conn.connectionId)) {
      const q: QuarantineEntry = settled.quarantine
        ? { ...settled.quarantine, publicationId }
        : {
            docType: 'quarantine',
            docId: `QUAR-${conn.connectionId}`,
            publicationId,
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
      out.quarantines.push(q);
      out.lineage.push(
        buildLineage(publicationId, q.docId, 'quarantine', conn, settled.readingSources, inputFiles),
      );
      continue;
    }

    if (settled.quarantine) {
      const q: QuarantineEntry = { ...settled.quarantine, publicationId };
      out.quarantines.push(q);
      out.lineage.push(
        buildLineage(publicationId, q.docId, 'quarantine', conn, settled.readingSources, inputFiles),
      );
      continue;
    }

    for (const acc of settled.accounts) {
      const stamped: DailyAccount = { ...acc, publicationId };
      out.accounts.push(stamped);
      out.lineage.push(
        buildLineage(publicationId, stamped.docId, 'daily-account', conn, settled.readingSources, inputFiles),
      );
    }
  }

  out.connectionIds.sort();
  return out;
}

// ────────────────────────────────────────────────────────────────────────────
// 发布
// ────────────────────────────────────────────────────────────────────────────

interface CommitInput {
  dataDir: string;
  mode: PublishMode;
  rerunDay?: string;
  inputFiles: InputFileInfo[];
  ledger: Ledger;
  recomputed: Recomputed;
  carried: { accounts: DailyAccount[]; quarantines: QuarantineEntry[]; lineage: LineageRecord[] };
  carriedIds: string[];
  withdrawnDocIds: string[];
  warnings: string[];
  previous: PublicationBundle | null;
  historyTombstones: string[];
  commitHooks?: CommitHooks;
}

/** 递归替换所有层级的 publicationId（含血缘等嵌套结构） */
function restamp<T>(value: T, publicationId: string): T {
  if (Array.isArray(value)) {
    return value.map((v) => restamp(v, publicationId)) as T;
  }
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = k === 'publicationId' ? publicationId : restamp(v, publicationId);
    }
    return out as T;
  }
  return value;
}

async function commitResult(input: CommitInput): Promise<RunOutcome> {
  const publicationId = newPublicationId(input.mode);
  const accounts = restamp([...input.recomputed.accounts, ...input.carried.accounts], publicationId);
  const quarantines = restamp(
    [...input.recomputed.quarantines, ...input.carried.quarantines],
    publicationId,
  );
  const lineage = restamp([...input.recomputed.lineage, ...input.carried.lineage], publicationId);

  // 撤下清单 = 历史墓碑 ∪ 本次撤下，只增不减
  const withdrawnDocIds = [...new Set([...input.historyTombstones, ...input.withdrawnDocIds])].sort();

  const activeBatchIds = input.ledger.batches.filter(isActive).map((b) => b.batchId).sort();
  const revokedBatchIds = input.ledger.batches
    .filter((b) => b.status === 'revoked')
    .map((b) => b.batchId)
    .sort();

  const manifestWithoutFingerprint: Omit<PublicationManifest, 'fingerprint'> = {
    docType: 'manifest',
    publicationId,
    createdAtMs: nowMs(),
    mode: input.mode,
    rerunDay: input.rerunDay,
    inputFiles: input.inputFiles,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    activeBatchIds,
    revokedBatchIds,
    recomputedConnectionIds: [...input.recomputed.connectionIds].sort(),
    carriedConnectionIds: [...input.carriedIds].sort(),
    withdrawnDocIds,
    warnings: input.warnings,
  };

  const fingerprint = contentFingerprint({
    manifest: manifestWithoutFingerprint,
    accounts,
    quarantines,
    lineage,
    batches: input.ledger.batches,
    batchRecords: input.ledger.records,
  });
  const manifest: PublicationManifest = { ...manifestWithoutFingerprint, fingerprint };

  const base: Omit<RunOutcome, 'publicationId' | 'skipped'> = {
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    recomputedConnectionIds: [...input.recomputed.connectionIds].sort(),
    carriedConnectionIds: [...input.carriedIds].sort(),
    withdrawnDocIds,
    activeBatchIds,
    revokedBatchIds,
    cleanedFiles: [],
    warnings: input.warnings,
  };

  if (input.previous && input.previous.manifest.fingerprint === fingerprint) {
    return {
      ...base,
      publicationId: input.previous.manifest.publicationId,
      skipped: true,
      reason: '相同批次状态与结算结果，内容指纹一致，跳过重复发布',
    };
  }

  const bundle: PublicationBundle = {
    manifest,
    accounts,
    quarantines,
    lineage,
    batches: input.ledger.batches,
    batchRecords: input.ledger.records,
  };
  const tombstoneDocIds = withdrawnDocIds;
  await commitPublication(input.dataDir, bundle, tombstoneDocIds, input.commitHooks);
  return { ...base, publicationId, skipped: false };
}

/** 旧发布文档中不再保留的部分进入墓碑（与历史墓碑累积） */
function mergeWithdrawals(
  previous: PublicationBundle | null,
  retainedDocIds: Set<string>,
  historyTombstones: string[] = [],
): string[] {
  const newWithdrawals = previous
    ? [...previous.accounts, ...previous.quarantines]
        .map((d) => d.docId)
        .filter((id) => !retainedDocIds.has(id))
    : [];
  return [...new Set([...historyTombstones, ...newWithdrawals])].sort();
}

// ────────────────────────────────────────────────────────────────────────────
// 投递文件清理
// ────────────────────────────────────────────────────────────────────────────

/**
 * 发布成功后清走投递文件。
 * 只删除本次实际纳入批次的文件，且严格限制在输入目录内；批次记录与文件哈希已随
 * 发布落盘，因此清走后撤销、重试仍可完整进行。
 */
async function cleanDeliveredFiles(inputDir: string, files: string[]): Promise<string[]> {
  const root = path.resolve(inputDir);
  const cleaned: string[] = [];
  for (const rel of files) {
    const abs = path.resolve(root, rel);
    if (abs !== root && !abs.startsWith(root + path.sep)) continue;
    try {
      await rm(abs, { force: true });
      cleaned.push(rel);
      // 自底向上清理空目录，保持交付目录干净
      let dir = path.dirname(abs);
      while (dir !== root && dir.startsWith(root + path.sep)) {
        try {
          await rmdir(dir);
        } catch {
          break;
        }
        dir = path.dirname(dir);
      }
    } catch {
      // 单个文件清理失败不影响已完成的发布，重试或人工处理即可
    }
  }
  return cleaned.sort();
}

// ────────────────────────────────────────────────────────────────────────────
// 对外入口
// ────────────────────────────────────────────────────────────────────────────

export interface RunOptions {
  /** 发布成功后清走投递文件（默认 true） */
  cleanDelivery?: boolean;
  /** 故障注入（测试用） */
  commitHooks?: CommitHooks;
}

/**
 * 首次导入 / 全量运行：把投递文件登记为批次，并基于全部有效批次重新结算。
 *
 * 结算只依赖已落盘的批次台账，不依赖输入目录，因此投递文件被清走后再次 run
 * 仍得到完整结果（而不会因为目录为空把既有日账清空）。
 */
export async function runFull(
  inputDir: string,
  dataDir: string,
  options: RunOptions = {},
): Promise<RunOutcome> {
  const [ingested, previous, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);
  const previousLedger = ledgerOf(previous);
  const registration = registerDelivery(ingested, previousLedger, nowMs());
  const ledger = registration.ledger;

  // 旧版发布没有批次台账，且本次也没有投递文件：无法重建来源，拒绝静默清空既有结果
  if (
    previous &&
    previous.batches.length === 0 &&
    (previous.accounts.length > 0 || previous.quarantines.length > 0) &&
    registration.deliveredFiles.length === 0
  ) {
    throw new Error(
      '当前发布早于批次台账机制且输入目录为空，无法重建来源；' +
        '请先重新投递原始文件（会登记为首次导入批次）再执行 run',
    );
  }

  const selection = selectEffective(ledger.batches, ledger.records);
  const recomputed = recompute(
    selection.connections,
    selection.readings,
    activeFiles(ledger.batches),
    '__pending__',
    selection.connectionConflictIds,
  );
  const retained = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
  ]);

  const outcome = await commitResult({
    dataDir,
    mode: 'full',
    inputFiles: activeFiles(ledger.batches),
    ledger,
    recomputed,
    carried: { accounts: [], quarantines: [], lineage: [] },
    carriedIds: [],
    withdrawnDocIds: mergeWithdrawals(previous, retained, historyTombstones),
    warnings: [...ingested.warnings, ...selection.warnings],
    previous,
    historyTombstones,
    commitHooks: options.commitHooks,
  });

  outcome.batchId = registration.batch?.batchId;
  if (options.cleanDelivery !== false) {
    outcome.cleanedFiles = await cleanDeliveredFiles(inputDir, registration.deliveredFiles);
  }
  return outcome;
}

/**
 * 补发 / 更正：把新投递文件登记为批次，只重算被它实际影响的接电。
 * 未被影响的接电、以及此前已生效的更正，原样沿用。
 */
export async function supplement(
  inputDir: string,
  dataDir: string,
  options: RunOptions = {},
): Promise<RunOutcome> {
  const [ingested, previous, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);

  if (!previous) {
    // 没有历史发布：就是首次导入
    return runFull(inputDir, dataDir, options);
  }

  const previousLedger = ledgerOf(previous);
  const registration = registerDelivery(ingested, previousLedger, nowMs());
  const ledger = registration.ledger;

  // 新增批次涉及的主键与相关电表（只读这些范围）
  let affectedIds: string[] = [];
  let scope: SelectionScope | undefined;
  if (registration.batch) {
    const scopeKeys = revokeScope(ledger.records, registration.batch.batchId);
    scope = { connectionIds: scopeKeys.connectionIds, meterIds: scopeKeys.meterIds };
    const before = selectEffective(previousLedger.batches, previousLedger.records, scope);
    const after = selectEffective(ledger.batches, ledger.records, scope);
    affectedIds = affectedConnectionIds(before, after, scopeKeys.connectionIds);
  }

  // 重复投递且无任何业务变化：不产生新发布，但仍清走多余投递文件
  if (!registration.changed && affectedIds.length === 0) {
    const outcome: RunOutcome = {
      publicationId: previous.manifest.publicationId,
      skipped: true,
      reason: '投递内容均已存在且结算结果无变化，跳过重复发布',
      accountCount: previous.manifest.accountCount,
      quarantineCount: previous.manifest.quarantineCount,
      recomputedConnectionIds: [],
      carriedConnectionIds: previous.manifest.carriedConnectionIds,
      withdrawnDocIds: [],
      activeBatchIds: previous.manifest.activeBatchIds,
      revokedBatchIds: previous.manifest.revokedBatchIds,
      batchId: registration.batch?.batchId,
      cleanedFiles: [],
      warnings: [...ingested.warnings],
    };
    if (options.cleanDelivery !== false) {
      outcome.cleanedFiles = await cleanDeliveredFiles(inputDir, registration.deliveredFiles);
    }
    return outcome;
  }

  const paths = buildIncremental(previous, ledger, affectedIds, scope);

  const outcome = await commitResult({
    dataDir,
    mode: registration.batch?.kind === 'correction' ? 'correction' : 'supplement',
    inputFiles: activeFiles(ledger.batches),
    ledger,
    recomputed: paths.recomputed,
    carried: paths.carried,
    carriedIds: paths.carriedIds,
    withdrawnDocIds: paths.withdrawnDocIds,
    warnings: [...ingested.warnings, ...paths.selectionWarnings],
    previous,
    historyTombstones,
    commitHooks: options.commitHooks,
  });

  outcome.batchId = registration.batch?.batchId;
  if (options.cleanDelivery !== false) {
    outcome.cleanedFiles = await cleanDeliveredFiles(inputDir, registration.deliveredFiles);
  }
  return outcome;
}

/** 指定北京时间日期重跑 */
export async function rerunDay(
  inputDir: string,
  dataDir: string,
  day: string,
  options: RunOptions = {},
): Promise<RunOutcome> {
  const [ingested, previous, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);

  if (!previous) {
    return runFull(inputDir, dataDir, options);
  }

  const previousLedger = ledgerOf(previous);
  const registration = registerDelivery(ingested, previousLedger, nowMs());
  const ledger = registration.ledger;
  const selection = selectEffective(ledger.batches, ledger.records);
  const latestById = new Map(selection.connections.map((c) => [c.connectionId, c]));

  // 纳入集合：有效接电时段与该日重叠，或上一版发布中接电时段与该日重叠
  const includedIds = new Set<string>();
  for (const c of selection.connections) {
    if (overlapsDay(c.startBeijingMs, c.endBeijingMs, day)) includedIds.add(c.connectionId);
  }
  const previousWindows = previousConnectionWindows(previous);
  for (const [id, w] of previousWindows) {
    if (overlapsDay(w.start, w.end, day)) includedIds.add(id);
  }

  const includedConnections: ConnectionRecord[] = [];
  const missingIncluded: string[] = [];
  for (const id of includedIds) {
    const latest = latestById.get(id);
    if (latest) includedConnections.push(latest);
    else missingIncluded.push(id);
  }

  const files = activeFiles(ledger.batches);
  const recomputed = recompute(
    includedConnections,
    selection.readings,
    files,
    '__pending__',
    selection.connectionConflictIds,
  );

  const carriedAccounts = previous.accounts.filter((a) => !includedIds.has(a.connectionId));
  const carriedQuarantines = previous.quarantines.filter((q) => !includedIds.has(q.connectionId));
  const carriedLineage = previous.lineage.filter((l) => !includedIds.has(l.connectionId));
  const carriedIds = [
    ...new Set([...carriedAccounts, ...carriedQuarantines].map((d) => d.connectionId)),
  ].sort();

  const retained = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
    ...carriedAccounts.map((d) => d.docId),
    ...carriedQuarantines.map((d) => d.docId),
  ]);

  const warnings = [...ingested.warnings, ...selection.warnings];
  if (missingIncluded.length > 0) {
    warnings.push(
      `以下接电在上一版发布中与 ${day} 重叠，但当前有效批次中已不存在，旧结果已撤下：${missingIncluded.join(', ')}`,
    );
  }

  const outcome = await commitResult({
    dataDir,
    mode: 'rerun',
    rerunDay: day,
    inputFiles: files,
    ledger,
    recomputed,
    carried: { accounts: carriedAccounts, quarantines: carriedQuarantines, lineage: carriedLineage },
    carriedIds,
    withdrawnDocIds: mergeWithdrawals(previous, retained, historyTombstones),
    warnings,
    previous,
    historyTombstones,
    commitHooks: options.commitHooks,
  });

  outcome.batchId = registration.batch?.batchId;
  if (options.cleanDelivery !== false) {
    outcome.cleanedFiles = await cleanDeliveredFiles(inputDir, registration.deliveredFiles);
  }
  return outcome;
}

/** 从上一版发布的日账/隔离反推接电窗口（用于重跑纳入判断） */
function previousConnectionWindows(
  previous: PublicationBundle,
): Map<string, { start: number; end: number }> {
  const windows = new Map<string, { start: number; end: number }>();
  for (const q of previous.quarantines) {
    windows.set(q.connectionId, { start: q.startBeijingMs, end: q.endBeijingMs });
  }
  for (const a of previous.accounts) {
    const w = windows.get(a.connectionId);
    if (!w) {
      windows.set(a.connectionId, { start: a.windowStartMs, end: a.windowEndMs });
    } else {
      windows.set(a.connectionId, {
        start: Math.min(w.start, a.windowStartMs),
        end: Math.max(w.end, a.windowEndMs),
      });
    }
  }
  return windows;
}

interface IncrementalPlan {
  recomputed: Recomputed;
  carried: { accounts: DailyAccount[]; quarantines: QuarantineEntry[]; lineage: LineageRecord[] };
  carriedIds: string[];
  withdrawnDocIds: string[];
  selectionWarnings: string[];
}

/**
 * 增量计划：重算受影响接电，其余沿用上一版；被替换或消失的旧文档进入墓碑。
 */
function buildIncremental(
  previous: PublicationBundle,
  ledger: Ledger,
  affectedIds: string[],
  scope: SelectionScope | undefined,
): IncrementalPlan {
  const after = selectEffective(ledger.batches, ledger.records, scope);
  const afterById = new Map(after.connections.map((c) => [c.connectionId, c]));
  const affectedSet = new Set(affectedIds);

  const affectedConnections = affectedIds
    .map((id) => afterById.get(id))
    .filter((c): c is ConnectionRecord => c !== undefined);

  // 只加载本次范围涉及的电表读数与接电；受影响接电全部落在 scope 内
  const files = activeFiles(ledger.batches);
  const recomputed = recompute(
    affectedConnections,
    after.readings,
    files,
    '__pending__',
    new Set([...after.connectionConflictIds].filter((id) => affectedSet.has(id))),
  );

  const carriedAccounts = previous.accounts.filter((a) => !affectedSet.has(a.connectionId));
  const carriedQuarantines = previous.quarantines.filter((q) => !affectedSet.has(q.connectionId));
  const carriedLineage = previous.lineage.filter((l) => !affectedSet.has(l.connectionId));
  const carriedIds = [
    ...new Set([...carriedAccounts, ...carriedQuarantines].map((d) => d.connectionId)),
  ].sort();

  const retained = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
    ...carriedAccounts.map((d) => d.docId),
    ...carriedQuarantines.map((d) => d.docId),
  ]);

  // 被替换或消失的旧文档；调用方再与历史墓碑合并
  const withdrawals = [...previous.accounts, ...previous.quarantines]
    .map((d) => d.docId)
    .filter((id) => !retained.has(id));

  return {
    recomputed,
    carried: { accounts: carriedAccounts, quarantines: carriedQuarantines, lineage: carriedLineage },
    carriedIds,
    withdrawnDocIds: withdrawals,
    selectionWarnings: after.warnings,
  };
}

/**
 * 撤销来源批次。
 *
 * 不扫描输入目录：仅从已发布台账中读取被撤批次涉及的主键版本与相关接电的结算输入，
 * 在剩余有效批次上重新选取最高修订，只重算真正受影响的接电。
 * 批次状态与日账一起发布；中断后再次调用即可完成（幂等）。
 */
export async function revokeBatch(
  dataDir: string,
  request: RevokeRequest,
  options: Omit<RunOptions, 'cleanDelivery'> = {},
): Promise<RevokeOutcome> {
  const [previous, historyTombstones] = await Promise.all([
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);
  if (!previous) {
    throw new Error('尚无完整发布，无法撤销批次');
  }

  const target = previous.batches.find((b) => b.batchId === request.batchId);
  if (!target) {
    throw new Error(`批次 ${request.batchId} 不存在或未随当前发布落盘`);
  }

  const scopeKeys = revokeScope(previous.batchRecords, request.batchId);
  const scope: SelectionScope = {
    connectionIds: scopeKeys.connectionIds,
    meterIds: scopeKeys.meterIds,
  };

  const before = selectEffective(previous.batches, previous.batchRecords, scope);
  const simulated: SourceBatch[] = previous.batches.map((b) =>
    b.batchId === request.batchId
      ? {
          ...b,
          status: 'revoked' as const,
          updatedAtMs: nowMs(),
          revokedAtMs: nowMs(),
          note: request.note ?? b.note,
        }
      : b,
  );
  const after = selectEffective(simulated, previous.batchRecords, scope);

  const affected = affectedConnectionIds(before, after, scopeKeys.connectionIds);

  // 幂等重试：批次已撤销且本次没有任何接电受影响时，账与批次状态都不会改变，
  // 直接返回当前发布，不产生冗余发布（发布中断后重试也只会在真正生效时切换指针）。
  if (target.status === 'revoked' && affected.length === 0) {
    return {
      batchId: request.batchId,
      dryRun: false,
      affectedConnectionIds: [],
      recomputedConnectionIds: [],
      withdrawnDocIds: [],
      publicationId: previous.manifest.publicationId,
      skipped: true,
      warnings: [`批次 ${request.batchId} 此前已撤销，本次按幂等重试跳过发布`],
    };
  }

  if (request.dryRun) {
    return {
      batchId: request.batchId,
      dryRun: true,
      affectedConnectionIds: affected,
      recomputedConnectionIds: affected,
      withdrawnDocIds: [],
      publicationId: null,
      skipped: false,
      warnings: [...before.warnings],
    };
  }

  const ledger: Ledger = { batches: sortBatches(simulated), records: previous.batchRecords };
  const plan = buildIncremental(previous, ledger, affected, scope);

  const warnings = [...plan.selectionWarnings];
  let outcomeNote: string | undefined;
  if (target.status === 'revoked') {
    // 已撤销批次的再次撤销是幂等重试：不向清单添加任何变化，指纹一致时直接跳过发布
    outcomeNote = `批次 ${request.batchId} 此前已撤销，本次按幂等重试处理`;
  }

  const outcome = await commitResult({
    dataDir,
    mode: 'revoke',
    inputFiles: activeFiles(ledger.batches),
    ledger,
    recomputed: plan.recomputed,
    carried: plan.carried,
    carriedIds: plan.carriedIds,
    withdrawnDocIds: mergeWithdrawals(previous, retainedDocIds(plan), historyTombstones),
    warnings,
    previous,
    historyTombstones,
    commitHooks: options.commitHooks,
  });

  return {
    batchId: request.batchId,
    dryRun: false,
    affectedConnectionIds: affected,
    recomputedConnectionIds: outcome.recomputedConnectionIds,
    withdrawnDocIds: outcome.withdrawnDocIds,
    publicationId: outcome.publicationId,
    skipped: outcome.skipped,
    warnings: outcomeNote ? [...outcome.warnings, outcomeNote] : outcome.warnings,
  };
}

function retainedDocIds(plan: IncrementalPlan): Set<string> {
  return new Set<string>([
    ...plan.recomputed.accounts.map((d) => d.docId),
    ...plan.recomputed.quarantines.map((d) => d.docId),
    ...plan.carried.accounts.map((d) => d.docId),
    ...plan.carried.quarantines.map((d) => d.docId),
  ]);
}
