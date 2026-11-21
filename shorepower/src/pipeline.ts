/**
 * 管线编排：全量运行、指定日期重跑与按北京时间日期范围的历史回补。
 *
 * 重跑规则（运营人员指定北京日期 D）：
 * - 纳入：最新接电时段与 D 重叠的接电，或上一版发布中接电时段与 D 重叠的接电；
 * - 对纳入的接电重新结算，在所有相关日期替换其日账/隔离/血缘；
 * - 上一版中已失效（最新输入里被更高修订取代、或不再出现）的旧接电结果随替换撤下；
 * - 其余接电沿用各自已经发布的结果（文档复制到新发布，保持快照完整）。
 *
 * 历史回补（运营人员指定北京日期范围 [D1, D2]）：
 * - 启动时取“最新输入窗口 ∪ 上一版发布窗口”与范围重叠的整笔跨日接电（整笔纳入，
 *   不按天切碎），全部重新结算后合并为一次发布；
 * - 逐笔接电持久化到回补暂存，中断后重跑同一范围即续跑：已完成接电不重算，
 *   即使输入投递目录已整体更换也能完成；
 * - 发布号由日期范围确定性派生：续跑与成功后的重试始终返回同一发布号，
 *   提交前任何查询都只能看到上一完整发布，提交后整批结果在同一发布号下可查。
 */

import { ingest } from './ingest.js';
import { settleConnection } from './settlement.js';
import {
  commitPublication,
  contentFingerprint,
  loadCurrentPublication,
  loadPublication,
  loadTombstones,
  type PublicationBundle,
} from './storage.js';
import {
  createBackfillJob,
  loadBackfillJob,
  loadStagedConnection,
  saveBackfillJob,
  stageConnection,
  type BackfillJobState,
  type StagedConnection,
} from './backfill-store.js';
import { overlapsDay, overlapsDayRange, assertValidDayRange } from './time.js';
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
  /** skipped=true 表示内容与当前发布一致（或回补已提交），未产生新发布 */
  skipped: boolean;
  reason?: string;
  accountCount: number;
  quarantineCount: number;
  recomputedConnectionIds: string[];
  carriedConnectionIds: string[];
  withdrawnDocIds: string[];
  warnings: string[];
  /** 回补续跑时为 true：存在历史暂存且部分接电被直接复用、未重算 */
  resumed?: boolean;
  backfillStartDay?: string;
  backfillEndDay?: string;
}

/** 回补中断注入钩子（测试用） */
export interface BackfillHooks {
  /** 每完成一笔接电暂存后触发（含实时计算与从暂存读回两种路径） */
  onConnectionStaged?: (info: {
    connectionId: string;
    completedCount: number;
    total: number;
  }) => void;
  /** 仅当一笔接电被实时结算（而非从暂存读回复用）时触发 */
  onConnectionComputed?: (info: { connectionId: string }) => void;
}

function newPublicationId(mode: 'full' | 'rerun'): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
  return `pub-${mode}-${ts}-${Math.random().toString(36).slice(2, 8)}`;
}

/** 为单个接电构建血缘文档（日账与隔离各一条） */
function buildLineage(
  publicationId: string,
  subjectDocId: string,
  subjectKind: LineageRecord['subjectKind'],
  conn: ConnectionRecord,
  readingSources: SourceRef[],
  inputFiles: InputFileInfo[],
): LineageRecord {
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

/** 对一批接电执行结算并打上新发布 id，附血缘 */
function recompute(
  connections: ConnectionRecord[],
  readings: import('./types.js').MeterReading[],
  inputFiles: InputFileInfo[],
  publicationId: string,
  revisionConflictIds: Set<string>,
  warnings: string[],
): Recomputed {
  const out: Recomputed = { accounts: [], quarantines: [], lineage: [], connectionIds: [] };

  for (const conn of connections) {
    out.connectionIds.push(conn.connectionId);
    const settled = settleConnection(conn, readings);

    if (revisionConflictIds.has(conn.connectionId)) {
      // 同主键同修订内容冲突：整次接电隔离（若结算本身也隔离，合并原因）
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

  // 未消费的告警仍写入清单，保证可追溯
  void warnings;
  out.connectionIds.sort();
  return out;
}

/** 单笔接电的结算产物（发布前形态，publicationId 由组装阶段统一盖章） */
export interface ConnectionOutput {
  connectionId: string;
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
  readingSources: SourceRef[];
  inputFiles: InputFileInfo[];
}

/** 结算单笔接电并构建其血缘（回补逐笔持久化使用，语义须与 recompute 完全一致） */
function computeConnection(
  conn: ConnectionRecord,
  readings: import('./types.js').MeterReading[],
  inputFiles: InputFileInfo[],
  revisionConflictIds: Set<string>,
): ConnectionOutput {
  const settled = settleConnection(conn, readings);
  const pendingId = '__pending__';
  const accounts: DailyAccount[] = [];
  const quarantines: QuarantineEntry[] = [];
  const lineage: LineageRecord[] = [];

  if (revisionConflictIds.has(conn.connectionId)) {
    // 同主键同修订内容冲突：整次接电隔离（若结算本身也隔离，合并原因）
    const q: QuarantineEntry = settled.quarantine
      ? { ...settled.quarantine, publicationId: pendingId }
      : {
          docType: 'quarantine',
          docId: `QUAR-${conn.connectionId}`,
          publicationId: pendingId,
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
    quarantines.push(q);
    lineage.push(
      buildLineage(pendingId, q.docId, 'quarantine', conn, settled.readingSources, inputFiles),
    );
  } else if (settled.quarantine) {
    const q: QuarantineEntry = { ...settled.quarantine, publicationId: pendingId };
    quarantines.push(q);
    lineage.push(
      buildLineage(pendingId, q.docId, 'quarantine', conn, settled.readingSources, inputFiles),
    );
  } else {
    for (const acc of settled.accounts) {
      const stamped: DailyAccount = { ...acc, publicationId: pendingId };
      accounts.push(stamped);
      lineage.push(
        buildLineage(pendingId, stamped.docId, 'daily-account', conn, settled.readingSources, inputFiles),
      );
    }
  }

  return {
    connectionId: conn.connectionId,
    accounts,
    quarantines,
    lineage,
    readingSources: settled.readingSources,
    inputFiles,
  };
}

/** 从告警中提取“同修订冲突”的接电/读数主键 */
function extractRevisionConflictIds(warnings: string[]): {
  connectionIds: Set<string>;
} {
  const connectionIds = new Set<string>();
  for (const w of warnings) {
    const m = w.match(/接电记录\s+(\S+)\s+rev=/);
    if (m) connectionIds.add(m[1]!);
  }
  return { connectionIds };
}

async function publish(
  dataDir: string,
  mode: 'full' | 'rerun',
  rerunDay: string | undefined,
  inputFiles: InputFileInfo[],
  recomputed: Recomputed,
  carried: { accounts: DailyAccount[]; quarantines: QuarantineEntry[]; lineage: LineageRecord[] },
  carriedIds: string[],
  withdrawnDocIds: string[],
  warnings: string[],
  previous: PublicationBundle | null,
): Promise<RunOutcome> {
  const publicationId = newPublicationId(mode);

  // 递归重写所有层级的 publicationId：沿用文档的嵌套结构（如血缘）里也带发布 id，
  // 只替换顶层会让旧 id 残留，破坏幂等指纹。
  const stamp = <T>(value: T): T => {
    if (Array.isArray(value)) {
      return value.map((v) => stamp(v)) as T;
    }
    if (value !== null && typeof value === 'object') {
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
        out[k] = k === 'publicationId' ? publicationId : stamp(v);
      }
      return out as T;
    }
    return value;
  };

  const accounts = stamp([...recomputed.accounts, ...carried.accounts]);
  const quarantines = stamp([...recomputed.quarantines, ...carried.quarantines]);
  const lineage = stamp([...recomputed.lineage, ...carried.lineage]);

  const manifestWithoutFingerprint: Omit<PublicationManifest, 'fingerprint'> = {
    docType: 'manifest',
    publicationId,
    createdAtMs: Date.now(),
    mode,
    rerunDay,
    inputFiles,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    recomputedConnectionIds: recomputed.connectionIds,
    carriedConnectionIds: carriedIds,
    withdrawnDocIds,
    warnings,
  };

  const fingerprint = contentFingerprint({
    manifest: manifestWithoutFingerprint,
    accounts,
    quarantines,
    lineage,
  });
  const manifest: PublicationManifest = { ...manifestWithoutFingerprint, fingerprint };

  // 幂等：与当前发布业务内容完全一致时不产生新发布
  if (previous && previous.manifest.fingerprint === fingerprint) {
    return {
      publicationId: previous.manifest.publicationId,
      skipped: true,
      reason: '相同输入与结算结果，内容指纹一致，跳过重复发布',
      accountCount: previous.manifest.accountCount,
      quarantineCount: previous.manifest.quarantineCount,
      recomputedConnectionIds: recomputed.connectionIds,
      carriedConnectionIds: carriedIds,
      withdrawnDocIds,
      warnings,
    };
  }

  await commitPublication(dataDir, { manifest, accounts, quarantines, lineage }, withdrawnDocIds);
  return {
    publicationId,
    skipped: false,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    recomputedConnectionIds: recomputed.connectionIds,
    carriedConnectionIds: carriedIds,
    withdrawnDocIds,
    warnings,
  };
}

/** 全量运行：重新结算输入中的全部接电 */
export async function runFull(inputDir: string, dataDir: string): Promise<RunOutcome> {
  const [{ connections, readings, files, warnings }, previous, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);
  const { connectionIds } = extractRevisionConflictIds(warnings);

  const recomputed = recompute(connections, readings, files, '__pending__', connectionIds, warnings);

  // 全量替换：上一版存在、但本次发布不再保留的文档即被撤下；与历史墓碑合并累积
  const retainedDocIds = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
  ]);
  const newWithdrawals = previous
    ? [
        ...previous.accounts.map((d) => d.docId),
        ...previous.quarantines.map((d) => d.docId),
      ].filter((id) => !retainedDocIds.has(id))
    : [];
  const withdrawnDocIds = [...new Set([...historyTombstones, ...newWithdrawals])].sort();

  return publish(
    dataDir,
    'full',
    undefined,
    files,
    recomputed,
    { accounts: [], quarantines: [], lineage: [] },
    [],
    withdrawnDocIds,
    warnings,
    previous,
  );
}

/** 从上一版发布还原每个接电的窗口（隔离取窗口；日账取窗口并集近似） */
function previousConnectionWindows(
  previous: PublicationBundle,
): Map<string, { start: number; end: number }> {
  const windows = new Map<string, { start: number; end: number }>();
  for (const q of previous.quarantines) {
    windows.set(q.connectionId, { start: q.startBeijingMs, end: q.endBeijingMs });
  }
  for (const a of previous.accounts) {
    if (!windows.has(a.connectionId)) {
      // 从日账窗口拼不出完整区间时，用窗口并集近似（仅用于重叠判断）
      windows.set(a.connectionId, {
        start: Math.min(a.windowStartMs),
        end: Math.max(a.windowEndMs),
      });
    } else {
      const w = windows.get(a.connectionId)!;
      windows.set(a.connectionId, {
        start: Math.min(w.start, a.windowStartMs),
        end: Math.max(w.end, a.windowEndMs),
      });
    }
  }
  return windows;
}

/** 选择与日期范围重叠的整笔接电：最新输入窗口 ∪ 上一版发布窗口 */
function selectIncludedIds(
  connections: ConnectionRecord[],
  previous: PublicationBundle | null,
  startDay: string,
  endDay: string,
): Set<string> {
  const includedIds = new Set<string>();
  const overlaps = (start: number, end: number) =>
    startDay === endDay
      ? overlapsDay(start, end, startDay)
      : overlapsDayRange(start, end, startDay, endDay);

  for (const c of connections) {
    if (overlaps(c.startBeijingMs, c.endBeijingMs)) includedIds.add(c.connectionId);
  }
  if (previous) {
    for (const [id, w] of previousConnectionWindows(previous)) {
      if (overlaps(w.start, w.end)) includedIds.add(id);
    }
  }
  return includedIds;
}

/** 指定北京时间日期重跑 */
export async function rerunDay(
  inputDir: string,
  dataDir: string,
  day: string,
): Promise<RunOutcome> {
  const [{ connections, readings, files, warnings }, previous, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);

  if (!previous) {
    // 没有历史发布可沿用，退化为全量运行
    return runFull(inputDir, dataDir);
  }

  const { connectionIds } = extractRevisionConflictIds(warnings);
  const latestById = new Map(connections.map((c) => [c.connectionId, c]));

  // 纳入集合：最新接电时段与该日重叠，或上一版发布接电时段与该日重叠（整笔纳入）
  const includedIds = selectIncludedIds(connections, previous, day, day);

  const includedConnections: ConnectionRecord[] = [];
  const missingIncluded: string[] = [];
  for (const id of includedIds) {
    const latest = latestById.get(id);
    if (latest) includedConnections.push(latest);
    else missingIncluded.push(id); // 最新输入已无此接电：其旧结果将被撤下
  }

  const recomputed = recompute(
    includedConnections,
    readings,
    files,
    '__pending__',
    connectionIds,
    warnings,
  );

  // 沿用：上一版中不属于纳入集合的文档原样复制
  const carriedAccounts = previous.accounts.filter((a) => !includedIds.has(a.connectionId));
  const carriedQuarantines = previous.quarantines.filter((q) => !includedIds.has(q.connectionId));
  const carriedLineage = previous.lineage.filter((l) => !includedIds.has(l.connectionId));
  const carriedIds = [...new Set([...carriedAccounts, ...carriedQuarantines].map((d) => d.connectionId))].sort();

  // 本次新增撤下：上一版存在、但新发布不再保留的文档（纳入接电的旧版本文档 + 已消失接电的文档）。
  // 注意必须与“新发布实际产出的文档 id”比较，而不是只按接电 id 判断——
  // 同一接电可能在上一版是日账、本次重算后变为隔离（或反之），其旧文档同样需要撤下。
  const retainedDocIds = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
    ...carriedAccounts.map((d) => d.docId),
    ...carriedQuarantines.map((d) => d.docId),
  ]);
  const newWithdrawals = [
    ...previous.accounts.map((d) => d.docId),
    ...previous.quarantines.map((d) => d.docId),
  ].filter((id) => !retainedDocIds.has(id));
  // 与历史墓碑合并累积：撤下记录只增不减，保证幂等重跑的清单稳定
  const withdrawnDocIds = [...new Set([...historyTombstones, ...newWithdrawals])].sort();

  if (missingIncluded.length > 0) {
    warnings.push(
      `以下接电在上一版发布中与 ${day} 重叠，但最新输入已不存在，旧结果已撤下：${missingIncluded.join(', ')}`,
    );
  }

  return publish(
    dataDir,
    'rerun',
    day,
    files,
    recomputed,
    { accounts: carriedAccounts, quarantines: carriedQuarantines, lineage: carriedLineage },
    carriedIds,
    withdrawnDocIds,
    warnings,
    previous,
  );
}

/**
 * 按北京时间日期范围 [startDay, endDay]（含首尾）做历史回补，整批一次发布。
 *
 * 断点续跑语义：
 * - 每笔纳入接电结算后立即原子暂存并更新作业状态；
 * - 中断后以同一日期范围再次调用即续跑（输入投递目录更换也不影响，因为已完成
 *   接电从暂存读回、不依赖原目录），已完成接电绝不重算；
 * - 发布号由日期范围确定性派生，成功后的重试返回原发布号、不产生重复历史版本。
 */
export async function rerunRange(
  inputDir: string,
  dataDir: string,
  startDay: string,
  endDay: string,
  hooks: BackfillHooks = {},
): Promise<RunOutcome> {
  assertValidDayRange(startDay, endDay);

  const [ingested, current, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);
  const { connections, readings, files, warnings } = ingested;
  const { connectionIds: revisionConflictIds } = extractRevisionConflictIds(warnings);
  const latestById = new Map(connections.map((c) => [c.connectionId, c]));

  const existingJob = await loadBackfillJob(dataDir, startDay, endDay);

  // 成功发布后的重试：作业已提交且发布目录可读，直接返回原发布号，不新增版本
  if (existingJob?.status === 'committed') {
    const committed = await loadPublication(dataDir, existingJob.publicationId);
    if (committed) {
      return {
        publicationId: committed.manifest.publicationId,
        skipped: true,
        reason: '该日期范围的回补已发布，复用原发布号，不产生重复版本',
        accountCount: committed.manifest.accountCount,
        quarantineCount: committed.manifest.quarantineCount,
        recomputedConnectionIds: committed.manifest.recomputedConnectionIds,
        carriedConnectionIds: committed.manifest.carriedConnectionIds,
        withdrawnDocIds: committed.manifest.withdrawnDocIds,
        warnings: committed.manifest.warnings,
        resumed: true,
        backfillStartDay: startDay,
        backfillEndDay: endDay,
      };
    }
    // 目录缺失（人为清理）：落到下方续跑重建，作业状态保持 running 语义
  }

  // 回补启动时的基准发布（整批提交前查询始终可见的“上一完整发布”）
  const base = current;
  const basePublicationId = base?.manifest.publicationId ?? '';

  let job: BackfillJobState;
  let resumed = false;
  if (existingJob) {
    // 续跑：基准发布必须仍是启动时的上一完整发布，否则说明期间有别的发布改变了基线
    if (existingJob.basePublicationId !== basePublicationId) {
      throw new Error(
        `回补续跑失败：基准发布已从 ${existingJob.basePublicationId || '(空)'} ` +
          `变为 ${basePublicationId || '(空)'}，请明确新的日期范围后重新回补`,
      );
    }
    job = existingJob;
    resumed = job.completedConnectionIds.length > 0;
  } else {
    if (!base) {
      // 没有任何历史发布：回补退化为对当前输入的全量回补（无沿用文档）
    }
    const includedIds = selectIncludedIds(connections, base, startDay, endDay);
    job = await createBackfillJob(
      dataDir,
      startDay,
      endDay,
      basePublicationId,
      [...includedIds].sort(),
    );
  }

  // 纳入集合以作业首次确定的为准（整笔语义稳定）；新增重叠接电并入，消失接电后续撤下
  const includedIds = new Set(job.includedConnectionIds);
  for (const id of selectIncludedIds(connections, base, startDay, endDay)) includedIds.add(id);
  if (includedIds.size !== job.includedConnectionIds.length) {
    job.includedConnectionIds = [...includedIds].sort();
    await saveBackfillJob(job, dataDir);
  }

  const completed = new Set(job.completedConnectionIds);

  // 1) 逐笔结算并原子暂存（已完成的直接跳过，不重算）
  const missingIncluded: string[] = [];
  for (const id of [...includedIds].sort()) {
    if (completed.has(id)) continue;
    const latest = latestById.get(id);
    if (!latest) {
      // 最新输入已无此接电：无可重算内容，其旧结果在组装时撤下
      missingIncluded.push(id);
      continue;
    }
    const output = computeConnection(latest, readings, files, revisionConflictIds);
    hooks.onConnectionComputed?.({ connectionId: id });
    const omitPub = <T extends { publicationId: string }>(doc: T): Omit<T, 'publicationId'> => {
      const { publicationId: _omit, ...rest } = doc;
      return rest;
    };
    const staged: StagedConnection = {
      connectionId: id,
      revision: latest.revision,
      accounts: output.accounts.map(omitPub),
      quarantines: output.quarantines.map(omitPub),
      lineage: output.lineage.map(omitPub),
      readingSources: output.readingSources,
      inputFiles: files.map((f) => ({ ...f })),
      computedAtMs: Date.now(),
    };
    await stageConnection(dataDir, job.jobKey, staged);
    completed.add(id);
    job.completedConnectionIds = [...completed].sort();
    await saveBackfillJob(job, dataDir);
    hooks.onConnectionStaged?.({
      connectionId: id,
      completedCount: completed.size,
      total: includedIds.size,
    });
  }

  // 2) 组装整批发布：暂存结果（重新盖章为同一发布号）+ 基准发布的沿用文档
  const publicationId = job.publicationId;
  const stamp = <T>(value: T): T => {
    if (Array.isArray(value)) {
      return value.map((v) => stamp(v)) as T;
    }
    if (value !== null && typeof value === 'object') {
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
        out[k] = k === 'publicationId' ? publicationId : stamp(v);
      }
      return out as T;
    }
    return value;
  };

  const recomputedAccounts: DailyAccount[] = [];
  const recomputedQuarantines: QuarantineEntry[] = [];
  const recomputedLineage: LineageRecord[] = [];
  const recomputedIds: string[] = [];

  for (const id of [...includedIds].sort()) {
    const staged = await loadStagedConnection(dataDir, job.jobKey, id);
    if (!staged) continue; // 已消失且无暂存的接电：仅参与撤下
    recomputedIds.push(id);
    const output = computeConnectionFromStaging(staged);
    recomputedAccounts.push(...stamp(output.accounts));
    recomputedQuarantines.push(...stamp(output.quarantines));
    recomputedLineage.push(...stamp(output.lineage));
  }

  const carriedAccounts = base ? base.accounts.filter((a) => !includedIds.has(a.connectionId)) : [];
  const carriedQuarantines = base
    ? base.quarantines.filter((q) => !includedIds.has(q.connectionId))
    : [];
  const carriedLineage = base ? base.lineage.filter((l) => !includedIds.has(l.connectionId)) : [];
  const carriedIds = [...new Set([...carriedAccounts, ...carriedQuarantines].map((d) => d.connectionId))].sort();

  const accounts = stamp([...recomputedAccounts, ...carriedAccounts]);
  const quarantines = stamp([...recomputedQuarantines, ...carriedQuarantines]);
  const lineage = stamp([...recomputedLineage, ...carriedLineage]);

  // 撤下：基准发布存在、但整批结果不再保留的文档（接电消失或日账/隔离形态变化）
  const retainedDocIds = new Set<string>([
    ...accounts.map((d) => d.docId),
    ...quarantines.map((d) => d.docId),
  ]);
  const newWithdrawals = base
    ? [
        ...base.accounts.map((d) => d.docId),
        ...base.quarantines.map((d) => d.docId),
      ].filter((id) => !retainedDocIds.has(id))
    : [];
  const withdrawnDocIds = [...new Set([...historyTombstones, ...newWithdrawals])].sort();

  const allWarnings = [...warnings];
  if (missingIncluded.length > 0) {
    allWarnings.push(
      `以下接电在回补范围中，但最新输入已不存在，旧结果已撤下：${missingIncluded.join(', ')}`,
    );
  }

  const manifestWithoutFingerprint: Omit<PublicationManifest, 'fingerprint'> = {
    docType: 'manifest',
    publicationId,
    createdAtMs: Date.now(),
    mode: 'backfill',
    rerunDay: undefined,
    backfillStartDay: startDay,
    backfillEndDay: endDay,
    inputFiles: files,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    recomputedConnectionIds: [...recomputedIds].sort(),
    carriedConnectionIds: carriedIds,
    withdrawnDocIds,
    warnings: allWarnings,
  };

  const fingerprint = contentFingerprint({
    manifest: manifestWithoutFingerprint,
    accounts,
    quarantines,
    lineage,
  });

  // 幂等：与当前发布业务内容完全一致时不产生新发布（仍返回同一确定性发布号）
  if (base && base.manifest.fingerprint === fingerprint) {
    return {
      publicationId: base.manifest.publicationId,
      skipped: true,
      reason: '相同输入与结算结果，内容指纹一致，跳过重复发布',
      accountCount: base.manifest.accountCount,
      quarantineCount: base.manifest.quarantineCount,
      recomputedConnectionIds: [...recomputedIds].sort(),
      carriedConnectionIds: carriedIds,
      withdrawnDocIds,
      warnings: allWarnings,
      resumed,
      backfillStartDay: startDay,
      backfillEndDay: endDay,
    };
  }

  const manifest: PublicationManifest = { ...manifestWithoutFingerprint, fingerprint };
  await commitPublication(dataDir, { manifest, accounts, quarantines, lineage }, withdrawnDocIds);

  // 发布目录与指针均已落盘后才把作业标记为 committed（先标记后切指针的中断也能被
  // 重试路径识别：committed + 目录可读 → 直接复用原发布号）
  job.status = 'committed';
  await saveBackfillJob(job, dataDir);

  return {
    publicationId,
    skipped: false,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    recomputedConnectionIds: [...recomputedIds].sort(),
    carriedConnectionIds: carriedIds,
    withdrawnDocIds,
    warnings: allWarnings,
    resumed,
    backfillStartDay: startDay,
    backfillEndDay: endDay,
  };
}

/** 把暂存的单笔接电结果还原为发布前文档（补回占位发布号，结构与实时结算一致） */
function computeConnectionFromStaging(staged: StagedConnection): ConnectionOutput {
  const pendingId = '__pending__';
  return {
    connectionId: staged.connectionId,
    accounts: staged.accounts.map((a) => ({ ...a, publicationId: pendingId })),
    quarantines: staged.quarantines.map((q) => ({ ...q, publicationId: pendingId })),
    lineage: staged.lineage.map((l) => ({ ...l, publicationId: pendingId })),
    readingSources: staged.readingSources,
    inputFiles: staged.inputFiles,
  };
}
