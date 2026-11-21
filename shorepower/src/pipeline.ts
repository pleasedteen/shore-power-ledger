/**
 * 管线编排：全量运行与指定日期重跑。
 *
 * 重跑规则（运营人员指定北京日期 D）：
 * - 纳入：最新接电时段与 D 重叠的接电，或上一版发布中接电时段与 D 重叠的接电；
 * - 对纳入的接电重新结算，在所有相关日期替换其日账/隔离/血缘；
 * - 上一版中已失效（最新输入里被更高修订取代、或不再出现）的旧接电结果随替换撤下；
 * - 其余接电沿用各自已经发布的结果（文档复制到新发布，保持快照完整）。
 */

import { ingest } from './ingest.js';
import { settleConnection } from './settlement.js';
import {
  commitPublication,
  contentFingerprint,
  loadCurrentPublication,
  loadTombstones,
  type PublicationBundle,
} from './storage.js';
import { enumerateBeijingDays, overlapsDay } from './time.js';
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
  recomputedConnectionIds: string[];
  carriedConnectionIds: string[];
  withdrawnDocIds: string[];
  warnings: string[];
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
): Recomputed {
  const out: Recomputed = { accounts: [], quarantines: [], lineage: [], connectionIds: [] };

  for (const conn of connections) {
    out.connectionIds.push(conn.connectionId);
    const settled = settleConnection(conn, readings);

    if (settled.quarantine) {
      // 同修订冲突（接电/读数）已由结算层并入同一条隔离文档：
      // 每个接电每版发布至多一条隔离，原因与各方出处合并保留
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

  const recomputed = recompute(connections, readings, files, '__pending__');

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

  const latestById = new Map(connections.map((c) => [c.connectionId, c]));

  // 上一版每个接电“实际涉及的旧日”集合：
  // 日账按各文档的自然日收集；隔离文档按其接电窗口覆盖的自然日收集。
  // 只按实际落点的日子判定，候选位置之间的空档不会被当作重叠。
  const previousDaysById = new Map<string, Set<string>>();
  const addDay = (id: string, day: string): void => {
    const set = previousDaysById.get(id);
    if (set) set.add(day);
    else previousDaysById.set(id, new Set([day]));
  };
  for (const a of previous.accounts) addDay(a.connectionId, a.beijingDay);
  for (const q of previous.quarantines) {
    for (const day of enumerateBeijingDays(q.startBeijingMs, q.endBeijingMs)) {
      addDay(q.connectionId, day);
    }
  }

  // 纳入集合（只按“候选实际涉及的旧日”判定，与文件顺序无关）：
  // 1. 最新接电窗口与重跑日重叠；或
  // 2. 上一版发布中该接电实际涉及重跑日（含旧原件已从输入移走的接电——
  //    按任一候选实际涉及的旧日重跑，也要撤换整笔旧结果）。
  const includedIds = new Set<string>();
  for (const c of connections) {
    if (overlapsDay(c.startBeijingMs, c.endBeijingMs, day)) includedIds.add(c.connectionId);
  }
  for (const [id, days] of previousDaysById) {
    if (days.has(day)) includedIds.add(id);
  }

  if (includedIds.size === 0) {
    // 重跑日没有命中任何最新窗口或上一版实际涉及的旧日（候选之间的空档不算重叠）：
    // 没有任何结果需要重算或撤换，直接跳过，指针与墓碑保持不动。
    return {
      publicationId: previous.manifest.publicationId,
      skipped: true,
      reason: `重跑日 ${day} 不涉及任何接电候选或已发布结果，无需重跑`,
      accountCount: previous.manifest.accountCount,
      quarantineCount: previous.manifest.quarantineCount,
      recomputedConnectionIds: [],
      carriedConnectionIds: previous.manifest.carriedConnectionIds,
      withdrawnDocIds: historyTombstones,
      warnings,
    };
  }

  const includedConnections: ConnectionRecord[] = [];
  const missingIncluded: string[] = [];
  for (const id of includedIds) {
    const latest = latestById.get(id);
    if (latest) includedConnections.push(latest);
    else missingIncluded.push(id); // 最新输入已无此接电：其旧结果将被整笔撤下
  }

  const recomputed = recompute(includedConnections, readings, files, '__pending__');

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
      `以下接电的旧结果实际涉及重跑日 ${day}，但最新输入已不存在该接电，旧结果已整笔撤下：${missingIncluded.join(', ')}`,
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
