/**
 * 管线编排：全量运行与指定日期重跑。
 *
 * 重跑规则（运营人员指定北京日期 D）：
 * - 纳入：最新接电时段与 D 重叠的接电，或上一版发布中接电时段与 D 重叠的接电；
 * - 对纳入的接电重新结算，在所有相关日期替换其日账/隔离/血缘；
 * - 上一版中已失效（最新输入里被更高修订取代、或不再出现）的旧接电结果随替换撤下；
 * - 其余接电沿用各自已经发布的结果（文档按内容哈希复用，血缘保留原清单根，
 *   因此查询沿用账时返回的仍是它被结算时的那份输入文件清单）。
 *
 * 存储侧的关键变化（见 storage.ts / objectstore.ts）：
 * - 文档按内容寻址，跨发布不重复落盘；
 * - 输入文件清单与索引按内容寻址的前缀树增量写入，每次发布只新增变化路径；
 * - 发布清单只记录各树根 id，相同输入重跑时根 id 与指纹一致，自动跳过。
 */

import { ingest } from './ingest.js';
import { settleConnection } from './settlement.js';
import {
  commitPublication,
  listResultDocIds,
  loadStoredPublication,
  loadTombstones,
  manifestFingerprint,
  openCurrentPublication,
  persistManifestIdSets,
  persistPublicationPlan,
  planManifestIdSets,
  planPublicationDocs,
  TREE_FILELISTS,
  type PublicationDocs,
} from './storage.js';
import { persistTree, planTree } from './objectstore.js';
import { overlapsDay } from './time.js';
import type {
  ConnectionRecord,
  InputFileInfo,
  MeterReading,
  PublicationManifest,
  SourceRef,
  StoredDailyAccount,
  StoredLineageRecord,
  StoredQuarantineEntry,
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
  subjectDocId: string,
  subjectKind: StoredLineageRecord['subjectKind'],
  conn: ConnectionRecord,
  readingSources: SourceRef[],
  fileListRoot: string,
): StoredLineageRecord {
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
    // 清单不再逐条内嵌：只记内容寻址快照根，查询时一次展开
    fileListRoot,
  };
}

interface Recomputed extends PublicationDocs {
  connectionIds: string[];
}

/** 对一批接电执行结算并附血缘（文档不带 publicationId，落盘按内容去重） */
function recompute(
  connections: ConnectionRecord[],
  readings: MeterReading[],
  fileListRoot: string,
  revisionConflictIds: Set<string>,
  warnings: string[],
): Recomputed {
  const out: Recomputed = { accounts: [], quarantines: [], lineage: [], connectionIds: [] };

  for (const conn of connections) {
    out.connectionIds.push(conn.connectionId);
    const settled = settleConnection(conn, readings);

    if (revisionConflictIds.has(conn.connectionId)) {
      // 同主键同修订内容冲突：整次接电隔离（若结算本身也隔离，合并原因）
      const q: StoredQuarantineEntry = settled.quarantine
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
      out.quarantines.push(q);
      out.lineage.push(
        buildLineage(q.docId, 'quarantine', conn, settled.readingSources, fileListRoot),
      );
      continue;
    }

    if (settled.quarantine) {
      const q: StoredQuarantineEntry = { ...settled.quarantine };
      out.quarantines.push(q);
      out.lineage.push(
        buildLineage(q.docId, 'quarantine', conn, settled.readingSources, fileListRoot),
      );
      continue;
    }

    for (const acc of settled.accounts) {
      out.accounts.push(acc);
      out.lineage.push(
        buildLineage(acc.docId, 'daily-account', conn, settled.readingSources, fileListRoot),
      );
    }
  }

  // 未消费的告警仍写入清单，保证可追溯
  void warnings;
  out.connectionIds.sort();
  return out;
}

/** 从告警中提取“同修订冲突”的接电主键 */
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
  fileListTree: ReturnType<typeof planTree>,
  recomputed: Recomputed,
  carried: PublicationDocs,
  carriedIds: string[],
  withdrawnDocIds: string[],
  warnings: string[],
  previousId: string | null,
): Promise<RunOutcome> {
  const publicationId = newPublicationId(mode);

  // 沿用旧版（内联清单）血缘时，把内联清单转成清单树根，避免新发布继续复制整批清单。
  // 这里只做内存规划，等幂等判定通过后再统一落盘。
  const extraFileTrees: Array<ReturnType<typeof planTree>> = [];
  const normalizedCarriedLineage: StoredLineageRecord[] = [];
  for (const l of carried.lineage) {
    if (l.fileListRoot || !l.inputFiles) {
      normalizedCarriedLineage.push(l);
      continue;
    }
    const legacyTree = planTree(l.inputFiles.map((f) => ({ key: f.file, value: f })));
    extraFileTrees.push(legacyTree);
    const { inputFiles: _inline, ...rest } = l;
    normalizedCarriedLineage.push({ ...rest, fileListRoot: legacyTree.rootId });
  }

  const docs: PublicationDocs = {
    accounts: [...recomputed.accounts, ...carried.accounts] as StoredDailyAccount[],
    quarantines: [...recomputed.quarantines, ...carried.quarantines] as StoredQuarantineEntry[],
    lineage: [...recomputed.lineage, ...normalizedCarriedLineage],
  };

  // 纯内存规划：先算出清单/索引/集合树根与指纹，判定幂等后再决定是否落盘
  const plan = planPublicationDocs(docs);
  const idSets = planManifestIdSets(recomputed.connectionIds, carriedIds, withdrawnDocIds);
  const fileListRoot = fileListTree.rootId;

  const manifestBase: Omit<PublicationManifest, 'fingerprint' | 'publicationId' | 'createdAtMs'> = {
    docType: 'manifest',
    mode,
    rerunDay,
    storageVersion: 2,
    fileListRoot,
    inputFiles,
    accountsIndexRoot: plan.accountsIndexRoot,
    quarantinesIndexRoot: plan.quarantinesIndexRoot,
    lineageIndexRoot: plan.lineageIndexRoot,
    accountCount: docs.accounts.length,
    quarantineCount: docs.quarantines.length,
    // 清单里只存这三组 id 集合的树根；展开视图供返回与指纹之外的展示使用
    recomputedConnectionIdsRoot: idSets.recomputedConnectionIdsRoot,
    carriedConnectionIdsRoot: idSets.carriedConnectionIdsRoot,
    withdrawnDocIdsRoot: idSets.withdrawnDocIdsRoot,
    recomputedConnectionIds: recomputed.connectionIds,
    carriedConnectionIds: carriedIds,
    withdrawnDocIds,
    warnings,
  };

  const fingerprint = manifestFingerprint(manifestBase);

  // 幂等：与当前发布业务内容完全一致时，不写任何新对象、不产生新发布
  if (previousId) {
    const previous = await openCurrentPublication(dataDir);
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
  }

  // 对象先落盘（内容寻址，重复内容不新增字节），最后原子切换发布指针
  await persistPublicationPlan(dataDir, plan);
  await persistTree(dataDir, TREE_FILELISTS, fileListTree);
  for (const tree of extraFileTrees) {
    await persistTree(dataDir, TREE_FILELISTS, tree);
  }
  await persistManifestIdSets(dataDir, idSets);

  const manifest: PublicationManifest = {
    ...manifestBase,
    publicationId,
    createdAtMs: Date.now(),
    fingerprint,
  };
  await commitPublication(dataDir, manifest, withdrawnDocIds);
  return {
    publicationId,
    skipped: false,
    accountCount: docs.accounts.length,
    quarantineCount: docs.quarantines.length,
    recomputedConnectionIds: recomputed.connectionIds,
    carriedConnectionIds: carriedIds,
    withdrawnDocIds,
    warnings,
  };
}

/** 全量运行：重新结算输入中的全部接电 */
export async function runFull(inputDir: string, dataDir: string): Promise<RunOutcome> {
  const [{ connections, readings, files, warnings }, previous, historyTombstones] =
    await Promise.all([
      ingest(inputDir),
      openCurrentPublication(dataDir),
      loadTombstones(dataDir),
    ]);
  const previousId = previous?.publicationId ?? null;
  const { connectionIds } = extractRevisionConflictIds(warnings);

  // 清单快照树（纯内存规划）；相同输入得到相同根 id
  const fileListTree = planTree(files.map((f) => ({ key: f.file, value: f })));
  const recomputed = recompute(connections, readings, fileListTree.rootId, connectionIds, warnings);

  // 全量替换：上一版存在、但本次发布不再保留的文档即被撤下；与历史墓碑合并累积。
  // 只需上一版的文档 id（读索引键，不读文档内容），避免大发布下整包加载。
  const retainedDocIds = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
  ]);
  const previousDocIds = previous ? await listResultDocIds(dataDir, previous) : null;
  const newWithdrawals = previousDocIds
    ? [...previousDocIds.accountDocIds, ...previousDocIds.quarantineDocIds].filter(
        (id) => !retainedDocIds.has(id),
      )
    : [];
  const withdrawnDocIds = [...new Set([...historyTombstones, ...newWithdrawals])].sort();

  return publish(
    dataDir,
    'full',
    undefined,
    files,
    fileListTree,
    recomputed,
    { accounts: [], quarantines: [], lineage: [] },
    [],
    withdrawnDocIds,
    warnings,
    previousId,
  );
}

/** 指定北京时间日期重跑 */
export async function rerunDay(
  inputDir: string,
  dataDir: string,
  day: string,
): Promise<RunOutcome> {
  const [{ connections, readings, files, warnings }, previous, historyTombstones] =
    await Promise.all([
      ingest(inputDir),
      openCurrentPublication(dataDir),
      loadTombstones(dataDir),
    ]);

  if (!previous) {
    // 没有历史发布可沿用，退化为全量运行
    return runFull(inputDir, dataDir);
  }

  const previousDocs = await loadStoredPublication(dataDir, previous.publicationId);
  if (!previousDocs) return runFull(inputDir, dataDir);

  const { connectionIds } = extractRevisionConflictIds(warnings);
  const latestById = new Map(connections.map((c) => [c.connectionId, c]));

  // 纳入集合：最新接电时段与该日重叠，或上一版发布接电时段与该日重叠
  const includedIds = new Set<string>();
  for (const c of connections) {
    if (overlapsDay(c.startBeijingMs, c.endBeijingMs, day)) includedIds.add(c.connectionId);
  }
  const previousConnWindows = new Map<string, { start: number; end: number }>();
  for (const q of previousDocs.quarantines) {
    previousConnWindows.set(q.connectionId, { start: q.startBeijingMs, end: q.endBeijingMs });
  }
  for (const a of previousDocs.accounts) {
    const exist = previousConnWindows.get(a.connectionId);
    previousConnWindows.set(a.connectionId, {
      start: exist ? Math.min(exist.start, a.windowStartMs) : a.windowStartMs,
      end: exist ? Math.max(exist.end, a.windowEndMs) : a.windowEndMs,
    });
  }
  for (const [id, w] of previousConnWindows) {
    if (overlapsDay(w.start, w.end, day)) includedIds.add(id);
  }

  const includedConnections: ConnectionRecord[] = [];
  const missingIncluded: string[] = [];
  for (const id of includedIds) {
    const latest = latestById.get(id);
    if (latest) includedConnections.push(latest);
    else missingIncluded.push(id); // 最新输入已无此接电：其旧结果将被撤下
  }

  const fileListTree = planTree(files.map((f) => ({ key: f.file, value: f })));
  const recomputed = recompute(
    includedConnections,
    readings,
    fileListTree.rootId,
    connectionIds,
    warnings,
  );

  // 沿用：上一版中不属于纳入集合的文档原样复制。
  // 它们的血缘保留原 fileListRoot，因此仍返回原结算时的输入文件清单。
  const carriedAccounts = previousDocs.accounts.filter((a) => !includedIds.has(a.connectionId));
  const carriedQuarantines = previousDocs.quarantines.filter(
    (q) => !includedIds.has(q.connectionId),
  );
  const carriedLineage = previousDocs.lineage.filter((l) => !includedIds.has(l.connectionId));
  const carriedIds = [
    ...new Set([...carriedAccounts, ...carriedQuarantines].map((d) => d.connectionId)),
  ].sort();

  // 本次新增撤下：上一版存在、但新发布不再保留的文档（纳入接电的旧版本文档 + 已消失接电的文档）。
  // 必须与“新发布实际产出的文档 id”比较：同一接电可能由日账转为隔离（或反之）。
  const retainedDocIds = new Set<string>([
    ...recomputed.accounts.map((d) => d.docId),
    ...recomputed.quarantines.map((d) => d.docId),
    ...carriedAccounts.map((d) => d.docId),
    ...carriedQuarantines.map((d) => d.docId),
  ]);
  const newWithdrawals = [
    ...previousDocs.accounts.map((d) => d.docId),
    ...previousDocs.quarantines.map((d) => d.docId),
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
    fileListTree,
    recomputed,
    {
      accounts: carriedAccounts,
      quarantines: carriedQuarantines,
      lineage: carriedLineage,
    },
    carriedIds,
    withdrawnDocIds,
    warnings,
    previous.publicationId,
  );
}
