/**
 * 管线编排：全量运行与指定日期重跑。
 *
 * 重跑规则（运营人员指定北京日期 D）：
 * - 纳入：最新接电时段与 D 重叠的接电，或上一版发布中接电时段与 D 重叠的接电；
 * - 对纳入的接电重新结算，在所有相关日期替换其日账/隔离/血缘；
 * - 上一版中已失效（最新输入里被更高修订取代、或不再出现）的旧接电结果随替换撤下；
 * - 其余接电沿用各自已经发布的结果（文档复制到新发布，保持快照完整）。
 *
 * 并发与中断：
 * - 每次发布在跨进程文件锁上串行化；重跑“算完之后、提交之前”的产物先持久化到
 *   .staging 暂存区，可在提交前暂停或被进程退出打断，恢复时凭 token 继续；
 * - 暂存期间另一笔（互不相干的）重跑已抢先发布时，恢复提交按接电分区合并：
 *   本笔重算的接电集合覆盖对应分区，其余分区沿用对方已发布结果，两笔更正都生效；
 * - 撤下墓碑取并集只增不减，输入文件清单与告警去重合并，血缘随各自分区保留。
 */

import { readFile, writeFile } from 'node:fs/promises';
import { ingest } from './ingest.js';
import { settleConnection } from './settlement.js';
import {
  acquirePublicationLock,
  commitPrepared,
  contentFingerprint,
  listStaging,
  loadCurrentPublication,
  loadTombstones,
  prepareStaging,
  readStaging,
  removeStagingState,
  withPublicationLock,
  type LockHandle,
  type PublicationBundle,
  type StagingPlan,
  type StagingState,
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
  recomputedConnectionIds: string[];
  carriedConnectionIds: string[];
  withdrawnDocIds: string[];
  warnings: string[];
  /** 暂停/中断恢复用的暂存 token；仅暂停场景返回 */
  stagingToken?: string;
}

/** 暂停控制：算完进入提交前，管线等待恢复信号 */
export interface PauseOptions {
  /** 暂停状态文件：管线写 paused，外部改写为 resume/cancel 即恢复或取消 */
  statusFile: string;
  /** 测试钩子：不轮询文件，直接等待该可等待对象 */
  waitForResume?: () => Promise<'resume' | 'cancel'>;
}

export interface RunOptions {
  pause?: PauseOptions;
  /** 复用指定暂存 token（默认新建） */
  token?: string;
}

interface PauseStatus {
  state: 'paused' | 'resume' | 'cancel';
  token: string;
  publicationId: string;
  basePublicationId: string | null;
  createdAtMs: number;
}

function newPublicationId(mode: 'full' | 'rerun'): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
  return `pub-${mode}-${ts}-${Math.random().toString(36).slice(2, 8)}`;
}

function newStageToken(mode: 'full' | 'rerun'): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
  return `stage-${mode}-${ts}-${Math.random().toString(36).slice(2, 8)}`;
}

/** 递归把任意层级的 publicationId 改写为新发布 id（沿用文档的嵌套血缘里也带发布 id） */
function stampPublicationId<T>(value: T, publicationId: string): T {
  if (Array.isArray(value)) {
    return value.map((v) => stampPublicationId(v, publicationId)) as T;
  }
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = k === 'publicationId' ? publicationId : stampPublicationId(v, publicationId);
    }
    return out as T;
  }
  return value;
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

/** 组装发布清单；指纹由调用方在血缘齐备后用 fingerprintOf 统一计算 */
function buildManifest(
  publicationId: string,
  mode: 'full' | 'rerun',
  rerunDay: string | undefined,
  inputFiles: InputFileInfo[],
  accounts: DailyAccount[],
  quarantines: QuarantineEntry[],
  recomputedConnectionIds: string[],
  carriedConnectionIds: string[],
  withdrawnDocIds: string[],
  warnings: string[],
): { manifest: PublicationManifest } {
  const manifestWithoutFingerprint: Omit<PublicationManifest, 'fingerprint'> = {
    docType: 'manifest',
    publicationId,
    createdAtMs: Date.now(),
    mode,
    rerunDay,
    inputFiles,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    recomputedConnectionIds: [...recomputedConnectionIds].sort(),
    carriedConnectionIds: [...carriedConnectionIds].sort(),
    withdrawnDocIds: [...withdrawnDocIds].sort(),
    warnings,
  };
  // 占位指纹；最终指纹必须覆盖血缘，由 fingerprintOf 统一重算
  return { manifest: { ...manifestWithoutFingerprint, fingerprint: '' } };
}

function fingerprintOf(
  manifest: PublicationManifest,
  accounts: DailyAccount[],
  quarantines: QuarantineEntry[],
  lineage: LineageRecord[],
): string {
  return contentFingerprint({ manifest, accounts, quarantines, lineage });
}

interface BundleParts {
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
}

function docConnectionId(doc: { connectionId: string }): string {
  return doc.connectionId;
}

/**
 * 分区合并：暂存发布（本笔更正）与并发抢先落地的当前发布（另一笔更正）融合。
 *
 * 以暂存发布重算的接电集合为分区边界：
 * - 分区内的日账/隔离/血缘全部以本笔暂存结果为准；
 * - 分区外的全部结果沿用当前发布（对方更正与既有结果都保留）；
 * - 墓碑取并集只增不减；输入文件按相对路径去重；告警按文本去重。
 */
function mergeStagedWithCurrent(
  current: PublicationBundle,
  staged: StagingState,
  newPublicationId: string,
): { bundle: PublicationBundle; withdrawnDocIds: string[]; warnings: string[] } {
  const partitions = new Set(staged.plan.recomputedConnectionIds);

  const fromCurrent: BundleParts = {
    accounts: current.accounts.filter((d) => !partitions.has(docConnectionId(d))),
    quarantines: current.quarantines.filter((d) => !partitions.has(docConnectionId(d))),
    lineage: current.lineage.filter((d) => !partitions.has(docConnectionId(d))),
  };
  const fromStaged: BundleParts = {
    accounts: staged.bundle.accounts.filter((d) => partitions.has(docConnectionId(d))),
    quarantines: staged.bundle.quarantines.filter((d) => partitions.has(docConnectionId(d))),
    lineage: staged.bundle.lineage.filter((d) => partitions.has(docConnectionId(d))),
  };

  const accounts = stampPublicationId(
    [...fromStaged.accounts, ...fromCurrent.accounts],
    newPublicationId,
  );
  const quarantines = stampPublicationId(
    [...fromStaged.quarantines, ...fromCurrent.quarantines],
    newPublicationId,
  );
  const lineage = stampPublicationId(
    [...fromStaged.lineage, ...fromCurrent.lineage],
    newPublicationId,
  );

  // 输入文件清单按相对路径去重合并（重复文件标记保留）
  const fileByName = new Map<string, InputFileInfo>();
  for (const f of [...staged.plan.inputFiles, ...current.manifest.inputFiles]) {
    const exist = fileByName.get(f.file);
    if (!exist) fileByName.set(f.file, { ...f });
    else if (exist.duplicateOf === undefined && f.duplicateOf !== undefined) {
      exist.duplicateOf = f.duplicateOf;
    }
  }
  const inputFiles = [...fileByName.values()].sort((a, b) => a.file.localeCompare(b.file));

  // 告警按文本去重，并追加一条并发合并审计记录
  const warnings = [...new Set([...staged.plan.warnings, ...current.manifest.warnings])].sort();
  warnings.push(
    `检测到并发发布 ${current.manifest.publicationId} 已在暂存期间落地，已按接电分区合并两笔更正`,
  );

  // 墓碑并集；再以最终保留文档集合做一次防御性核对（正常情况下不产生新增项）
  const withdrawnDocIds = new Set<string>([
    ...staged.withdrawnDocIds,
    ...current.manifest.withdrawnDocIds,
  ]);
  const retainedDocIds = new Set<string>([
    ...accounts.map((d) => d.docId),
    ...quarantines.map((d) => d.docId),
  ]);
  for (const d of [...current.accounts, ...current.quarantines, ...staged.bundle.accounts, ...staged.bundle.quarantines]) {
    if (!retainedDocIds.has(d.docId)) withdrawnDocIds.add(d.docId);
  }

  const recomputedConnectionIds = [
    ...new Set([
      ...staged.plan.recomputedConnectionIds,
      ...current.manifest.recomputedConnectionIds,
    ]),
  ].sort();
  const finalConnectionIds = new Set<string>([
    ...accounts.map((d) => d.connectionId),
    ...quarantines.map((d) => d.connectionId),
  ]);
  const carriedConnectionIds = [...finalConnectionIds]
    .filter((id) => !recomputedConnectionIds.includes(id))
    .sort();

  const mode: PublicationManifest['mode'] =
    staged.plan.mode === 'full' || current.manifest.mode === 'full' ? 'full' : 'rerun';
  const rerunDay = staged.plan.rerunDay ?? current.manifest.rerunDay;

  const { manifest } = buildManifest(
    newPublicationId,
    mode,
    rerunDay,
    inputFiles,
    accounts,
    quarantines,
    recomputedConnectionIds,
    carriedConnectionIds,
    [...withdrawnDocIds].sort(),
    warnings,
  );
  manifest.fingerprint = fingerprintOf(manifest, accounts, quarantines, lineage);

  return {
    bundle: { manifest, accounts, quarantines, lineage },
    withdrawnDocIds: [...withdrawnDocIds].sort(),
    warnings,
  };
}

/** 依据当前指针状态，把暂存发布直接提交或先合并并发发布再提交（调用方持锁） */
async function commitUnderLock(
  dataDir: string,
  initial: StagingState,
  lock: LockHandle,
): Promise<RunOutcome> {
  void lock;
  const pointerBundle = await loadCurrentPublication(dataDir);
  const currentId = pointerBundle?.manifest.publicationId ?? null;

  // 已被提交过（理论上提交成功即删暂存，这里做幂等兜底）
  if (currentId === initial.publicationId) {
    await removeStagingState(dataDir, initial.token);
    return outcomeFromStaging(initial, true, '暂存发布此前已提交，跳过重复提交');
  }

  let state = initial;

  // 暂存期间有并发发布落地：按接电分区合并后替换暂存（token 不变）
  if (currentId !== null && currentId !== state.basePublicationId && pointerBundle) {
    const mergedId = newPublicationId('rerun');
    const merged = mergeStagedWithCurrent(pointerBundle, state, mergedId);
    if (merged.bundle.manifest.fingerprint === pointerBundle.manifest.fingerprint) {
      // 合并后与当前发布业务内容完全一致：无需新发布，清理暂存
      await removeStagingState(dataDir, state.token);
      return {
        publicationId: pointerBundle.manifest.publicationId,
        skipped: true,
        reason: '并发发布已包含等价更正，内容指纹一致，跳过重复发布',
        accountCount: pointerBundle.manifest.accountCount,
        quarantineCount: pointerBundle.manifest.quarantineCount,
        recomputedConnectionIds: merged.bundle.manifest.recomputedConnectionIds,
        carriedConnectionIds: merged.bundle.manifest.carriedConnectionIds,
        withdrawnDocIds: merged.withdrawnDocIds,
        warnings: merged.warnings,
        stagingToken: state.token,
      };
    }
    const mergedState: StagingState = {
      ...state,
      publicationId: mergedId,
      basePublicationId: currentId,
      fingerprint: merged.bundle.manifest.fingerprint,
      withdrawnDocIds: merged.withdrawnDocIds,
      plan: { ...state.plan, warnings: merged.warnings, inputFiles: merged.bundle.manifest.inputFiles },
      bundle: merged.bundle,
    };
    await prepareStaging(dataDir, mergedState);
    state = mergedState;
  }

  await commitPrepared(dataDir, state);
  return outcomeFromStaging(state, false);
}

function outcomeFromStaging(state: StagingState, skipped: boolean, reason?: string): RunOutcome {
  return {
    publicationId: state.publicationId,
    skipped,
    reason,
    accountCount: state.bundle.manifest.accountCount,
    quarantineCount: state.bundle.manifest.quarantineCount,
    recomputedConnectionIds: [...state.plan.recomputedConnectionIds].sort(),
    carriedConnectionIds: [...state.plan.carriedConnectionIds].sort(),
    withdrawnDocIds: state.withdrawnDocIds,
    warnings: state.plan.warnings,
    stagingToken: state.token,
  };
}

/** 暂停：落状态文件并等待外部把状态改写为 resume/cancel，返回恢复信号 */
async function pauseAndWait(
  opts: PauseOptions,
  status: Omit<PauseStatus, 'state'>,
): Promise<'resume' | 'cancel'> {
  const payload: PauseStatus = { state: 'paused', ...status };
  await writeFile(opts.statusFile, JSON.stringify(payload, null, 2) + '\n', 'utf-8');

  return opts.waitForResume ? opts.waitForResume() : pollStatusFile(opts.statusFile);
}

async function pollStatusFile(statusFile: string): Promise<'resume' | 'cancel'> {
  for (;;) {
    try {
      const raw = JSON.parse(await readFile(statusFile, 'utf-8')) as PauseStatus;
      if (raw.state === 'resume' || raw.state === 'cancel') return raw.state;
    } catch {
      // 文件暂时不可读时继续等待
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/**
 * 通用收尾：指纹幂等判断 → 持锁持久化暂存 → （可选）暂停 → 恢复后持锁提交/合并。
 * 锁不跨暂停持有，暂停期间其他重跑可以正常发布。
 */
async function finalize(
  dataDir: string,
  params: {
    mode: 'full' | 'rerun';
    rerunDay: string | undefined;
    inputFiles: InputFileInfo[];
    recomputed: Recomputed;
    carried: BundleParts;
    carriedIds: string[];
    withdrawnDocIds: string[];
    warnings: string[];
    previous: PublicationBundle | null;
    options?: RunOptions;
  },
): Promise<RunOutcome> {
  const { mode, rerunDay, inputFiles, recomputed, carried, carriedIds, warnings, previous, options } = params;
  const withdrawnDocIds = params.withdrawnDocIds;

  const publicationId = newPublicationId(mode);
  const token = options?.token ?? newStageToken(mode);

  const accounts = stampPublicationId([...recomputed.accounts, ...carried.accounts], publicationId);
  const quarantines = stampPublicationId([...recomputed.quarantines, ...carried.quarantines], publicationId);
  const lineage = stampPublicationId([...recomputed.lineage, ...carried.lineage], publicationId);

  const { manifest } = buildManifest(
    publicationId,
    mode,
    rerunDay,
    inputFiles,
    accounts,
    quarantines,
    recomputed.connectionIds,
    carriedIds,
    withdrawnDocIds,
    warnings,
  );
  manifest.fingerprint = fingerprintOf(manifest, accounts, quarantines, lineage);
  const bundle: PublicationBundle = { manifest, accounts, quarantines, lineage };

  const plan: StagingPlan = {
    mode,
    rerunDay,
    recomputedConnectionIds: [...recomputed.connectionIds].sort(),
    carriedConnectionIds: [...carriedIds].sort(),
    warnings: [...warnings],
    inputFiles: inputFiles.map((f) => ({ ...f })),
  };

  // 关键区 1：幂等判断 + 暂存持久化。持锁时间只覆盖本地文件操作。
  const lock = await acquirePublicationLock(dataDir, { mode, rerunDay });
  let state: StagingState;
  try {
    const currentNow = await loadCurrentPublication(dataDir);
    if (currentNow && currentNow.manifest.fingerprint === manifest.fingerprint) {
      return {
        publicationId: currentNow.manifest.publicationId,
        skipped: true,
        reason: '相同输入与结算结果，内容指纹一致，跳过重复发布',
        accountCount: currentNow.manifest.accountCount,
        quarantineCount: currentNow.manifest.quarantineCount,
        recomputedConnectionIds: plan.recomputedConnectionIds,
        carriedConnectionIds: plan.carriedConnectionIds,
        withdrawnDocIds,
        warnings,
      };
    }
    state = {
      token,
      publicationId,
      basePublicationId: currentNow?.manifest.publicationId ?? null,
      createdAtMs: Date.now(),
      fingerprint: manifest.fingerprint,
      withdrawnDocIds,
      plan,
      bundle,
    };
    await prepareStaging(dataDir, state);
  } finally {
    await lock.release();
  }

  // 提交前暂停：此时不持锁，另一日期的重跑可正常发布；查询仍只看到完整发布
  if (options?.pause) {
    const signal = await pauseAndWait(options.pause, {
      token,
      publicationId,
      basePublicationId: state.basePublicationId,
      createdAtMs: state.createdAtMs,
    });
    if (signal === 'cancel') {
      // 主动取消：清理本笔暂存，指针保持不变
      await withPublicationLock(dataDir, { mode, rerunDay }, async () => {
        await removeStagingState(dataDir, token);
      });
      throw new Error(`暂存发布 ${token} 已被取消，未提交任何发布`);
    }
  }

  // 关键区 2：恢复后重新持锁；若基准已被并发发布取代，commitUnderLock 内分区合并
  return withPublicationLock(dataDir, { mode, rerunDay }, (lock2) =>
    commitUnderLock(dataDir, state, lock2),
  );
}

/** 全量运行：重新结算输入中的全部接电 */
export async function runFull(
  inputDir: string,
  dataDir: string,
  options?: RunOptions,
): Promise<RunOutcome> {
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

  return finalize(dataDir, {
    mode: 'full',
    rerunDay: undefined,
    inputFiles: files,
    recomputed,
    carried: { accounts: [], quarantines: [], lineage: [] },
    carriedIds: [],
    withdrawnDocIds,
    warnings,
    previous,
    options,
  });
}

/** 指定北京时间日期重跑 */
export async function rerunDay(
  inputDir: string,
  dataDir: string,
  day: string,
  options?: RunOptions,
): Promise<RunOutcome> {
  const [{ connections, readings, files, warnings }, previous, historyTombstones] = await Promise.all([
    ingest(inputDir),
    loadCurrentPublication(dataDir),
    loadTombstones(dataDir),
  ]);

  if (!previous) {
    // 没有历史发布可沿用，退化为全量运行
    return runFull(inputDir, dataDir, options);
  }

  const { connectionIds } = extractRevisionConflictIds(warnings);
  const latestById = new Map(connections.map((c) => [c.connectionId, c]));

  // 纳入集合：最新接电时段与该日重叠，或上一版发布接电时段与该日重叠
  const includedIds = new Set<string>();
  for (const c of connections) {
    if (overlapsDay(c.startBeijingMs, c.endBeijingMs, day)) includedIds.add(c.connectionId);
  }
  const previousConnWindows = new Map<string, { start: number; end: number }>();
  for (const q of previous.quarantines) {
    previousConnWindows.set(q.connectionId, { start: q.startBeijingMs, end: q.endBeijingMs });
  }
  for (const a of previous.accounts) {
    if (!previousConnWindows.has(a.connectionId)) {
      // 从日账窗口拼不出完整区间时，用窗口并集近似（仅用于重叠判断）
      previousConnWindows.set(a.connectionId, {
        start: Math.min(a.windowStartMs),
        end: Math.max(a.windowEndMs),
      });
    } else {
      const w = previousConnWindows.get(a.connectionId)!;
      previousConnWindows.set(a.connectionId, {
        start: Math.min(w.start, a.windowStartMs),
        end: Math.max(w.end, a.windowEndMs),
      });
    }
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

  return finalize(dataDir, {
    mode: 'rerun',
    rerunDay: day,
    inputFiles: files,
    recomputed,
    carried: { accounts: carriedAccounts, quarantines: carriedQuarantines, lineage: carriedLineage },
    carriedIds,
    withdrawnDocIds,
    warnings,
    previous,
    options,
  });
}

/**
 * 恢复一个已暂存（暂停或中断）的发布并提交。
 * 暂存期间若有并发发布落地，自动按接电分区合并，保证两笔互不相干的更正都生效。
 */
export async function resumePublication(dataDir: string, token: string): Promise<RunOutcome> {
  const staged = await readStaging(dataDir, token);
  if (!staged) {
    throw new Error(`暂存发布 ${token} 不存在（可能已提交、已取消或被清理）`);
  }
  return withPublicationLock(dataDir, { mode: staged.plan.mode, rerunDay: staged.plan.rerunDay }, (lock) =>
    commitUnderLock(dataDir, staged, lock),
  );
}

/** 取消一个暂存发布：删除其暂存产物，当前指针不做任何改动 */
export async function cancelStaging(dataDir: string, token: string): Promise<void> {
  const staged = await readStaging(dataDir, token);
  if (!staged) throw new Error(`暂存发布 ${token} 不存在`);
  await withPublicationLock(dataDir, { mode: staged.plan.mode, rerunDay: staged.plan.rerunDay }, async () => {
    await removeStagingState(dataDir, token);
  });
}

/** 列出全部待提交暂存发布（暂停中或中断残留） */
export async function listPendingStaging(dataDir: string): Promise<StagingState[]> {
  return listStaging(dataDir);
}
