/**
 * 批次审计：为每一次 run / rerun 分配尝试编号，并记录该次尝试
 * 使用了哪个基线发布、最终发布结果，以及每条接电为何被纳入、沿用或撤下。
 *
 * 设计要点（与发布机制配合，保证进程在发布切换前后退出时判定仍准确）：
 *
 * - 尝试记录写入 `<data>/audit/attempts/<attemptId>.json`，原子替换，自包含；
 *   按编号查询只读这一个文件（必要时再读一次 current 指针），
 *   读取量与日账条数、历史发布版本数无关。
 * - 发布切换（原子替换 current.json）时把尝试编号一并写入指针，
 *   指针切换因此是“该尝试已发布”的持久证据。
 * - 进入发布前先落盘 `publishing` 状态（含目标发布 id）并写 pending 标记；
 *   指针切换成功后写 `published` 终态并删除 pending 标记。
 *      · 切换前退出：指针不会指向该尝试 → 判定“未完成”；
 *      · 切换后、终态落盘前退出：指针仍指向该尝试 → 查询按指针判定“已发布”；
 *      · 之后又发布了新版本：新运行开始时会先对 pending 尝试做一次对账，
 *        用当时的指针把该尝试固化为“已发布”，因此旧尝试判定不会随后续发布漂移。
 * - 幂等跳过不切换指针，直接记为 `skipped`（与“已发布”区分）。
 */

import { mkdir, readdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { readPointer } from './storage.js';

export type AttemptStatus = 'published' | 'skipped' | 'incomplete';

/** 记录在尝试文件里的内部阶段（比对外状态更细，便于对账） */
type AttemptPhase = 'running' | 'publishing' | 'published' | 'skipped' | 'incomplete';

export interface AttemptBaseline {
  /** 本次尝试开始时 current 指针指向的发布 id */
  publicationId: string;
  /** 该基线的内容指纹（若可读取） */
  fingerprint?: string;
}

/** 单条接电在本次尝试中的处置 */
export interface AttemptConnectionDecision {
  connectionId: string;
  /** recomputed=纳入重算；carried=沿用；withdrawn=旧结果撤下 */
  action: 'recomputed' | 'carried' | 'withdrawn';
  /** 人类可读中文原因 */
  reason: string;
  /** 纳入判断依据：latest=最新接电窗口与目标日重叠；previous=上一版窗口与目标日重叠 */
  basis?: Array<'latest' | 'previous'>;
  /** 该接电的最新修订号（纳入重算时） */
  revision?: number;
  /** 该接电在本次发布中被撤下的旧文档 id */
  withdrawnDocIds?: string[];
}

export interface AttemptResult {
  publicationId: string;
  skipped: boolean;
  reason?: string;
  accountCount: number;
  quarantineCount: number;
  withdrawnDocIds: string[];
}

export interface AttemptRecord {
  docType: 'audit-attempt';
  attemptId: string;
  /** 实际执行的模式（rerun 在无基线时退化为 full） */
  mode: 'full' | 'rerun';
  rerunDay?: string;
  startedAtMs: number;
  finishedAtMs?: number;
  phase: AttemptPhase;
  baseline: AttemptBaseline | null;
  /** 本尝试计划/实际切换到的发布 id */
  publicationId?: string;
  result?: AttemptResult;
  decisions: AttemptConnectionDecision[];
  warnings: string[];
  /** 未完成时的原因（进程退出、异常等） */
  incompleteReason?: string;
  /** 对账/判定说明 */
  note?: string;
}

/** 对外查询视图：状态已收敛为 已发布 / 幂等跳过 / 未完成 */
export interface AttemptAudit {
  attemptId: string;
  status: AttemptStatus;
  statusLabel: '已发布' | '幂等跳过' | '未完成';
  mode: 'full' | 'rerun';
  rerunDay: string | null;
  startedAt: string;
  finishedAt: string | null;
  baseline: AttemptBaseline | null;
  /** 发布结果（未完成时为 null，可能含目标发布 id） */
  result: AttemptResult | null;
  /** 进程已进入发布但未能确认时给出的目标发布 id */
  pendingPublicationId: string | null;
  decisions: AttemptConnectionDecision[];
  warnings: string[];
  note: string | null;
}

const AUDIT_DIR = 'audit';
const ATTEMPTS_DIR = 'attempts';
const PENDING_DIR = 'pending';

function auditRoot(dataDir: string): string {
  return path.join(dataDir, AUDIT_DIR);
}
function attemptsDir(dataDir: string): string {
  return path.join(auditRoot(dataDir), ATTEMPTS_DIR);
}
function pendingDir(dataDir: string): string {
  return path.join(auditRoot(dataDir), PENDING_DIR);
}
function attemptFile(dataDir: string, attemptId: string): string {
  return path.join(attemptsDir(dataDir), `${attemptId}.json`);
}
function pendingFile(dataDir: string, attemptId: string): string {
  return path.join(pendingDir(dataDir), `${attemptId}.json`);
}

function newAttemptId(mode: 'full' | 'rerun'): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
  const rand = Math.random().toString(36).slice(2, 8);
  return `att-${mode}-${ts}-${rand}`;
}

async function writeJsonAtomic(file: string, value: unknown): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  await writeFile(tmp, JSON.stringify(value, null, 2) + '\n');
  await rename(tmp, file);
}

async function loadAttemptRecord(
  dataDir: string,
  attemptId: string,
): Promise<AttemptRecord | null> {
  try {
    const raw = JSON.parse(await readFile(attemptFile(dataDir, attemptId), 'utf-8')) as AttemptRecord;
    return {
      ...raw,
      decisions: raw.decisions ?? [],
      warnings: raw.warnings ?? [],
    };
  } catch {
    return null;
  }
}

export interface AttemptHandle {
  attemptId: string;
  /** 进入发布前：记录目标发布 id、发布结果摘要与接电处置 */
  markPublishing(publicationId: string, summary: Omit<AttemptResult, 'publicationId' | 'skipped' | 'reason'>): Promise<void>;
  /** 指针切换成功后的终态 */
  markPublished(): Promise<void>;
  /** 幂等跳过（不切换指针） */
  markSkipped(publicationId: string, reason: string, summary: Omit<AttemptResult, 'publicationId' | 'skipped' | 'reason'>): Promise<void>;
  /** 异常终止（正常抛错路径；进程被强杀时不会执行） */
  markIncomplete(reason: string): Promise<void>;
  /** 写入本次尝试的接电处置与告警（可在 mark* 之前调用） */
  setDecisions(decisions: AttemptConnectionDecision[], warnings: string[]): Promise<void>;
  snapshot(): AttemptRecord;
}

/**
 * 开始一次尝试：立即分配编号并落盘 running 状态。
 * baseline 来自调用方在开始时读取的 current 指针。
 */
export async function startAttempt(
  dataDir: string,
  opts: { mode: 'full' | 'rerun'; rerunDay?: string; baseline: AttemptBaseline | null },
): Promise<AttemptHandle> {
  const attemptId = newAttemptId(opts.mode);
  const record: AttemptRecord = {
    docType: 'audit-attempt',
    attemptId,
    mode: opts.mode,
    rerunDay: opts.rerunDay,
    startedAtMs: Date.now(),
    phase: 'running',
    baseline: opts.baseline,
    decisions: [],
    warnings: [],
  };
  await writeJsonAtomic(attemptFile(dataDir, attemptId), record);

  const persist = () => writeJsonAtomic(attemptFile(dataDir, attemptId), record);

  return {
    attemptId,
    async setDecisions(decisions, warnings) {
      record.decisions = [...decisions].sort((a, b) => a.connectionId.localeCompare(b.connectionId));
      record.warnings = [...warnings];
      await persist();
    },
    async markPublishing(publicationId, summary) {
      record.phase = 'publishing';
      record.publicationId = publicationId;
      record.result = { publicationId, skipped: false, ...summary };
      // pending 标记用于后续运行对账：指针若已切到本尝试，则固化“已发布”
      await writeJsonAtomic(pendingFile(dataDir, attemptId), { attemptId });
      await persist();
    },
    async markPublished() {
      record.phase = 'published';
      record.finishedAtMs = Date.now();
      await persist();
      await rm(pendingFile(dataDir, attemptId), { force: true });
    },
    async markSkipped(publicationId, reason, summary) {
      record.phase = 'skipped';
      record.publicationId = publicationId;
      record.finishedAtMs = Date.now();
      record.result = { publicationId, skipped: true, reason, ...summary };
      await persist();
    },
    async markIncomplete(reason) {
      record.phase = 'incomplete';
      record.incompleteReason = reason;
      record.finishedAtMs = Date.now();
      await persist();
      await rm(pendingFile(dataDir, attemptId), { force: true });
    },
    snapshot() {
      return JSON.parse(JSON.stringify(record)) as AttemptRecord;
    },
  };
}

/**
 * 对账 pending 尝试：在下一次运行动手改指针之前调用。
 * - 指针尝试编号与 pending 尝试一致 → 该尝试确实完成过指针切换，固化“已发布”；
 * - 否则 → 指针从未切到它，固化“未完成”。
 * 因为任何后续发布都先经过本对账，旧尝试的判定不会随后续发布而变化。
 */
export async function reconcilePendingAttempts(dataDir: string): Promise<void> {
  let names: string[];
  try {
    names = await readdir(pendingDir(dataDir));
  } catch {
    return;
  }
  const markers = names.filter((n) => n.endsWith('.json'));
  if (markers.length === 0) return;

  const pointer = await readPointer(dataDir);

  for (const name of markers) {
    const attemptId = name.slice(0, -'.json'.length);
    const record = await loadAttemptRecord(dataDir, attemptId);
    if (!record) {
      await rm(path.join(pendingDir(dataDir), name), { force: true });
      continue;
    }
    if (record.phase === 'published' || record.phase === 'skipped') {
      // 终态已落盘，仅残留 pending 标记
      await rm(pendingFile(dataDir, attemptId), { force: true });
      continue;
    }
    if (pointer?.attemptId === attemptId) {
      record.phase = 'published';
      record.finishedAtMs = record.finishedAtMs ?? Date.now();
      record.note = '进程在发布切换后、审计终态落盘前退出：依据指针中的尝试编号对账判定为已发布';
    } else {
      record.phase = 'incomplete';
      record.finishedAtMs = record.finishedAtMs ?? Date.now();
      record.incompleteReason =
        record.incompleteReason ?? '进程在发布切换前退出，current 指针未切换到本尝试';
    }
    await writeJsonAtomic(attemptFile(dataDir, attemptId), record);
    await rm(pendingFile(dataDir, attemptId), { force: true });
  }
}

/**
 * 按尝试编号查询审计视图。
 * 只读取该尝试的记录文件；仅当状态尚未收敛时才额外读取一次 current 指针。
 * 不枚举历史发布、不加载日账/隔离/血缘。
 */
export async function readAttemptAudit(
  dataDir: string,
  attemptId: string,
): Promise<AttemptAudit | null> {
  const record = await loadAttemptRecord(dataDir, attemptId);
  if (!record) return null;

  let status: AttemptStatus;
  let note = record.note ?? null;
  let pendingPublicationId: string | null = null;

  if (record.phase === 'published') {
    status = 'published';
  } else if (record.phase === 'skipped') {
    status = 'skipped';
  } else if (record.phase === 'publishing') {
    const pointer = await readPointer(dataDir);
    if (pointer?.attemptId === attemptId && pointer.publicationId === record.publicationId) {
      // 切换后、终态落盘前退出：指针就是证据
      status = 'published';
      note = note ?? '进程在发布切换后、审计终态落盘前退出：指针中的尝试编号与本次一致，判定已发布';
    } else {
      status = 'incomplete';
      pendingPublicationId = record.publicationId ?? null;
      note = note ?? '进程在发布切换前退出，current 指针未切换到本尝试';
    }
  } else {
    status = 'incomplete';
    note = note ?? record.incompleteReason ?? '尝试未进入发布阶段';
  }

  const statusLabel: AttemptAudit['statusLabel'] =
    status === 'published' ? '已发布' : status === 'skipped' ? '幂等跳过' : '未完成';

  return {
    attemptId: record.attemptId,
    status,
    statusLabel,
    mode: record.mode,
    rerunDay: record.rerunDay ?? null,
    startedAt: new Date(record.startedAtMs).toISOString(),
    finishedAt: record.finishedAtMs ? new Date(record.finishedAtMs).toISOString() : null,
    baseline: record.baseline,
    result: status === 'published' || status === 'skipped' ? (record.result ?? null) : null,
    pendingPublicationId,
    decisions: record.decisions,
    warnings: record.warnings,
    note,
  };
}
