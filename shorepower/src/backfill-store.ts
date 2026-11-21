/**
 * 历史回补作业的持久化与断点续跑。
 *
 * 回补是“按北京时间日期范围、一次发布”的长事务：
 * - 作业状态与逐接电结算暂存全部落在 <data>/backfills/<jobKey>/ 下，
 *   与 publications/ 完全隔离——发布提交前，任何查询接口都看不到半成品；
 * - 每完成一笔接电就把其完整结算结果原子落盘并更新作业状态，
 *   进程中断（即使输入投递目录已整体更换）后重跑同一日期范围即可续跑：
 *   已持久化的接电直接读回、绝不重算；
 * - 暂存内容只与“纳入的接电笔数”有关：只存每笔接电的结算文档、读数出处与
 *   输入文件元数据（文件名/哈希/大小），不复制任何读数明细，因此输入中无关
 *   读数数量翻十倍也不会让续跑暂存量翻倍；
 * - 发布号由日期范围确定性派生，续跑与成功后的重试都复用同一发布号，
 *   历史发布列表不会出现重复版本。
 */

import { mkdir, readFile, readdir, rename, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import type {
  DailyAccount,
  InputFileInfo,
  LineageRecord,
  QuarantineEntry,
  SourceRef,
} from './types.js';

const BACKFILLS_DIR = 'backfills';
const JOB_FILE = 'job.json';
const STAGING_DIR = 'staging';

export type BackfillJobStatus = 'running' | 'committed';

/**
 * 单个接电的已持久化结算结果（发布前形态，publicationId 统一为占位值，
 * 在组装发布时递归盖章为真实发布号）。每笔接电一个文件，数量与无关读数无关。
 */
export interface StagedConnection {
  connectionId: string;
  revision: number;
  accounts: Omit<DailyAccount, 'publicationId'>[];
  quarantines: Omit<QuarantineEntry, 'publicationId'>[];
  lineage: Omit<LineageRecord, 'publicationId'>[];
  /** 该接电结算实际依据的读数出处 */
  readingSources: SourceRef[];
  /** 计算该接电时扫描到的输入文件清单（用于组装血缘，保证追溯到当时的输入版本） */
  inputFiles: InputFileInfo[];
  computedAtMs: number;
}

/** 回补作业状态（原子写 job.json） */
export interface BackfillJobState {
  jobKey: string;
  startDay: string;
  endDay: string;
  /** 日期范围确定性派生的发布号，续跑/重试始终复用 */
  publicationId: string;
  /** 启动时所依据的上一完整发布号（续跑时校验其未被别的发布取代） */
  basePublicationId: string;
  status: BackfillJobStatus;
  createdAtMs: number;
  updatedAtMs: number;
  /** 已完成并持久化暂存的接电 id（与 staging/ 下文件一一对应） */
  completedConnectionIds: string[];
  /** 首次启动确定的纳入集合（续跑时只增不减，保证整笔语义稳定） */
  includedConnectionIds: string[];
}

function backfillsRoot(dataDir: string): string {
  return path.join(dataDir, BACKFILLS_DIR);
}

function jobDir(dataDir: string, jobKey: string): string {
  return path.join(backfillsRoot(dataDir), jobKey);
}

function jobFilePath(dataDir: string, jobKey: string): string {
  return path.join(jobDir(dataDir, jobKey), JOB_FILE);
}

function stagingDirPath(dataDir: string, jobKey: string): string {
  return path.join(jobDir(dataDir, jobKey), STAGING_DIR);
}

/** 同一日期范围的回补作业键（也是确定性发布号的依据） */
export function backfillJobKey(startDay: string, endDay: string): string {
  return `${startDay}_${endDay}`;
}

/** 由日期范围确定性派生发布号：重试/续跑返回同一发布号，历史列表不新增重复版本 */
export function backfillPublicationId(startDay: string, endDay: string): string {
  const compact = (d: string) => d.replaceAll('-', '');
  return `pub-backfill-${compact(startDay)}-${compact(endDay)}`;
}

/** 接电 id 转安全文件名（接电主键通常为 [A-Z0-9-]，这里兜底转义） */
function safeFileName(connectionId: string): string {
  return `${connectionId.replace(/[^A-Za-z0-9_.-]/g, '_')}.json`;
}

async function atomicWrite(file: string, content: string): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  await writeFile(tmp, content);
  await rename(tmp, file);
}

export async function loadBackfillJob(
  dataDir: string,
  startDay: string,
  endDay: string,
): Promise<BackfillJobState | null> {
  const jobKey = backfillJobKey(startDay, endDay);
  try {
    const raw = await readFile(jobFilePath(dataDir, jobKey), 'utf-8');
    return JSON.parse(raw) as BackfillJobState;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw err;
  }
}

export async function saveBackfillJob(state: BackfillJobState, dataDir: string): Promise<void> {
  await atomicWrite(
    jobFilePath(dataDir, state.jobKey),
    JSON.stringify({ ...state, updatedAtMs: Date.now() }, null, 2) + '\n',
  );
}

export async function createBackfillJob(
  dataDir: string,
  startDay: string,
  endDay: string,
  basePublicationId: string,
  includedConnectionIds: string[],
): Promise<BackfillJobState> {
  const jobKey = backfillJobKey(startDay, endDay);
  const now = Date.now();
  const state: BackfillJobState = {
    jobKey,
    startDay,
    endDay,
    publicationId: backfillPublicationId(startDay, endDay),
    basePublicationId,
    status: 'running',
    createdAtMs: now,
    updatedAtMs: now,
    completedConnectionIds: [],
    includedConnectionIds: [...includedConnectionIds].sort(),
  };
  await mkdir(stagingDirPath(dataDir, jobKey), { recursive: true });
  await saveBackfillJob(state, dataDir);
  return state;
}

/** 原子持久化单笔接电的结算结果；已存在则不覆盖（完成记录只增不减） */
export async function stageConnection(
  dataDir: string,
  jobKey: string,
  staged: StagedConnection,
): Promise<void> {
  const file = path.join(stagingDirPath(dataDir, jobKey), safeFileName(staged.connectionId));
  // 已完成的接电不重写、不重算
  try {
    await readFile(file);
    return;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
  }
  await atomicWrite(file, JSON.stringify(staged) + '\n');
}

export async function loadStagedConnection(
  dataDir: string,
  jobKey: string,
  connectionId: string,
): Promise<StagedConnection | null> {
  try {
    const raw = await readFile(
      path.join(stagingDirPath(dataDir, jobKey), safeFileName(connectionId)),
      'utf-8',
    );
    return JSON.parse(raw) as StagedConnection;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw err;
  }
}

export async function listStagedConnectionIds(
  dataDir: string,
  jobKey: string,
): Promise<string[]> {
  try {
    const entries = await readdir(stagingDirPath(dataDir, jobKey));
    return entries.filter((e) => e.endsWith('.json')).map((e) => e.replace(/\.json$/, ''));
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw err;
  }
}

/**
 * 暂存目录总字节数。
 * 仅统计逐接电结算文档与作业状态，不含任何输入读数明细，
 * 用于核对“无关读数增至十倍，续跑暂存量不得翻倍”。
 */
export async function stagingTotalBytes(
  dataDir: string,
  jobKey: string,
): Promise<number> {
  let entries: import('node:fs').Dirent[];
  try {
    entries = await readdir(jobDir(dataDir, jobKey), { recursive: true, withFileTypes: true });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
    throw err;
  }
  let total = 0;
  for (const e of entries) {
    if (!e.isFile()) continue;
    const abs = path.join(e.parentPath ?? jobDir(dataDir, jobKey), e.name);
    const buf = await readFile(abs);
    total += buf.byteLength;
  }
  return total;
}

/** 删除回补作业的全部暂存（正常提交成功后可选清理；默认保留以支撑审计与重试判定） */
export async function removeBackfillJob(dataDir: string, jobKey: string): Promise<void> {
  await rm(jobDir(dataDir, jobKey), { recursive: true, force: true });
}
