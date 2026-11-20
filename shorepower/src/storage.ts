/**
 * 本地存储层。
 *
 * 目录结构：
 *   <data>/publications/pub-<mode>-<时间戳>-<随机>/   一次完整、不可变的发布
 *       daily-accounts.jsonl
 *       quarantines.jsonl
 *       lineage.jsonl
 *       manifest.json
 *   <data>/publications/pub-...tmp-xxx/                发布落盘的残留临时目录（启动时清理）
 *   <data>/publications/current.json                   指向最近一次完整发布的指针（原子 rename）
 *   <data>/publications/pipeline.lock                  跨进程发布互斥锁（持有者崩溃后自动接管）
 *   <data>/publications/.staging/<token>.json          已算完、待提交的暂存发布（进程退出后仍在）
 *   <data>/publications/.staging/<token>.dir/          暂存发布的完整产物（提交时原子物化）
 *
 * 并发与中断语义：
 * - 同一数据目录上的发布在 pipeline.lock 上串行化；持锁进程崩溃（锁残留）会被后来者
 *   依据进程存活状态与锁龄自动接管，不会死锁；
 * - 重跑“算完之后、提交之前”的全部产物先写入 .staging，暂停或被杀都不丢，
 *   恢复时凭 token 继续提交；期间 current.json 不变，查询始终读到完整发布；
 * - 暂存期间另一并发重跑已抢先发布时，恢复提交按接电分区合并两笔更正（见 pipeline.ts），
 *   互不重叠的更正都会生效，撤下墓碑取并集只增不减。
 */

import { createHash } from 'node:crypto';
import { open, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import type {
  DailyAccount,
  InputFileInfo,
  LineageRecord,
  PublicationManifest,
  QuarantineEntry,
} from './types.js';

export interface PublicationBundle {
  manifest: PublicationManifest;
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
}

const PUBLICATIONS_DIR = 'publications';
const CURRENT_POINTER = 'current.json';
const LOCK_FILE = 'pipeline.lock';
const STAGING_DIR = '.staging';
/**
 * 锁持有者存活但锁龄超过该值时，视为僵死并允许接管。
 * 本地发布均为秒级文件操作，5 分钟余量同时覆盖 pid 复用（原持有者已死、pid 被新进程占用）
 * 等极端情况，且远长于任何正常提交，避免误抢存活持有者的锁。
 */
const LOCK_STALE_MS = 300_000;
/** 等锁默认超时：本地串行发布，通常只在另一次重跑暂停/中断时短暂等待 */
const DEFAULT_LOCK_TIMEOUT_MS = 120_000;

interface CurrentPointer {
  publicationId: string;
  /**
   * 跨发布累积的文档墓碑：曾经被撤下的结果文档 id。
   * 重跑时某文档可能在“上一版”里已经不存在，单靠相邻两版对比会让撤下清单
   * 在幂等重跑间抖动；持久墓碑保证撤下记录只增不减、重跑确定。
   */
  tombstoneDocIds: string[];
}

/** 暂存发布的编排信息（恢复合并时需要的全部上下文） */
export interface StagingPlan {
  mode: 'full' | 'rerun';
  rerunDay?: string;
  /** 本次重新结算的接电 id（分区合并时以该集合为界） */
  recomputedConnectionIds: string[];
  /** 本次沿用上一版结果的接电 id */
  carriedConnectionIds: string[];
  warnings: string[];
  inputFiles: InputFileInfo[];
}

/** 已算完、等待提交的暂存发布状态（.staging/<token>.json 的落盘形态） */
export interface StagingState {
  token: string;
  publicationId: string;
  /** 计算时所依据的当前发布 id（无发布时为 null） */
  basePublicationId: string | null;
  createdAtMs: number;
  fingerprint: string;
  /** 截至本次发布的全部撤下文档 id（历史墓碑 + 本次新增） */
  withdrawnDocIds: string[];
  plan: StagingPlan;
  bundle: PublicationBundle;
}

interface LockInfo {
  token: string;
  pid: number;
  acquiredAtMs: number;
  mode: string;
  rerunDay?: string;
}

export interface LockHandle {
  token: string;
  release(): Promise<void>;
}

function publicationsRoot(dataDir: string): string {
  return path.join(dataDir, PUBLICATIONS_DIR);
}

function pointerPath(dataDir: string): string {
  return path.join(publicationsRoot(dataDir), CURRENT_POINTER);
}

function lockPath(dataDir: string): string {
  return path.join(publicationsRoot(dataDir), LOCK_FILE);
}

function stagingRoot(dataDir: string): string {
  return path.join(publicationsRoot(dataDir), STAGING_DIR);
}

function stagingStatePath(dataDir: string, token: string): string {
  return path.join(stagingRoot(dataDir), `${token}.json`);
}

function stagingBundleDir(dataDir: string, token: string): string {
  return path.join(stagingRoot(dataDir), `${token}.dir`);
}

function randomSuffix(): string {
  return Math.random().toString(36).slice(2, 8);
}

/** 读取当前指针；无发布时返回 null（含空墓碑的初始态由提交逻辑建立） */
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

function toJsonl(rows: unknown[]): string {
  return rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length > 0 ? '\n' : '');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** 判断锁持有者进程是否仍存活（EPERM 表示进程在但无权发信号，视为存活） */
function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/**
 * 清理未完成的临时产物：
 * - 旧版发布残留目录 pub-*.tmp-*（不影响任何暂存状态）；
 * - 暂存落盘过程中的 .staging/*.tmp-* 中间目录/文件。
 *
 * 已完成的 .staging/<token>.json 是中断恢复所必需的，绝不在此删除。
 */
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
  let stagingEntries: string[] = [];
  try {
    stagingEntries = await readdir(stagingRoot(dataDir));
  } catch {
    return;
  }
  await Promise.all(
    stagingEntries
      .filter((e) => e.includes('.tmp-'))
      .map((e) => rm(path.join(stagingRoot(dataDir), e), { recursive: true, force: true })),
  );
}

/** 读取当前（最近一次完整）发布；从未发布过时返回 null */
export async function loadCurrentPublication(dataDir: string): Promise<PublicationBundle | null> {
  const pointer = await readPointer(dataDir);
  if (!pointer) return null;
  return loadPublication(dataDir, pointer.publicationId);
}

export async function loadPublication(
  dataDir: string,
  publicationId: string,
): Promise<PublicationBundle | null> {
  const dir = path.join(publicationsRoot(dataDir), publicationId);
  try {
    const [manifest, accounts, quarantines, lineage] = await Promise.all([
      readFile(path.join(dir, 'manifest.json'), 'utf-8').then((t) => JSON.parse(t) as PublicationManifest),
      readJsonl<DailyAccount>(path.join(dir, 'daily-accounts.jsonl')),
      readJsonl<QuarantineEntry>(path.join(dir, 'quarantines.jsonl')),
      readJsonl<LineageRecord>(path.join(dir, 'lineage.jsonl')),
    ]);
    return { manifest, accounts, quarantines, lineage };
  } catch {
    return null;
  }
}

/**
 * 枚举历史完整发布 id。
 * 只认 publications 根目录下 pub- 前缀的正式目录；.staging 下未提交的暂存发布
 * 既不在此列出，也无法凭发布号查到。
 */
export async function listPublicationIds(dataDir: string): Promise<string[]> {
  let entries: string[];
  try {
    entries = await readdir(publicationsRoot(dataDir));
  } catch {
    return [];
  }
  const ids: string[] = [];
  for (const e of entries) {
    if (!e.startsWith('pub-')) continue;
    // 仅保留真正物化完成的目录（临时目录含 .tmp-，排除）
    if (e.includes('.tmp-')) continue;
    try {
      await readFile(path.join(publicationsRoot(dataDir), e, 'manifest.json'), 'utf-8');
      ids.push(e);
    } catch {
      // 目录不完整（理论上 rename 原子性保证不会出现），忽略
    }
  }
  return ids.sort();
}

/**
 * 获取跨进程发布锁。
 *
 * 锁文件以 O_EXCL 方式独占创建；持有者崩溃后文件残留，后来者依据
 * “持有者进程已不存在”或“锁龄超期”判定僵死并接管，保证可恢复且不会死锁。
 */
export async function acquirePublicationLock(
  dataDir: string,
  opts: { mode: string; rerunDay?: string; timeoutMs?: number; token?: string },
): Promise<LockHandle> {
  const root = publicationsRoot(dataDir);
  await mkdir(root, { recursive: true });
  const token = opts.token ?? `lock-${Date.now()}-${randomSuffix()}`;
  const deadline = Date.now() + (opts.timeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS);
  const lPath = lockPath(dataDir);

  for (;;) {
    const info: LockInfo = {
      token,
      pid: process.pid,
      acquiredAtMs: Date.now(),
      mode: opts.mode,
      rerunDay: opts.rerunDay,
    };
    try {
      const fh = await open(lPath, 'wx');
      await fh.writeFile(JSON.stringify(info, null, 2) + '\n', 'utf-8');
      await fh.close();
      return {
        token,
        release: async () => {
          try {
            const raw = JSON.parse(await readFile(lPath, 'utf-8')) as LockInfo;
            if (raw.token === token) await rm(lPath, { force: true });
          } catch {
            // 锁已被接管或删除：不删除别人的锁
          }
        },
      };
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    }

    // 锁被占用：判断是否僵死
    let stale = false;
    try {
      const raw = JSON.parse(await readFile(lPath, 'utf-8')) as LockInfo;
      stale = !processAlive(raw.pid) || Date.now() - raw.acquiredAtMs > LOCK_STALE_MS;
      if (stale) await rm(lPath, { force: true });
    } catch {
      // 读不到说明持有者刚释放，下一轮直接尝试抢占
    }

    if (!stale) {
      if (Date.now() >= deadline) {
        throw new Error('等待发布锁超时：另一次发布仍在进行或暂停中，请稍后重试或先 resume/cancel 暂存发布');
      }
      await sleep(100);
    }
  }
}

/** 在持锁区间内执行关键区，异常时自动释放 */
export async function withPublicationLock<T>(
  dataDir: string,
  opts: { mode: string; rerunDay?: string; timeoutMs?: number },
  fn: (lock: LockHandle) => Promise<T>,
): Promise<T> {
  const lock = await acquirePublicationLock(dataDir, opts);
  try {
    return await fn(lock);
  } finally {
    await lock.release();
  }
}

/**
 * 持久化一个“已算完、待提交”的暂存发布。
 * 先写临时目录/文件再原子 rename：进程在任何一步退出都不会留下半截状态。
 * 同一 token 重复准备（恢复时合并重算）会整体替换旧暂存。
 */
export async function prepareStaging(dataDir: string, state: StagingState): Promise<StagingState> {
  const root = stagingRoot(dataDir);
  await mkdir(root, { recursive: true });

  // 同 token 重入：先清掉旧产物
  await rm(stagingBundleDir(dataDir, state.token), { recursive: true, force: true });

  const tmpBundle = path.join(root, `${state.token}.dir.tmp-${process.pid}-${randomSuffix()}`);
  await mkdir(tmpBundle, { recursive: true });
  await Promise.all([
    writeFile(path.join(tmpBundle, 'daily-accounts.jsonl'), toJsonl(state.bundle.accounts)),
    writeFile(path.join(tmpBundle, 'quarantines.jsonl'), toJsonl(state.bundle.quarantines)),
    writeFile(path.join(tmpBundle, 'lineage.jsonl'), toJsonl(state.bundle.lineage)),
    writeFile(path.join(tmpBundle, 'manifest.json'), JSON.stringify(state.bundle.manifest, null, 2) + '\n'),
  ]);
  await rename(tmpBundle, stagingBundleDir(dataDir, state.token));

  const tmpState = path.join(root, `${state.token}.json.tmp-${process.pid}-${randomSuffix()}`);
  await writeFile(tmpState, JSON.stringify(state, null, 2) + '\n', 'utf-8');
  await rename(tmpState, stagingStatePath(dataDir, state.token));
  return state;
}

export async function readStaging(dataDir: string, token: string): Promise<StagingState | null> {
  try {
    const raw = await readFile(stagingStatePath(dataDir, token), 'utf-8');
    return JSON.parse(raw) as StagingState;
  } catch {
    return null;
  }
}

/** 列出全部待提交暂存发布（含中断残留），按创建时间排序 */
export async function listStaging(dataDir: string): Promise<StagingState[]> {
  let entries: string[];
  try {
    entries = await readdir(stagingRoot(dataDir));
  } catch {
    return [];
  }
  const out: StagingState[] = [];
  for (const e of entries) {
    if (!e.endsWith('.json') || e.includes('.tmp-')) continue;
    try {
      const state = JSON.parse(await readFile(path.join(stagingRoot(dataDir), e), 'utf-8')) as StagingState;
      out.push(state);
    } catch {
      // 损坏的暂存状态跳过
    }
  }
  return out.sort((a, b) => a.createdAtMs - b.createdAtMs);
}

/** 删除暂存发布（提交成功后或取消时调用），best-effort */
export async function removeStagingState(dataDir: string, token: string): Promise<void> {
  await rm(stagingStatePath(dataDir, token), { force: true });
  await rm(stagingBundleDir(dataDir, token), { recursive: true, force: true });
}

/**
 * 将暂存产物物化为正式发布并原子切换指针。
 * 调用方必须持有发布锁，并已处理过“基准发布已被并发更新”的合并。
 */
export async function commitPrepared(
  dataDir: string,
  state: StagingState,
): Promise<{ publicationId: string; skipped: false }> {
  const root = publicationsRoot(dataDir);
  await mkdir(root, { recursive: true });
  const publicationId = state.publicationId;
  const finalDir = path.join(root, publicationId);
  const bundleDir = stagingBundleDir(dataDir, state.token);

  // 崩溃恢复：若上次在“正式目录 rename 完成、指针切换之前”退出，重试时正式目录已存在；
  // 校验确为同一发布后直接复用，避免 rename 抛 ENOTEMPTY，保证 resume 可重入。
  let finalDirReady = false;
  try {
    const existing = JSON.parse(
      await readFile(path.join(finalDir, 'manifest.json'), 'utf-8'),
    ) as PublicationManifest;
    if (existing.publicationId !== publicationId) {
      throw new Error(`发布目录 ${publicationId} 已存在但内容不符，拒绝覆盖`);
    }
    finalDirReady = true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
  }

  if (!finalDirReady) {
    const tmpDir = path.join(root, `${publicationId}.tmp-${process.pid}-${randomSuffix()}`);
    await mkdir(tmpDir, { recursive: true });

    // 直接从暂存目录复制落盘内容，避免“暂存后内存态丢失”（进程被杀恢复场景）
    await Promise.all(
      ['daily-accounts.jsonl', 'quarantines.jsonl', 'lineage.jsonl', 'manifest.json'].map((f) =>
        cpFile(path.join(bundleDir, f), path.join(tmpDir, f)),
      ),
    );
    await rename(tmpDir, finalDir);
  }

  // 指针写入同目录临时文件后原子替换；墓碑随指针一并持久化
  const pointerTmp = path.join(root, `${CURRENT_POINTER}.tmp-${process.pid}-${randomSuffix()}`);
  const pointer: CurrentPointer = {
    publicationId,
    tombstoneDocIds: [...state.withdrawnDocIds].sort(),
  };
  await writeFile(pointerTmp, JSON.stringify(pointer, null, 2) + '\n');
  await rename(pointerTmp, pointerPath(dataDir));
  await removeStagingState(dataDir, state.token);
  return { publicationId, skipped: false };
}

async function cpFile(src: string, dst: string): Promise<void> {
  await writeFile(dst, await readFile(src));
}

/**
 * 幂等指纹：对发布的业务内容做规范化哈希。
 * 剔除 publicationId/createdAt 等易变字段，相同输入重跑得到相同指纹，
 * 从而“相同输入重跑不会产生重复结果”。
 */
export function contentFingerprint(bundle: {
  manifest: Omit<PublicationManifest, 'publicationId' | 'createdAtMs' | 'fingerprint'>;
  accounts: DailyAccount[];
  quarantines: QuarantineEntry[];
  lineage: LineageRecord[];
}): string {
  // 清单运行时仍带 publicationId/createdAtMs（类型上 Omit 仅为编译期约束），
  // 这里显式剔除，避免易变字段污染指纹。
  const {
    publicationId: _pubId,
    createdAtMs: _createdAt,
    fingerprint: _fp,
    ...manifestStable
  } = bundle.manifest as PublicationManifest;
  const stable = {
    manifest: stripVolatile(manifestStable),
    accounts: bundle.accounts.map((d) => stripVolatile(d)),
    quarantines: bundle.quarantines.map((d) => stripVolatile(d)),
    lineage: bundle.lineage.map((d) => stripVolatile(d)),
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
