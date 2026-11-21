/**
 * 本地存储层。
 *
 * 目录结构：
 *   <data>/publications/pub-<时间戳>-<随机>/   一次完整、不可变的发布
 *       daily-accounts.jsonl
 *       quarantines.jsonl
 *       lineage.jsonl
 *       manifest.json
 *   <data>/publications/pub-...tmp-xxx/        发布中途退出的残留目录（启动时清理）
 *   <data>/publications/current.json           指向最近一次完整发布的指针（原子 rename）
 *
 * 单次查询只读取一个发布目录，天然实现“同一次完整发布”的快照隔离；
 * 发布中途退出不会改动 current.json，最近一次完整结果始终可查。
 */

import { createHash } from 'node:crypto';
import { mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import type {
  DailyAccount,
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

export async function listPublicationIds(dataDir: string): Promise<string[]> {
  let entries: string[];
  try {
    entries = await readdir(publicationsRoot(dataDir));
  } catch {
    return [];
  }
  // 只承认含 manifest.json 的目录：任何临时/半成品目录（含 .tmp- 暂存）都不会
  // 出现在历史发布清单里，避免“历史列表新增重复版本/幽灵版本”。
  const ids: string[] = [];
  for (const e of entries) {
    if (!e.startsWith('pub-')) continue;
    try {
      await readFile(path.join(publicationsRoot(dataDir), e, 'manifest.json'));
      ids.push(e);
    } catch {
      // 无清单的目录（残留暂存等）不计入历史发布
    }
  }
  return ids.sort();
}

/**
 * 原子发布：先写临时目录，全部文件落盘后 rename 为正式目录，再切换指针。
 * 任一步失败/进程退出，current.json 都保持指向上一完整发布。
 *
 * 可续跑：若正式目录已存在（上一轮在 rename 之后、切换指针之前中断），
 * 校验其清单指纹与本次一致后直接复用该目录、只补切指针，绝不产生重复发布。
 *
 * @param tombstoneDocIds 截至本次发布的全部撤下文档 id（历史墓碑 + 本次新增）
 */
export async function commitPublication(
  dataDir: string,
  bundle: PublicationBundle,
  tombstoneDocIds: string[],
): Promise<{ publicationId: string; skipped: false; reusedDirectory: boolean }> {
  const root = publicationsRoot(dataDir);
  await mkdir(root, { recursive: true });
  const publicationId = bundle.manifest.publicationId;
  const finalDir = path.join(root, publicationId);

  let reusedDirectory = false;
  try {
    const existingRaw = await readFile(path.join(finalDir, 'manifest.json'), 'utf-8');
    const existing = JSON.parse(existingRaw) as PublicationManifest;
    if (existing.fingerprint !== bundle.manifest.fingerprint) {
      throw new Error(
        `发布目录 ${publicationId} 已存在但内容指纹不一致，拒绝覆盖以保证不可变性`,
      );
    }
    // 同一发布号、同一指纹：上一轮在切指针前中断，目录可直接复用
    reusedDirectory = true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
    const tmpDir = path.join(
      root,
      `${publicationId}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`,
    );
    await mkdir(tmpDir, { recursive: true });

    await Promise.all([
      writeFile(path.join(tmpDir, 'daily-accounts.jsonl'), toJsonl(bundle.accounts)),
      writeFile(path.join(tmpDir, 'quarantines.jsonl'), toJsonl(bundle.quarantines)),
      writeFile(path.join(tmpDir, 'lineage.jsonl'), toJsonl(bundle.lineage)),
      writeFile(path.join(tmpDir, 'manifest.json'), JSON.stringify(bundle.manifest, null, 2) + '\n'),
    ]);

    await rename(tmpDir, finalDir);
  }

  // 指针写入同目录临时文件后原子替换；墓碑随指针一并持久化
  const pointerTmp = path.join(root, `${CURRENT_POINTER}.tmp-${process.pid}`);
  const pointer: CurrentPointer = {
    publicationId,
    tombstoneDocIds: [...tombstoneDocIds].sort(),
  };
  await writeFile(pointerTmp, JSON.stringify(pointer, null, 2) + '\n');
  await rename(pointerTmp, pointerPath(dataDir));
  return { publicationId, skipped: false, reusedDirectory };
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
