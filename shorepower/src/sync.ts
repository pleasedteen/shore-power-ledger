/**
 * 下游差量同步。
 *
 * 两个“已提交发布”之间按结果文档 id 对日账、隔离、血缘三类文档做净变更：
 * - added：基版没有、目标版新增的文档；
 * - replaced：两版都有同一 docId、但业务内容（剔除 publicationId 后）发生变化的文档，载荷为目标版文档；
 * - removed：基版有、目标版已删除的文档（含隔离转日账等形态切换），仅传 docId。
 *
 * 仅发布号变化、或中间改过又恢复原内容的文档，剔除 publicationId 规范化后逐字节等价，
 * 不进入任何差量分组，不重传。应用差量后的三类文档与血缘，按目标版重新读取时与目标版完全一致。
 *
 * 下游应用带严格前置校验（基线不匹配即拒绝，绝不静默错账），应用后再用发布指纹复核，
 * 最后以原子指针切换到物化的目标版发布目录；旧版查询入口与存储布局保持不变。
 */

import { mkdir, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import {
  contentFingerprint,
  loadTombstones,
  stableDocKey,
  writePointerAtomic,
  type PublicationBundle,
} from './storage.js';
import type {
  DailyAccount,
  LineageRecord,
  PublicationManifest,
  QuarantineEntry,
} from './types.js';

/** 一类文档的净变更分组 */
export interface DeltaGroup<T> {
  /** 目标版新增的完整文档（已剔除源发布号，应用时盖目标发布号） */
  added: T[];
  /** docId 相同但内容变化的文档，载荷为目标版完整文档 */
  replaced: T[];
  /** 目标版不再保留的文档 id */
  removed: string[];
}

export interface PublicationDelta {
  docType: 'publication-delta';
  /** 基版发布 id；引导同步（下游尚无发布）时为 null */
  fromPublicationId: string | null;
  /** 目标版发布 id，应用成功后下游指针指向它 */
  toPublicationId: string;
  /** 目标版清单（与目标发布目录中的 manifest.json 一致） */
  targetManifest: PublicationManifest;
  /** 目标版业务内容指纹，应用后用于复核 */
  targetFingerprint: string;
  accounts: DeltaGroup<DailyAccount>;
  quarantines: DeltaGroup<QuarantineEntry>;
  lineage: DeltaGroup<LineageRecord>;
  /**
   * 目标版三类文档的 docId 落盘顺序。下游应用差量后按此顺序重排，
   * 使物化发布与上游目标版不仅集合等价、连 JSONL 行序也一致（逐字节可比）。
   */
  targetOrder: {
    accounts: string[];
    quarantines: string[];
    lineage: string[];
  };
  stats: DeltaStats;
}

export interface DeltaStats {
  /** 三类文档的差量明细行数（added+replaced 文档行 + removed id 行 + 血缘同类计数） */
  deltaRows: number;
  /** 目标版三类文档总行数（整版口径） */
  fullRows: number;
  /** 差量明细行数占整版行数的比例 */
  deltaRowRatio: number;
  /** 三类文档各自的净变更计数 */
  perKind: {
    accounts: { added: number; replaced: number; removed: number };
    quarantines: { added: number; replaced: number; removed: number };
    lineage: { added: number; replaced: number; removed: number };
  };
}

interface DocWithId {
  docId: string;
}

function byId<T extends DocWithId>(docs: T[]): Map<string, T> {
  return new Map(docs.map((d) => [d.docId, d]));
}

/** 单类文档按 docId 求净变更；输出分组按 docId 排序，保证线上字节确定 */
function diffDocs<T extends DocWithId>(base: T[], target: T[]): DeltaGroup<T> {
  const baseMap = byId(base);
  const targetMap = byId(target);
  const added: T[] = [];
  const replaced: T[] = [];
  const removed: string[] = [];

  for (const [id, targetDoc] of targetMap) {
    const baseDoc = baseMap.get(id);
    if (!baseDoc) {
      added.push(targetDoc);
    } else if (stableDocKey(baseDoc) !== stableDocKey(targetDoc)) {
      // 剔除 publicationId 后比较：仅发布号变化、或改后又恢复原内容的行不会进入这里
      replaced.push(targetDoc);
    }
  }
  for (const id of baseMap.keys()) {
    if (!targetMap.has(id)) removed.push(id);
  }

  const cmp = (a: DocWithId, b: DocWithId) => a.docId.localeCompare(b.docId);
  added.sort(cmp);
  replaced.sort(cmp);
  removed.sort();
  return { added, replaced, removed };
}

function groupSize(g: DeltaGroup<unknown>): number {
  return g.added.length + g.replaced.length + g.removed.length;
}

/**
 * 计算两个已提交发布之间的净差量。
 * @param from 基版发布；null 表示下游引导同步，目标版全部文档计入 added
 */
export function buildPublicationDelta(
  from: PublicationBundle | null,
  to: PublicationBundle,
): PublicationDelta {
  const accounts = diffDocs(from?.accounts ?? [], to.accounts);
  const quarantines = diffDocs(from?.quarantines ?? [], to.quarantines);
  const lineage = diffDocs(from?.lineage ?? [], to.lineage);

  const deltaRows = groupSize(accounts) + groupSize(quarantines) + groupSize(lineage);
  const fullRows = to.accounts.length + to.quarantines.length + to.lineage.length;
  const stats: DeltaStats = {
    deltaRows,
    fullRows,
    deltaRowRatio: fullRows === 0 ? 0 : deltaRows / fullRows,
    perKind: {
      accounts: { added: accounts.added.length, replaced: accounts.replaced.length, removed: accounts.removed.length },
      quarantines: { added: quarantines.added.length, replaced: quarantines.replaced.length, removed: quarantines.removed.length },
      lineage: { added: lineage.added.length, replaced: lineage.replaced.length, removed: lineage.removed.length },
    },
  };

  return {
    docType: 'publication-delta',
    fromPublicationId: from ? from.manifest.publicationId : null,
    toPublicationId: to.manifest.publicationId,
    targetManifest: to.manifest,
    targetFingerprint: to.manifest.fingerprint,
    accounts,
    quarantines,
    lineage,
    targetOrder: {
      accounts: to.accounts.map((d) => d.docId),
      quarantines: to.quarantines.map((d) => d.docId),
      lineage: to.lineage.map((d) => d.docId),
    },
    stats,
  };
}

/** 递归把文档任意层级的 publicationId 盖成目标发布号（血缘等嵌套结构同样覆盖） */
function restampPublicationId<T>(value: T, publicationId: string): T {
  if (Array.isArray(value)) {
    return value.map((v) => restampPublicationId(v, publicationId)) as T;
  }
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = k === 'publicationId' ? publicationId : restampPublicationId(v, publicationId);
    }
    return out as T;
  }
  return value;
}

/**
 * 对一类文档应用净变更，并严格校验前置条件：
 * - replaced/removed 的 docId 必须存在于本地基线；
 * - added 的 docId 必须不存在于本地基线。
 * 任一不满足即拒绝（通常意味着下游落后/分叉，应改用正确的基版重新拉取）。
 */
function applyGroup<T extends DocWithId>(
  base: T[],
  group: DeltaGroup<T>,
  publicationId: string,
  label: string,
): T[] {
  const map = byId(base);
  for (const doc of group.added) {
    if (map.has(doc.docId)) {
      throw new Error(`差量应用失败：${label} 新增文档 ${doc.docId} 在本地基线已存在`);
    }
    map.set(doc.docId, doc);
  }
  for (const doc of group.replaced) {
    if (!map.has(doc.docId)) {
      throw new Error(`差量应用失败：${label} 替换文档 ${doc.docId} 在本地基线不存在`);
    }
    map.set(doc.docId, doc);
  }
  for (const id of group.removed) {
    if (!map.has(id)) {
      throw new Error(`差量应用失败：${label} 删除文档 ${id} 在本地基线不存在`);
    }
    map.delete(id);
  }
  return [...map.values()].map((d) => restampPublicationId(d, publicationId));
}

function toJsonl(rows: unknown[]): string {
  return rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length > 0 ? '\n' : '');
}

export interface SyncOutcome {
  publicationId: string;
  /** true 表示目标版发布目录已存在（此前已同步过），本次未重复写入 */
  alreadyPresent: boolean;
  accountCount: number;
  quarantineCount: number;
  lineageCount: number;
  deltaRows: number;
}

/**
 * 在下游数据目录应用差量并物化目标版发布：
 * 1. 校验差量的基版与本地当前发布一致（引导同步要求本地尚无发布）；
 * 2. 三类文档分别过严格前置校验后应用差量，并盖目标发布号；
 * 3. 用目标版业务指纹复核物化结果，与目标版不一致即中止、不切指针；
 * 4. 原子写入不可变发布目录并切换 current.json，墓碑并入目标版撤下清单与本地历史墓碑。
 */
export async function materializeSyncedPublication(
  dataDir: string,
  delta: PublicationDelta,
  base: PublicationBundle | null,
): Promise<SyncOutcome> {
  if (delta.fromPublicationId === null) {
    if (base) {
      throw new Error('引导差量只能应用于尚无发布的下游数据目录');
    }
  } else if (!base || base.manifest.publicationId !== delta.fromPublicationId) {
    const localId = base ? base.manifest.publicationId : '（无发布）';
    throw new Error(
      `差量基线不匹配：差量基于 ${delta.fromPublicationId}，本地当前为 ${localId}`,
    );
  }

  const targetId = delta.toPublicationId;
  const accountsRaw = applyGroup(base?.accounts ?? [], delta.accounts, targetId, '日账');
  const quarantinesRaw = applyGroup(base?.quarantines ?? [], delta.quarantines, targetId, '隔离');
  const lineageRaw = applyGroup(base?.lineage ?? [], delta.lineage, targetId, '血缘');

  // 按目标版落盘顺序重排：docId 集合必须与目标顺序一一对应，否则视为差量与清单不一致
  const reorder = <T extends DocWithId>(rows: T[], order: string[], label: string): T[] => {
    const map = byId(rows);
    if (map.size !== order.length || order.some((id) => !map.has(id))) {
      throw new Error(`差量应用后 ${label} 文档集合与目标版顺序清单不一致`);
    }
    return order.map((id) => map.get(id)!);
  };
  const accounts = reorder(accountsRaw, delta.targetOrder.accounts, '日账');
  const quarantines = reorder(quarantinesRaw, delta.targetOrder.quarantines, '隔离');
  const lineage = reorder(lineageRaw, delta.targetOrder.lineage, '血缘');

  // 清单计数与目标版核对
  if (accounts.length !== delta.targetManifest.accountCount) {
    throw new Error(
      `差量应用后日账计数 ${accounts.length} 与目标版 ${delta.targetManifest.accountCount} 不一致`,
    );
  }
  if (quarantines.length !== delta.targetManifest.quarantineCount) {
    throw new Error(
      `差量应用后隔离计数 ${quarantines.length} 与目标版 ${delta.targetManifest.quarantineCount} 不一致`,
    );
  }

  // 指纹复核：应用差量后的三类文档与血缘，按目标版口径必须与目标版完全一致
  const manifest = restampPublicationId(delta.targetManifest, targetId);
  const fingerprint = contentFingerprint({ manifest, accounts, quarantines, lineage });
  if (fingerprint !== delta.targetFingerprint) {
    throw new Error('差量应用后内容指纹与目标版不一致，已中止同步（未切换指针）');
  }

  const root = path.join(dataDir, 'publications');
  const finalDir = path.join(root, targetId);
  const historyTombstones = await loadTombstones(dataDir);
  const mergedTombstones = [...new Set([...historyTombstones, ...manifest.withdrawnDocIds])].sort();

  // 幂等：目标版目录已存在时不重复写盘，仅确保指针指向它（重启后重复拉取安全）
  let alreadyPresent = false;
  await mkdir(root, { recursive: true });
  try {
    await mkdir(finalDir, { recursive: false });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    alreadyPresent = true;
  }

  if (!alreadyPresent) {
    const tmpDir = path.join(
      root,
      `${targetId}.tmp-sync-${process.pid}-${Math.random().toString(36).slice(2, 8)}`,
    );
    await mkdir(tmpDir, { recursive: true });
    await Promise.all([
      writeFile(path.join(tmpDir, 'daily-accounts.jsonl'), toJsonl(accounts)),
      writeFile(path.join(tmpDir, 'quarantines.jsonl'), toJsonl(quarantines)),
      writeFile(path.join(tmpDir, 'lineage.jsonl'), toJsonl(lineage)),
      writeFile(path.join(tmpDir, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n'),
    ]);
    await rename(tmpDir, finalDir);
  }

  await writePointerAtomic(dataDir, targetId, mergedTombstones);
  return {
    publicationId: targetId,
    alreadyPresent,
    accountCount: accounts.length,
    quarantineCount: quarantines.length,
    lineageCount: lineage.length,
    deltaRows: delta.stats.deltaRows,
  };
}
