/**
 * 内容寻址对象存储：不可变文档（blob）+ 持久化有序映射树（tree）。
 *
 * 设计目标（对应「清单/索引随版本翻倍而近似恒定」的要求）：
 *
 * - blob 以规范化 JSON（含结尾换行）的 SHA-256 为文件名，内容相同只落盘一次；
 * - tree 是按键哈希的十六进制位分叉的前缀树，每个节点把「键 → 值」切成
 *   至多 16 份；节点内容同样以 SHA-256 寻址；
 * - 每次发布都会在内存里重建整棵树，但落盘时按内容哈希做存在性检查，
 *   只有本次真正变化的节点（新增键所在的一条路径 + 被改写的叶子）才会
 *   写入新字节。因此清单/索引规模翻倍时，单版新增字节只随树高
 *   log₁₆N 缓慢增长；
 * - 树根 id 记录在发布清单里，读取某个发布只需按根 id 下钻，
 *   无需逐版回放，也不依赖发布顺序；
 * - 所有写入都先写临时文件再原子 rename，且发生在发布指针切换之前，
 *   中断只会留下无人引用的对象，上一完整发布始终可读。
 */

import { createHash } from 'node:crypto';
import { access, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';

/** 叶子最多承载的条目数 */
export const LEAF_MAX = 16;
/** 键哈希共 64 个十六进制位；极端情况下不再分叉 */
const MAX_LEVEL = 64;

export interface TreePair {
  key: string;
  value: unknown;
}

interface SortedEntry {
  h: string;
  key: string;
  value: unknown;
}

interface LeafNode {
  t: 'leaf';
  e: Array<[string, string, unknown]>;
}

interface BranchNode {
  t: 'branch';
  /** [十六进制位, 子节点 id]，按位升序 */
  c: Array<[string, string]>;
}

type TreeNode = LeafNode | BranchNode;

const EMPTY_LEAF: LeafNode = { t: 'leaf', e: [] };

function sha256(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}

/** 规范化 JSON：对象键排序，保证同内容不同属性顺序得到同一哈希 */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

/** blob 的规范落盘文本与内容哈希（二者严格对应） */
export function blobText(value: unknown): string {
  return canonicalJson(value) + '\n';
}

export function hashValue(value: unknown): string {
  return sha256(blobText(value));
}

/**
 * 有并发上限的 map。批量读写数千个对象时避免同时打开过多文件描述符，
 * 也避免一次性持有大量中间结果。
 */
export async function mapLimit<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      out[i] = await fn(items[i]!, i);
    }
  });
  await Promise.all(workers);
  return out;
}

function storeRoot(dataDir: string): string {
  return path.join(dataDir, 'store');
}

function blobFile(dataDir: string, hash: string): string {
  return path.join(storeRoot(dataDir), 'blobs', hash.slice(0, 2), `${hash}.json`);
}

function treeNodesDir(dataDir: string, namespace: string): string {
  return path.join(storeRoot(dataDir), 'trees', namespace, 'nodes');
}

function treeNodeFile(dataDir: string, namespace: string, id: string): string {
  return path.join(treeNodesDir(dataDir, namespace), id.slice(0, 2), `${id}.json`);
}

async function fileExists(file: string): Promise<boolean> {
  try {
    await access(file);
    return true;
  } catch {
    return false;
  }
}

async function writeAtomicIfAbsent(file: string, text: string): Promise<void> {
  if (await fileExists(file)) return;
  await mkdir(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  await writeFile(tmp, text, 'utf-8');
  try {
    await rename(tmp, file);
  } catch (err) {
    // 并发写入相同内容：目标已存在即视为成功
    if (!(await fileExists(file))) throw err;
    await rm(tmp, { force: true });
  }
}

/** 写入不可变文档，返回内容哈希；相同内容重复写入不产生新字节 */
export async function putBlob(dataDir: string, value: unknown): Promise<string> {
  const text = blobText(value);
  const hash = sha256(text);
  await writeAtomicIfAbsent(blobFile(dataDir, hash), text);
  return hash;
}

/** 按已知内容哈希落盘文档（已存在则跳过） */
export async function persistBlob(
  dataDir: string,
  hash: string,
  value: unknown,
): Promise<void> {
  const text = blobText(value);
  if (sha256(text) !== hash) {
    throw new Error(`文档内容与哈希不符：${hash}`);
  }
  await writeAtomicIfAbsent(blobFile(dataDir, hash), text);
}

export async function getBlob<T>(dataDir: string, hash: string): Promise<T> {
  return JSON.parse(await readFile(blobFile(dataDir, hash), 'utf-8')) as T;
}

function sortEntries(pairs: TreePair[]): SortedEntry[] {
  return pairs
    .map((p) => ({ h: sha256(p.key), key: p.key, value: p.value }))
    .sort((a, b) => {
      if (a.h !== b.h) return a.h < b.h ? -1 : 1;
      return a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
    });
}

function groupByNibble(entries: SortedEntry[], level: number): Array<[string, SortedEntry[]]> {
  const groups = new Map<string, SortedEntry[]>();
  for (const e of entries) {
    const nibble = e.h[level] ?? '0';
    const bucket = groups.get(nibble);
    if (bucket) bucket.push(e);
    else groups.set(nibble, [e]);
  }
  return [...groups.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

/** 递归构建节点：返回根 id，并把新节点文本收集到 out */
function buildNode(entries: SortedEntry[], level: number, out: Map<string, string>): string {
  if (entries.length <= LEAF_MAX || level >= MAX_LEVEL) {
    const node: LeafNode = { t: 'leaf', e: entries.map((e) => [e.h, e.key, e.value]) };
    const text = blobText(node);
    const id = sha256(text);
    if (!out.has(id)) out.set(id, text);
    return id;
  }
  const groups = groupByNibble(entries, level);
  if (groups.length === 1) {
    return buildNode(entries, level + 1, out);
  }
  const childIds = groups.map(([, bucket]) => buildNode(bucket, level + 1, out));
  const node: BranchNode = {
    t: 'branch',
    c: groups.map(([nibble], i) => [nibble, childIds[i]!] as [string, string]),
  };
  const text = blobText(node);
  const id = sha256(text);
  if (!out.has(id)) out.set(id, text);
  return id;
}

/**
 * 仅构建树、不落盘：返回根 id 与全部节点文本。
 * 用于先算发布指纹、判定幂等后再决定是否写入。
 */
export interface TreePlan {
  rootId: string;
  nodes: Map<string, string>;
}

/**
 * 构建并持久化一棵有序映射树，返回根 id。
 * 重复调用（相同键值集合）会得到相同根 id，且只做存在性检查。
 */
export async function putTree(
  dataDir: string,
  namespace: string,
  pairs: TreePair[],
): Promise<string> {
  const plan = planTree(pairs);
  await persistTree(dataDir, namespace, plan);
  return plan.rootId;
}

/** 在内存中构建树，不写任何文件 */
export function planTree(pairs: TreePair[]): TreePlan {
  const entries = sortEntries(pairs);
  const out = new Map<string, string>();
  let rootId: string;
  if (entries.length === 0) {
    const text = blobText(EMPTY_LEAF);
    rootId = sha256(text);
    out.set(rootId, text);
  } else {
    rootId = buildNode(entries, 0, out);
  }
  return { rootId, nodes: out };
}

/** 落盘一棵已构建的树；已存在的节点按内容哈希跳过 */
export async function persistTree(
  dataDir: string,
  namespace: string,
  plan: TreePlan,
): Promise<void> {
  await mapLimit([...plan.nodes.entries()], 32, ([id, text]) =>
    writeAtomicIfAbsent(treeNodeFile(dataDir, namespace, id), text),
  );
}

async function readTreeNode(dataDir: string, namespace: string, id: string): Promise<TreeNode> {
  return JSON.parse(await readFile(treeNodeFile(dataDir, namespace, id), 'utf-8')) as TreeNode;
}

/** 按键精确点查：只读取一条树路径，不展开整棵树 */
export async function getTreeValue(
  dataDir: string,
  namespace: string,
  rootId: string,
  key: string,
): Promise<unknown | undefined> {
  const h = sha256(key);
  let node = await readTreeNode(dataDir, namespace, rootId);
  let level = 0;
  while (node.t === 'branch') {
    const nibble = h[level] ?? '0';
    const child = node.c.find(([n]) => n === nibble);
    if (!child) return undefined;
    node = await readTreeNode(dataDir, namespace, child[1]);
    level++;
  }
  const hit = node.e.find(([eh, ek]) => eh === h && ek === key);
  return hit ? hit[2] : undefined;
}

/** 展开整棵树，按键哈希顺序返回全部条目 */
export async function getTreeEntries(
  dataDir: string,
  namespace: string,
  rootId: string,
): Promise<Array<[string, unknown]>> {
  const out: Array<[string, unknown]> = [];
  await collect(dataDir, namespace, rootId, out);
  return out;
}

async function collect(
  dataDir: string,
  namespace: string,
  id: string,
  out: Array<[string, unknown]>,
): Promise<void> {
  const node = await readTreeNode(dataDir, namespace, id);
  if (node.t === 'leaf') {
    for (const [, key, value] of node.e) out.push([key, value]);
    return;
  }
  for (const [, childId] of node.c) {
    await collect(dataDir, namespace, childId, out);
  }
}

/** 清理写入中断留下的临时文件（blob 与所有树的节点） */
export async function cleanupObjectStoreStaging(dataDir: string): Promise<void> {
  const root = storeRoot(dataDir);
  const dirs: string[] = [path.join(root, 'blobs')];
  try {
    const namespaces = await readdir(path.join(root, 'trees'));
    for (const ns of namespaces) dirs.push(path.join(root, 'trees', ns, 'nodes'));
  } catch {
    // 无树目录
  }
  await Promise.all(
    dirs.map(async (dir) => {
      let prefixes: string[];
      try {
        prefixes = await readdir(dir);
      } catch {
        return;
      }
      await Promise.all(
        prefixes.map(async (prefix) => {
          const sub = path.join(dir, prefix);
          let names: string[];
          try {
            names = await readdir(sub);
          } catch {
            return;
          }
          await Promise.all(
            names
              .filter((n) => n.includes('.tmp-'))
              .map((n) => rm(path.join(sub, n), { force: true })),
          );
        }),
      );
    }),
  );
}
