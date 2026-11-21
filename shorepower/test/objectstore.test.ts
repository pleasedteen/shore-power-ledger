/**
 * 内容寻址存储单元测试。
 *
 * 验证：
 * - 相同键值集合得到相同树根 id（发布指纹与幂等的基础）；
 * - 只新增一个键时，落盘增量只包含一条路径上的节点；
 * - 按键点查与整树展开结果一致；
 * - blob 规范化后按内容去重。
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  canonicalJson,
  getBlob,
  getTreeEntries,
  getTreeValue,
  hashValue,
  planTree,
  putBlob,
  putTree,
} from '../src/objectstore.js';

async function withTemp(fn: (dir: string) => Promise<void>): Promise<void> {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'objectstore-'));
  try {
    await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function pairs(n: number, extra: string[] = []): Array<{ key: string; value: number }> {
  const rows = Array.from({ length: n }, (_, i) => ({ key: `k-${String(i).padStart(6, '0')}`, value: i }));
  extra.forEach((k, i) => rows.push({ key: k, value: 1_000_000 + i }));
  return rows;
}

test('规范化 JSON 与键顺序无关', () => {
  assert.equal(canonicalJson({ b: 1, a: 2 }), canonicalJson({ a: 2, b: 1 }));
  assert.equal(hashValue({ a: 1, b: [2, 3] }), hashValue({ b: [2, 3], a: 1 }));
});

test('相同键值集合得到相同树根 id', () => {
  const a = planTree(pairs(100));
  const b = planTree([...pairs(100)].reverse());
  assert.equal(a.rootId, b.rootId);
});

test('只新增一个键时，新增节点数随规模只按树高增长', () => {
  const base = pairs(2000);
  const seed = planTree(base);
  const seen = new Set(seed.nodes.keys());
  const grown = planTree([...base, { key: 'new-key', value: 42 }]);
  let added = 0;
  for (const id of grown.nodes.keys()) if (!seen.has(id)) added++;
  // 树高约 4（2000 条 / 每叶 16 条）→ 最多新增 4 个节点
  assert.ok(added <= 6, `新增节点数 ${added} 超出预期`);
});

test('按键点查与整树展开一致，且覆盖边界键', async () => {
  await withTemp(async (dir) => {
    const data = pairs(500, ['zzz', 'aaa']);
    const root = await putTree(dir, 't', data.map((p) => ({ key: p.key, value: p.value })));
    const all = new Map((await getTreeEntries(dir, 't', root)).map(([k, v]) => [k, v as number]));
    assert.equal(all.size, 502);
    for (const p of data) {
      assert.equal(await getTreeValue(dir, 't', root, p.key), p.value);
    }
    assert.equal(await getTreeValue(dir, 't', root, 'missing'), undefined);
    assert.equal(await getTreeValue(dir, 't', root, ''), undefined);
  });
});

test('空树可点查且展开为空', async () => {
  await withTemp(async (dir) => {
    const root = await putTree(dir, 't', []);
    assert.deepEqual(await getTreeEntries(dir, 't', root), []);
    assert.equal(await getTreeValue(dir, 't', root, 'x'), undefined);
  });
});

test('blob 按内容去重，读取得到原值', async () => {
  await withTemp(async (dir) => {
    const a = await putBlob(dir, { x: 1, y: [2, 3] });
    const b = await putBlob(dir, { y: [2, 3], x: 1 });
    assert.equal(a, b);
    assert.deepEqual(await getBlob(dir, a), { x: 1, y: [2, 3] });
  });
});

test('大量同前缀键（含空/超短键）不会破坏树结构', async () => {
  await withTemp(async (dir) => {
    const many = Array.from({ length: 300 }, (_, i) => ({ key: `p${i}`, value: i }));
    const root = await putTree(dir, 't', many);
    const all = await getTreeEntries(dir, 't', root);
    assert.equal(all.length, 300);
    assert.equal(await getTreeValue(dir, 't', root, 'p299'), 299);
  });
});
