import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ingest } from '../src/ingest.js';
import { encodeGb18030 } from '../src/encoding.js';

async function withTempDir(fn: (dir: string) => Promise<void>): Promise<void> {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'ingest-test-'));
  try {
    await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

const csv = (rows: string[]): string =>
  ['connection_id,revision,vessel,meter_id,start_beijing,end_beijing', ...rows].join('\n') + '\n';

test('GB18030 CSV 中文正确解码，JSONL 按 UTC 解析并归一化单位', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'c'));
    await writeFile(
      path.join(dir, 'c', 'a.csv'),
      encodeGb18030(
        'connection_id,revision,vessel,meter_id,start_beijing,end_beijing\n' +
          'CONN-1,1,远洋之星,M1,2026-11-03 08:00:00,2026-11-04 08:00:00\n',
      ),
    );
    await mkdir(path.join(dir, 'r'));
    await writeFile(
      path.join(dir, 'r', 'a.jsonl'),
      JSON.stringify({
        reading_id: 'R1',
        revision: 1,
        meter_id: 'M1',
        sampled_at_utc: '2026-11-03T00:00:00Z',
        unit: 'kWh',
        cumulative: '12.5',
      }) + '\n',
      'utf-8',
    );
    const r = await ingest(dir);
    assert.equal(r.connections[0]!.vessel, '远洋之星');
    assert.equal(r.readings[0]!.cumulativeWh, 12500);
    assert.equal(r.readings[0]!.originalUnit, 'kWh');
    assert.equal(r.readings[0]!.sampledAtMs, Date.UTC(2026, 10, 3, 0, 0, 0));
  });
});

test('同一记录以最高修订为准，低修订被取代', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'c'));
    await writeFile(path.join(dir, 'c', 'a.csv'), encodeGb18030(csv([
      'CONN-1,1,旧船名,M1,2026-11-03 08:00:00,2026-11-04 08:00:00',
      'CONN-1,2,新船名,M1,2026-11-03 08:00:00,2026-11-04 12:00:00',
    ])));
    const r = await ingest(dir);
    assert.equal(r.connections.length, 1);
    assert.equal(r.connections[0]!.revision, 2);
    assert.equal(r.connections[0]!.vessel, '新船名');
    // 高修订完全取代低修订，出处只保留高修订那一行
    assert.equal(r.connections[0]!.sources.length, 1);
    assert.equal(r.connections[0]!.sources[0]!.revision, 2);
  });
});

test('字节相同的重送文件被哈希去重，只处理一次', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'r'));
    const line =
      JSON.stringify({
        reading_id: 'R1',
        revision: 1,
        meter_id: 'M1',
        sampled_at_utc: '2026-11-03T00:00:00Z',
        unit: 'Wh',
        cumulative: '100',
      }) + '\n';
    await writeFile(path.join(dir, 'r', 'first.jsonl'), line, 'utf-8');
    await writeFile(path.join(dir, 'r', 'second-resend.jsonl'), line, 'utf-8');
    const r = await ingest(dir);
    assert.equal(r.readings.length, 1);
    assert.equal(r.files.filter((f) => f.duplicateOf).length, 1);
  });
});

test('同主键同修订内容冲突产生告警且保留先到记录', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'c'));
    await writeFile(path.join(dir, 'c', 'a.csv'), encodeGb18030(csv([
      'CONN-1,1,船名A,M1,2026-11-03 08:00:00,2026-11-04 08:00:00',
      'CONN-1,1,船名B,M1,2026-11-03 08:00:00,2026-11-04 09:00:00',
    ])));
    const r = await ingest(dir);
    assert.equal(r.connections[0]!.vessel, '船名A');
    assert.equal(r.warnings.length, 1);
  });
});
