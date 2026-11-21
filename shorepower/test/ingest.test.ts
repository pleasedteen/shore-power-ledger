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

test('同主键同修订内容冲突：顺序无关地整笔标记冲突并保留各方出处', async () => {
  // 两个文件的行故意写成相反顺序，验证结果不依赖文件/记录到达顺序
  const makeInput = async (dir: string, fileAFirst: boolean): Promise<void> => {
    await mkdir(path.join(dir, 'c'));
    const rowsA = [
      'CONN-1,1,船名A,M1,2026-11-03 08:00:00,2026-11-04 08:00:00',
      'CONN-1,1,船名B,M1,2026-11-03 08:00:00,2026-11-04 09:00:00',
    ];
    const rowsB = [...rowsA].reverse();
    await writeFile(
      path.join(dir, 'c', fileAFirst ? 'a.csv' : 'b.csv'),
      encodeGb18030(csv(fileAFirst ? rowsA : rowsB)),
    );
    await writeFile(
      path.join(dir, 'c', fileAFirst ? 'b.csv' : 'a.csv'),
      encodeGb18030(csv(fileAFirst ? rowsB : rowsA)),
    );
  };

  for (const fileAFirst of [true, false]) {
    await withTempDir(async (dir) => {
      await makeInput(dir, fileAFirst);
      const r = await ingest(dir);
      assert.equal(r.connections.length, 1);
      const c = r.connections[0]!;
      // 代表记录按（文件、行号）确定选取，两种文件顺序下一致
      assert.equal(c.vessel, '船名A');
      assert.equal(c.revisionConflict, true);
      // 各方出处全部保留（两个文件各含冲突两行，共 4 个来源行）
      assert.equal(c.sources.length, 4);
      assert.deepEqual([...new Set(c.sources.map((s) => s.file))].sort(), ['c/a.csv', 'c/b.csv']);
      assert.equal(r.warnings.length, 1);
    });
  }
});

test('同修订冲突已被更高修订覆盖时不再隔离，且不产生告警', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'c'));
    await writeFile(path.join(dir, 'c', 'a.csv'), encodeGb18030(csv([
      'CONN-1,1,船名A,M1,2026-11-03 08:00:00,2026-11-04 08:00:00',
      'CONN-1,1,船名B,M1,2026-11-03 08:00:00,2026-11-04 09:00:00',
      'CONN-1,2,船名C,M1,2026-11-03 08:00:00,2026-11-04 10:00:00',
    ])));
    const r = await ingest(dir);
    assert.equal(r.connections.length, 1);
    const c = r.connections[0]!;
    assert.equal(c.revision, 2);
    assert.equal(c.vessel, '船名C');
    assert.equal(c.revisionConflict, undefined);
    assert.deepEqual(c.sources.map((s) => s.revision), [2]);
    assert.equal(r.warnings.length, 0);
  });
});

test('同主键同修订等值（含不同单位）读数合并出处，不视为冲突', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'r'));
    await writeFile(path.join(dir, 'r', 'a.jsonl'), [
      JSON.stringify({ reading_id: 'R1', revision: 1, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '10000' }),
      JSON.stringify({ reading_id: 'R1', revision: 1, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'kWh', cumulative: '10.0' }),
    ].join('\n') + '\n', 'utf-8');
    const r = await ingest(dir);
    assert.equal(r.readings.length, 1);
    assert.equal(r.readings[0]!.revisionConflict, undefined);
    assert.equal(r.readings[0]!.sources.length, 2);
    assert.equal(r.warnings.length, 0);
  });
});

test('读数同修订冲突按候选位置各产出一条并带各方出处；换表/换时刻只影响命中位置的接电', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'r'));
    // 乱序双源：两份文件给出同一 reading_id 同修订但不同（电表/时刻/累计值）的候选
    const rowsA = [
      JSON.stringify({ reading_id: 'R9', revision: 2, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '100' }),
    ];
    const rowsB = [
      JSON.stringify({ reading_id: 'R9', revision: 2, meter_id: 'M2', sampled_at_utc: '2026-11-03T08:00:00Z', unit: 'Wh', cumulative: '200' }),
    ];
    await writeFile(path.join(dir, 'r', 'a.jsonl'), rowsA.join('\n') + '\n', 'utf-8');
    await writeFile(path.join(dir, 'r', 'b.jsonl'), rowsB.join('\n') + '\n', 'utf-8');

    const r1 = await ingest(dir);
    assert.equal(r1.warnings.length, 1);
    assert.equal(r1.readings.length, 2);
    // 两个候选位置各自保留最高修订组的全部各方出处
    for (const rr of r1.readings) {
      assert.equal(rr.revisionConflict, true);
      assert.equal(rr.revision, 2);
      assert.deepEqual([...new Set(rr.sources.map((s) => s.file))].sort(), ['r/a.jsonl', 'r/b.jsonl']);
    }
    assert.deepEqual(r1.readings.map((x) => x.meterId).sort(), ['M1', 'M2']);

    // 颠倒文件顺序（重命名）后结果一致
    await rm(path.join(dir, 'r', 'a.jsonl'));
    await rm(path.join(dir, 'r', 'b.jsonl'));
    await writeFile(path.join(dir, 'r', 'a.jsonl'), rowsB.join('\n') + '\n', 'utf-8');
    await writeFile(path.join(dir, 'r', 'b.jsonl'), rowsA.join('\n') + '\n', 'utf-8');
    const r2 = await ingest(dir);
    assert.deepEqual(
      r2.readings.map((x) => [x.meterId, x.sampledAtMs, x.cumulativeWh]).sort(),
      r1.readings.map((x) => [x.meterId, x.sampledAtMs, x.cumulativeWh]).sort(),
    );
  });
});

test('读数低修订冲突被高修订覆盖后不产出冲突标记', async () => {
  await withTempDir(async (dir) => {
    await mkdir(path.join(dir, 'r'));
    await writeFile(path.join(dir, 'r', 'a.jsonl'), [
      JSON.stringify({ reading_id: 'R1', revision: 1, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '100' }),
      JSON.stringify({ reading_id: 'R1', revision: 1, meter_id: 'M9', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '999' }),
      JSON.stringify({ reading_id: 'R1', revision: 2, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '110' }),
    ].join('\n') + '\n', 'utf-8');
    const r = await ingest(dir);
    assert.equal(r.readings.length, 1);
    assert.equal(r.readings[0]!.revision, 2);
    assert.equal(r.readings[0]!.revisionConflict, undefined);
    assert.equal(r.readings[0]!.meterId, 'M1');
    assert.equal(r.warnings.length, 0);
  });
});
