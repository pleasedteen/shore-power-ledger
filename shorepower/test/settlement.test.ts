import { test } from 'node:test';
import assert from 'node:assert/strict';
import { settleConnection } from '../src/settlement.js';
import { parseBeijingLocal } from '../src/time.js';
import type { ConnectionRecord, MeterReading, SourceRef } from '../src/types.js';

const src = (recordId: string, revision = 1): SourceRef => ({
  file: 'readings.jsonl',
  fileSha256: 'x',
  line: 1,
  recordId,
  revision,
});

function conn(over: Partial<ConnectionRecord> = {}): ConnectionRecord {
  return {
    connectionId: 'C1',
    revision: 1,
    vessel: '测试轮',
    meterId: 'M1',
    startBeijingMs: parseBeijingLocal('2026-11-03 08:00:00'),
    endBeijingMs: parseBeijingLocal('2026-11-04 08:00:00'),
    sources: [src('C1')],
    ...over,
  };
}

function reading(over: Partial<MeterReading> = {}): MeterReading {
  return {
    readingId: 'R',
    revision: 1,
    meterId: 'M1',
    sampledAtMs: 0,
    cumulativeWh: 0,
    originalUnit: 'Wh',
    originalValue: '0',
    sources: [src('R')],
    ...over,
  };
}

const start = parseBeijingLocal('2026-11-03 08:00:00');
const midnight = parseBeijingLocal('2026-11-04 00:00:00');
const end = parseBeijingLocal('2026-11-04 08:00:00');

test('正常跨日接电按午夜读数拆成两条日账', () => {
  const c = conn();
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ];
  const r = settleConnection(c, readings);
  assert.equal(r.quarantine, undefined);
  assert.equal(r.accounts.length, 2);
  assert.deepEqual(
    r.accounts.map((a) => [a.beijingDay, a.usageWh]),
    [
      ['2026-11-03', 4000],
      ['2026-11-04', 3000],
    ],
  );
});

test('kWh 与 Wh 等值读数不构成冲突，结果一致', () => {
  const c = conn({
    startBeijingMs: parseBeijingLocal('2026-11-03 09:00:00'),
    endBeijingMs: parseBeijingLocal('2026-11-03 17:00:00'),
  });
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: parseBeijingLocal('2026-11-03 09:00:00'), cumulativeWh: 500 }),
    reading({ readingId: 'R2', sampledAtMs: parseBeijingLocal('2026-11-03 09:00:00'), cumulativeWh: 0.5 * 1000, originalUnit: 'kWh', originalValue: '0.5' }),
    reading({ readingId: 'R3', sampledAtMs: parseBeijingLocal('2026-11-03 17:00:00'), cumulativeWh: 2000 }),
  ];
  const r = settleConnection(c, readings);
  assert.equal(r.quarantine, undefined);
  assert.equal(r.accounts.length, 1);
  assert.equal(r.accounts[0]!.usageWh, 1500);
});

test('同一时刻读数不一致 -> 整次隔离 CONFLICTING_READING', () => {
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
    reading({ readingId: 'R4', sampledAtMs: midnight, cumulativeWh: 5050 }),
  ];
  const r = settleConnection(conn(), readings);
  assert.deepEqual(r.accounts, []);
  assert.ok(r.quarantine);
  assert.deepEqual(r.quarantine!.reasons, ['CONFLICTING_READING']);
});

test('累计电量回退 -> CUMULATIVE_REGRESSION', () => {
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 900 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ];
  const r = settleConnection(conn(), readings);
  assert.ok(r.quarantine);
  assert.ok(r.quarantine!.reasons.includes('CUMULATIVE_REGRESSION'));
});

test('缺起点/终点/午夜读数分别给出对应原因，可合并多原因', () => {
  const noMidnight = settleConnection(conn(), [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ]);
  assert.deepEqual(noMidnight.quarantine!.reasons, ['MISSING_MIDNIGHT_READING']);

  const noStart = settleConnection(conn(), [
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ]);
  // 起点缺失在首条边界即短路隔离
  assert.deepEqual(noStart.quarantine!.reasons, ['MISSING_START_READING']);

  const noEnd = settleConnection(conn(), [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
  ]);
  assert.equal(noEnd.quarantine!.reasons.includes('MISSING_END_READING'), true);
});

test('非法接电窗口 -> INVALID_CONNECTION_WINDOW', () => {
  const c = conn({
    startBeijingMs: end,
    endBeijingMs: start,
  });
  const r = settleConnection(c, []);
  assert.deepEqual(r.quarantine!.reasons, ['INVALID_CONNECTION_WINDOW']);
});

test('区间外读数不参与结算；区间内其他电表读数被忽略', () => {
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
    reading({ readingId: 'OUT', sampledAtMs: start - 1000, cumulativeWh: 0 }),
    reading({ readingId: 'OTHER', meterId: 'M9', sampledAtMs: midnight, cumulativeWh: 99999 }),
  ];
  const r = settleConnection(conn(), readings);
  assert.equal(r.quarantine, undefined);
  assert.equal(r.accounts[0]!.readingsUsed.length, 2);
});
