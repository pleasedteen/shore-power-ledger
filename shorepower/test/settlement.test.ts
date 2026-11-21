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

test('接电记录同修订冲突 -> 整笔 SOURCE_REVISION_CONFLICT，证据保留各方出处', () => {
  const c = conn({
    revisionConflict: true,
    sources: [src('C1', 1), { ...src('C1', 1), file: 'other.csv', line: 7 }],
  });
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ];
  const r = settleConnection(c, readings);
  assert.deepEqual(r.accounts, []);
  assert.ok(r.quarantine);
  assert.deepEqual(r.quarantine!.reasons, ['SOURCE_REVISION_CONFLICT']);
  const ev = r.quarantine!.evidence.find((e) => e.sources && e.sources.length === 2);
  assert.ok(ev, '冲突证据应携带各方出处');
  assert.deepEqual([...new Set(ev!.sources!.map((s) => s.file))].sort(), ['other.csv', 'readings.jsonl']);
});

test('读数同修订冲突只隔离命中候选位置的接电，且每笔接电只产出一条隔离', () => {
  // R9 的两个冲突候选：M1@start（命中 C1）与 M2@另一时刻（不命中 C1）
  const conflictOnM1 = reading({
    readingId: 'R9',
    meterId: 'M1',
    sampledAtMs: start,
    cumulativeWh: 1000,
    revisionConflict: true,
    sources: [src('R9', 2), { ...src('R9', 2), file: 'late.jsonl', line: 3 }],
  });
  const conflictOnM2 = reading({
    readingId: 'R9',
    meterId: 'M2',
    sampledAtMs: start + 3600_000,
    cumulativeWh: 2000,
    revisionConflict: true,
    sources: [src('R9', 2), { ...src('R9', 2), file: 'late.jsonl', line: 3 }],
  });
  const readings = [
    conflictOnM1,
    conflictOnM2,
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ];

  const r = settleConnection(conn(), readings);
  assert.ok(r.quarantine);
  assert.deepEqual(r.quarantine!.reasons, ['SOURCE_REVISION_CONFLICT']);
  // 整笔隔离：没有任何日账，且只有一条隔离文档
  assert.deepEqual(r.accounts, []);
  assert.equal(r.quarantine!.docId, 'QUAR-C1');
  const ev = r.quarantine!.evidence.find((e) => e.sources && e.sources.length === 2);
  assert.ok(ev);
  assert.deepEqual([...new Set(ev!.sources!.map((s) => s.file))].sort(), ['late.jsonl', 'readings.jsonl']);

  // 另一只电表 M2 上的接电不会被 M1 候选的冲突波及
  const c2 = conn({
    connectionId: 'C2',
    meterId: 'M2',
    startBeijingMs: start + 2 * 3600_000,
    endBeijingMs: start + 6 * 3600_000,
  });
  const r2 = settleConnection(c2, readings);
  assert.equal(r2.quarantine?.reasons.includes('SOURCE_REVISION_CONFLICT'), false);
});

test('同修订冲突与其他结算原因合并到同一条隔离文档', () => {
  const c = conn({ revisionConflict: true });
  // 同时制造同一时刻累计值不一致
  const readings = [
    reading({ readingId: 'R1', sampledAtMs: start, cumulativeWh: 1000 }),
    reading({ readingId: 'R2', sampledAtMs: midnight, cumulativeWh: 5000 }),
    reading({ readingId: 'R4', sampledAtMs: midnight, cumulativeWh: 5050 }),
    reading({ readingId: 'R3', sampledAtMs: end, cumulativeWh: 8000 }),
  ];
  const r = settleConnection(c, readings);
  assert.ok(r.quarantine);
  assert.deepEqual([...new Set(r.quarantine!.reasons)].sort(), [
    'CONFLICTING_READING',
    'SOURCE_REVISION_CONFLICT',
  ]);
  // 仍然只有一条隔离
  assert.equal(r.quarantine!.docId, 'QUAR-C1');
});

test('非法窗口且接电同修订冲突：两类原因合并，不丢失冲突出处', () => {
  const c = conn({
    startBeijingMs: end,
    endBeijingMs: start,
    revisionConflict: true,
    sources: [src('C1', 1), { ...src('C1', 1), file: 'other.csv', line: 9 }],
  });
  const r = settleConnection(c, []);
  assert.deepEqual([...new Set(r.quarantine!.reasons)].sort(), [
    'INVALID_CONNECTION_WINDOW',
    'SOURCE_REVISION_CONFLICT',
  ]);
  assert.ok(r.quarantine!.evidence.some((e) => e.sources && e.sources.length === 2));
});
