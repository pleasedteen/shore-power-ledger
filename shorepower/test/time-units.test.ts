import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  BEIJING_OFFSET_MS,
  beijingDayOf,
  beijingMidnightMs,
  enumerateBeijingDays,
  formatBeijing,
  overlapsDay,
  parseBeijingLocal,
} from '../src/time.js';
import { energyEqual, roundKwh, toWh } from '../src/units.js';

test('北京时间固定 +08:00，不受运行环境时区影响', () => {
  // 北京 2026-11-03 00:00 == UTC 2026-11-02 16:00
  assert.equal(beijingMidnightMs('2026-11-03'), Date.UTC(2026, 10, 2, 16, 0, 0));
  assert.equal(beijingDayOf(Date.UTC(2026, 10, 2, 16, 0, 0)), '2026-11-03');
  assert.equal(beijingDayOf(Date.UTC(2026, 10, 2, 15, 59, 59)), '2026-11-02');
});

test('parseBeijingLocal 按 UTC+8 解释无时区时间串', () => {
  assert.equal(
    parseBeijingLocal('2026-11-03 08:00:00'),
    Date.UTC(2026, 10, 3, 0, 0, 0),
  );
  assert.equal(
    formatBeijing(Date.UTC(2026, 10, 3, 0, 0, 0)),
    '2026-11-03 08:00:00',
  );
});

test('自然日枚举与日重叠判断', () => {
  const start = parseBeijingLocal('2026-11-03 08:00:00');
  const end = parseBeijingLocal('2026-11-04 08:00:00');
  assert.deepEqual(enumerateBeijingDays(start, end), ['2026-11-03', '2026-11-04']);
  assert.equal(overlapsDay(start, end, '2026-11-03'), true);
  assert.equal(overlapsDay(start, end, '2026-11-05'), false);
  // 边界相接不算重叠
  assert.equal(
    overlapsDay(beijingMidnightMs('2026-11-04'), beijingMidnightMs('2026-11-04') + 1000, '2026-11-03'),
    false,
  );
  void BEIJING_OFFSET_MS;
});

test('单位换算与等值比较：不同单位的等值读数视为一致', () => {
  assert.equal(toWh(10, 'kWh'), 10000);
  assert.equal(toWh(1000, 'Wh'), 1000);
  assert.equal(energyEqual(toWh(10.0, 'kWh'), toWh(10000, 'Wh')), true);
  assert.equal(energyEqual(10000, 9999), false);
  // 浮点尾差在容差内
  assert.equal(energyEqual(1000, 1000 + 1e-9), true);
  assert.equal(roundKwh(12345.6789), 12.345679);
  assert.equal(roundKwh(1000), 1);
});
