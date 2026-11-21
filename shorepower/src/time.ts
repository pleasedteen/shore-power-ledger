/**
 * 北京时间（UTC+8）工具。
 *
 * 业务上“北京时间自然日”按固定 +08:00 偏移计算，不依赖运行机器的本地时区，
 * 保证在任意容器/CI 环境下结果一致。
 */

export const BEIJING_OFFSET_MS = 8 * 60 * 60 * 1000;

/** 绝对时间 ms -> 北京时间自然日字符串 YYYY-MM-DD */
export function beijingDayOf(epochMs: number): string {
  const shifted = new Date(epochMs + BEIJING_OFFSET_MS);
  const y = shifted.getUTCFullYear();
  const m = shifted.getUTCMonth() + 1;
  const d = shifted.getUTCDate();
  return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}

/** 北京时间自然日 00:00（UTC+8）对应的绝对时间 ms */
export function beijingMidnightMs(day: string): number {
  const [y, m, d] = day.split('-').map((v) => Number(v));
  return Date.UTC(y!, m! - 1, d!) - BEIJING_OFFSET_MS;
}

/**
 * 解析“北京时间本地时间”字符串（如 2026-11-03 08:00:00）为绝对时间 ms。
 * 输入没有时区标记，按 UTC+8 解释。
 */
export function parseBeijingLocal(s: string): number {
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
  if (!m) {
    throw new Error(`无法解析的北京时间字符串: ${s}`);
  }
  const [, y, mo, d, h, mi, se] = m;
  return Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(se)) -
    BEIJING_OFFSET_MS;
}

/** 绝对时间 ms -> 北京时间展示字符串 */
export function formatBeijing(epochMs: number): string {
  const shifted = new Date(epochMs + BEIJING_OFFSET_MS);
  const p = (n: number) => String(n).padStart(2, '0');
  return (
    `${shifted.getUTCFullYear()}-${p(shifted.getUTCMonth() + 1)}-${p(shifted.getUTCDate())} ` +
    `${p(shifted.getUTCHours())}:${p(shifted.getUTCMinutes())}:${p(shifted.getUTCSeconds())}`
  );
}

/** 绝对时间 ms -> UTC 展示字符串（读数时间用） */
export function formatUtc(epochMs: number): string {
  const d = new Date(epochMs);
  const p = (n: number) => String(n).padStart(2, '0');
  return (
    `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ` +
    `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())} UTC`
  );
}

/** 枚举区间覆盖到的全部北京时间自然日（含首尾） */
export function enumerateBeijingDays(startMs: number, endMs: number): string[] {
  const first = beijingDayOf(startMs);
  const last = beijingDayOf(endMs);
  const days: string[] = [first];
  // 下一个北京时间自然日的午夜：首日午夜 + 24h
  let cursor = beijingMidnightMs(first) + 24 * 60 * 60 * 1000;
  const stop = beijingMidnightMs(last);
  while (cursor <= stop) {
    days.push(beijingDayOf(cursor));
    cursor += 24 * 60 * 60 * 1000;
  }
  return days;
}

/** 判断接电区间与某个北京时间自然日是否重叠 */
export function overlapsDay(startMs: number, endMs: number, day: string): boolean {
  const dayStart = beijingMidnightMs(day);
  const dayEnd = dayStart + 24 * 60 * 60 * 1000;
  return startMs < dayEnd && endMs > dayStart;
}

/** 判断接电区间与北京时间日期范围 [startDay, endDay]（含首尾）是否重叠 */
export function overlapsDayRange(
  startMs: number,
  endMs: number,
  startDay: string,
  endDay: string,
): boolean {
  const rangeStart = beijingMidnightMs(startDay);
  // 范围末日的次日午夜（即范围结束的开区间边界）
  const rangeEnd = beijingMidnightMs(endDay) + 24 * 60 * 60 * 1000;
  return startMs < rangeEnd && endMs > rangeStart;
}

/** 校验北京时间日期串 YYYY-MM-DD 且起不晚于止 */
export function assertValidDayRange(startDay: string, endDay: string): void {
  const re = /^\d{4}-\d{2}-\d{2}$/;
  if (!re.test(startDay) || !re.test(endDay)) {
    throw new Error('日期必须为 YYYY-MM-DD（北京时间自然日）');
  }
  if (Number.isNaN(beijingMidnightMs(startDay)) || Number.isNaN(beijingMidnightMs(endDay))) {
    throw new Error('日期无法解析为北京时间自然日');
  }
  if (startDay > endDay) {
    throw new Error(`日期范围非法：起始日 ${startDay} 晚于结束日 ${endDay}`);
  }
}
