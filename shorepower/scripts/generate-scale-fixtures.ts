/**
 * 90 天规模夹具生成器（仅用于本地规模验证，不提交大文件）。
 *
 * 生成约 90 天、每天多条接电的双源输入：
 * - connections/YYYY-MM-DD_connections.csv   每天一批接电（GB18030）
 * - readings/YYYY-MM-DD_readings.jsonl       对应电表读数（UTF-8，含每日午夜与小时采样）
 *
 * 场景：每天 3 条接电，其中跨日接电 CONN-D<day> 缴纳“跨日接电更正”的场景：
 * 场景 A（首次全量）与场景 B（更正后）由调用方分别生成，用于反复更正同一笔跨日接电。
 *
 * 用法：
 *   tsx scripts/generate-scale-fixtures.ts <输出目录> [--days 90] [--per-day 3] [--variant base|corrected]
 */

import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { encodeGb18030 } from '../src/encoding.js';
import { beijingMidnightMs, formatBeijing } from '../src/time.js';

interface Args {
  outDir: string;
  days: number;
  perDay: number;
  variant: 'base' | 'corrected';
  startDay: string;
  /** 目标跨日接电：并列出的接电 id（例如 CONN-2026-10-29-00） */
  targetConn: string;
  /** 目标接电的修订阶梯：1,2,3... 每次重跑只产生一份新输入 */
  targetRevision: number;
}

function parseArgs(argv: string[]): Args {
  const outDir = argv[0] ?? 'scale-input';
  const get = (name: string, fallback: string): string => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1]! : fallback;
  };
  const variant = get('--variant', 'base');
  if (variant !== 'base' && variant !== 'corrected') {
    throw new Error(`--variant 只能是 base 或 corrected，实际为 ${variant}`);
  }
  return {
    outDir,
    days: Number(get('--days', '90')),
    perDay: Number(get('--per-day', '3')),
    variant,
    startDay: get('--start', '2026-08-01'),
    targetConn: get('--target', ''),
    targetRevision: Number(get('--revision', variant === 'corrected' ? '2' : '1')),
  };
}

function addDays(day: string, n: number): string {
  const t = beijingMidnightMs(day) + n * 24 * 60 * 60 * 1000;
  const shifted = new Date(t + 8 * 60 * 60 * 1000);
  const p = (v: number) => String(v).padStart(2, '0');
  return `${shifted.getUTCFullYear()}-${p(shifted.getUTCMonth() + 1)}-${p(shifted.getUTCDate())}`;
}

/** 小时级读数：从 day 的接电起点前一小时到次日终点后一小时，每小时一条，累计电量单调递增 */
function hourlyRows(
  meterId: string,
  startMs: number,
  endMs: number,
  baseWh: number,
  perHourWh: number,
  idPrefix: string,
): Array<Record<string, string | number>> {
  const rows: Array<Record<string, string | number>> = [];
  let i = 0;
  for (let t = startMs - 3600_000; t <= endMs + 3600_000; t += 3600_000) {
    rows.push({
      reading_id: `${idPrefix}-${i}`,
      revision: 1,
      meter_id: meterId,
      sampled_at_utc: new Date(t).toISOString().replace('.000Z', 'Z'),
      unit: 'Wh',
      cumulative: String(baseWh + perHourWh * i),
    });
    i++;
  }
  return rows;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const connDir = path.join(args.outDir, 'connections');
  const readDir = path.join(args.outDir, 'readings');
  await mkdir(connDir, { recursive: true });
  await mkdir(readDir, { recursive: true });

  const csvHeader = 'connection_id,revision,vessel,meter_id,start_beijing,end_beijing';
  let connectionCount = 0;
  let readingCount = 0;

  for (let d = 0; d < args.days; d++) {
    const day = addDays(args.startDay, d);
    const next = addDays(day, 1);
    const dayStart = beijingMidnightMs(day);
    const csvLines: string[] = [csvHeader];
    const readings: Array<Record<string, string | number>> = [];

    for (let k = 0; k < args.perDay; k++) {
      const seq = String(d * args.perDay + k).padStart(5, '0');
      const meterId = `M-${seq}`;
      // 均匀铺开：每天 k 号接电从 02:00 + k*6h 起，时长 5~8 小时（k 为偶数时跨日）
      const startOffsetH = 2 + k * 6;
      const durationH = k % 2 === 0 ? 26 : 6; // 偶数号跨日
      const startMs = dayStart + startOffsetH * 3600_000;
      const endMs = startMs + durationH * 3600_000;
      const connId = `CONN-${day}-${String(k).padStart(2, '0')}`;

      // 目标跨日接电：把每天 0 号接电的窗口按修订阶梯逐次延长 1 小时，
      // 模拟“同一笔跨日接电反复更正”：每次重跑只替换相关日期的结果。
      const isTarget = k === 0 && (args.targetConn === '' || args.targetConn === connId);
      const shiftSteps = isTarget ? Math.max(0, args.targetRevision - 1) : 0;
      const effectiveStart = startMs - shiftSteps * 3600_000;
      const effectiveEnd = endMs + shiftSteps * 3600_000;
      const revision = isTarget ? Math.max(1, args.targetRevision) : 1;

      csvLines.push(
        [
          connId,
          revision,
          `测试船-${seq}`,
          meterId,
          formatBeijing(effectiveStart),
          formatBeijing(effectiveEnd),
        ].join(','),
      );
      readings.push(
        ...hourlyRows(
          meterId,
          effectiveStart,
          effectiveEnd,
          shiftSteps > 0 ? 0 : 1000,
          250,
          `R-${seq}-rev-${revision}`,
        ),
      );
      connectionCount++;
    }

    await writeFile(path.join(connDir, `${day}_connections.csv`), encodeGb18030(csvLines.join('\n') + '\n'));
    await writeFile(
      path.join(readDir, `${day}_readings.jsonl`),
      readings.map((r) => JSON.stringify(r)).join('\n') + '\n',
      'utf-8',
    );
    readingCount += readings.length;
    void next;
  }

  console.log(
    `规模夹具已生成：${args.outDir}（${args.days} 天，${connectionCount} 次接电，${readingCount} 条读数，variant=${args.variant}）`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
