/**
 * 规模夹具生成器（供 192 MiB 堆内存验收使用）。
 *
 * 生成：
 *   <out>/connections/*.csv   数千个接电（GB18030），分散在若干小文件里
 *   <out>/readings/*.jsonl    数千个读数文件（UTF-8），每个文件若干读数
 *
 * 每笔接电配一个独立电表，三个读数（接电起点 / 北京午夜 / 接电终点），
 * 全部走正常日账路径，避免共用时间轴导致的 O(接电 × 读数) 扫描。
 *
 * 用法：tsx scripts/generate-scale-fixtures.ts --out <目录> [--count 2000] [--files 2000]
 */

import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { encodeGb18030 } from '../src/encoding.js';
import { formatBeijing } from '../src/time.js';

const args = process.argv.slice(2);
function arg(name: string, fallback: string): string {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1]! : fallback;
}

const outDir = arg('--out', '');
const count = Number(arg('--count', '2000'));
const fileCount = Number(arg('--files', '2000'));

if (!outDir) {
  console.error('用法：tsx scripts/generate-scale-fixtures.ts --out <目录> [--count N] [--files N]');
  process.exit(1);
}

const csvHeader = 'connection_id,revision,vessel,meter_id,start_beijing,end_beijing';
const DAY_MS = 86_400_000;
/** 北京 2026-01-01 00:00 对应的绝对时刻 */
const BEIJING_ANCHOR_MS = Date.UTC(2026, 0, 1) - 8 * 3_600_000;

function pad(n: number, width: number): string {
  return String(n).padStart(width, '0');
}

/** 第 i 笔接电：在北京时间某日 hour 点开始，持续 24 小时（跨一个北京午夜） */
function connectionWindow(i: number): { startMs: number; endMs: number; midnightMs: number } {
  const dayOffset = i % 300;
  const hour = 8 + (i % 8);
  const dayStart = BEIJING_ANCHOR_MS + dayOffset * DAY_MS;
  const startMs = dayStart + hour * 3_600_000;
  const midnightMs = dayStart + DAY_MS;
  return { startMs, endMs: startMs + DAY_MS, midnightMs };
}

async function main(): Promise<void> {
  const connDir = path.join(outDir, 'connections');
  const readingDir = path.join(outDir, 'readings');
  await mkdir(connDir, { recursive: true });
  await mkdir(readingDir, { recursive: true });

  // 接电与读数各占约一半文件，合计约 fileCount 个小文件
  const connFileCount = Math.max(1, Math.floor(fileCount / 2));
  const readingFileCount = Math.max(1, fileCount - connFileCount);
  const connPerFile = Math.ceil(count / connFileCount);
  const readingPerFile = Math.ceil(count / readingFileCount);

  for (let f = 0; f < connFileCount; f++) {
    const rows: string[] = [csvHeader];
    for (let i = f * connPerFile; i < Math.min(count, (f + 1) * connPerFile); i++) {
      const { startMs, endMs } = connectionWindow(i);
      rows.push(
        [
          `CONN-${pad(i, 6)}`,
          1,
          `船舶-${i}`,
          `M-${pad(i, 6)}`,
          formatBeijing(startMs),
          formatBeijing(endMs),
        ].join(','),
      );
    }
    await writeFile(path.join(connDir, `${pad(f, 5)}.csv`), encodeGb18030(rows.join('\n') + '\n'));
  }

  for (let f = 0; f < readingFileCount; f++) {
    const lines: string[] = [];
    for (let i = f * readingPerFile; i < Math.min(count, (f + 1) * readingPerFile); i++) {
      const { startMs, endMs, midnightMs } = connectionWindow(i);
      const meter = `M-${pad(i, 6)}`;
      const mk = (id: string, at: number, value: number) =>
        JSON.stringify({
          reading_id: id,
          revision: 1,
          meter_id: meter,
          sampled_at_utc: new Date(at).toISOString().replace('.000Z', 'Z'),
          unit: 'Wh',
          cumulative: String(value),
        });
      lines.push(mk(`R-${pad(i, 6)}-S`, startMs, 1000));
      lines.push(mk(`R-${pad(i, 6)}-M`, midnightMs, 6000));
      lines.push(mk(`R-${pad(i, 6)}-E`, endMs, 11000));
    }
    await writeFile(path.join(readingDir, `${pad(f, 5)}.jsonl`), lines.join('\n') + '\n', 'utf-8');
  }

  console.log(
    `规模夹具已生成：${count} 笔接电，${connFileCount} 个 CSV + ${readingFileCount} 个 JSONL = ${connFileCount + readingFileCount} 个文件`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
