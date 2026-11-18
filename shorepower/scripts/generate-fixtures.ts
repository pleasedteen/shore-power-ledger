/**
 * 双源夹具生成器。
 *
 * 生成三套夹具（全部由本项目自身生成，非外部下载数据）：
 *   fixtures/run1/        首次到达的两批文件（GB18030 CSV + UTF-8 JSONL）
 *   fixtures/run2/        补发/更正/乱序重送文件（文件名日期更早，模拟重送）
 *   fixtures/all-in-one/  合并后的完整输入，供单元测试直接使用
 *
 * 场景一览：
 *   CONN-101 正常跨两日；Wh/kWh 混用；run2 对起始读数做修订更正
 *   CONN-102 单日；同一时刻 kWh/Wh 等值读数（视为一致，不隔离）
 *   CONN-103 跨日但缺午夜读数 → 隔离 MISSING_MIDNIGHT_READING
 *   CONN-104 同一时刻读数不一致 → 隔离 CONFLICTING_READING
 *   CONN-105 累计电量回退 → 隔离 CUMULATIVE_REGRESSION
 *   CONN-106 run2 接电记录升修订、缩短窗口，旧日账被撤下替换
 *   CONN-107 run1 缺午夜读数被隔离；run2 补发后重跑恢复为日账
 */

import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { encodeGb18030 } from '../src/encoding.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const fixturesRoot = path.join(here, '..', 'fixtures');

interface CsvRow {
  connection_id: string;
  revision: number;
  vessel: string;
  meter_id: string;
  start_beijing: string;
  end_beijing: string;
}

interface JsonlRow {
  reading_id: string;
  revision: number;
  meter_id: string;
  sampled_at_utc: string;
  unit: 'Wh' | 'kWh';
  cumulative: string;
}

const csvHeader = 'connection_id,revision,vessel,meter_id,start_beijing,end_beijing';

function toCsv(rows: CsvRow[]): string {
  const lines = [csvHeader];
  for (const r of rows) {
    lines.push(
      [r.connection_id, r.revision, r.vessel, r.meter_id, r.start_beijing, r.end_beijing].join(','),
    );
  }
  return lines.join('\n') + '\n';
}

function toJsonl(rows: JsonlRow[]): string {
  return rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
}

/** 第一批接电记录（北京时间） */
const run1Connections: CsvRow[] = [
  { connection_id: 'CONN-101', revision: 1, vessel: '远洋之星', meter_id: 'M1', start_beijing: '2026-11-03 08:00:00', end_beijing: '2026-11-04 08:00:00' },
  { connection_id: 'CONN-102', revision: 1, vessel: '海风号', meter_id: 'M2', start_beijing: '2026-11-03 09:30:00', end_beijing: '2026-11-03 17:30:00' },
  { connection_id: 'CONN-103', revision: 1, vessel: '蓝鲸号', meter_id: 'M3', start_beijing: '2026-11-03 10:00:00', end_beijing: '2026-11-04 10:00:00' },
  { connection_id: 'CONN-104', revision: 1, vessel: '港通轮', meter_id: 'M4', start_beijing: '2026-11-03 12:00:00', end_beijing: '2026-11-04 06:00:00' },
  { connection_id: 'CONN-105', revision: 1, vessel: '北辰号', meter_id: 'M5', start_beijing: '2026-11-04 01:00:00', end_beijing: '2026-11-04 09:00:00' },
  { connection_id: 'CONN-106', revision: 1, vessel: '东海号', meter_id: 'M6', start_beijing: '2026-11-03 18:00:00', end_beijing: '2026-11-04 18:00:00' },
  { connection_id: 'CONN-107', revision: 1, vessel: '南极光', meter_id: 'M7', start_beijing: '2026-11-03 20:00:00', end_beijing: '2026-11-04 04:00:00' },
];

/** 第一批电表读数（UTC 采样时间，Wh/kWh 混排） */
const run1Readings: JsonlRow[] = [
  // M1：跨 11-03/11-04 两天，单位 Wh 与 kWh 混用；R101 为接电起点读数，run2 对其修订更正。
  // 北京 11-04 午夜 = UTC 11-03T16:00（R102）；接电终点北京 11-04 08:00 = UTC 11-04T00:00（R103）。
  { reading_id: 'R101', revision: 1, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '12000' },
  { reading_id: 'R102', revision: 1, meter_id: 'M1', sampled_at_utc: '2026-11-03T16:00:00Z', unit: 'kWh', cumulative: '28' },
  { reading_id: 'R103', revision: 1, meter_id: 'M1', sampled_at_utc: '2026-11-04T00:00:00Z', unit: 'Wh', cumulative: '34000' },
  // M2：单日；R202DUP 与 R202 同一时刻、不同单位但等值
  { reading_id: 'R201', revision: 1, meter_id: 'M2', sampled_at_utc: '2026-11-03T01:30:00Z', unit: 'Wh', cumulative: '500' },
  { reading_id: 'R202', revision: 1, meter_id: 'M2', sampled_at_utc: '2026-11-03T09:30:00Z', unit: 'Wh', cumulative: '10000' },
  { reading_id: 'R202DUP', revision: 1, meter_id: 'M2', sampled_at_utc: '2026-11-03T09:30:00Z', unit: 'kWh', cumulative: '10.0' },
  // M3：缺北京时间 11-04 午夜（UTC 11-03T16:00）读数
  { reading_id: 'R301', revision: 1, meter_id: 'M3', sampled_at_utc: '2026-11-03T02:00:00Z', unit: 'Wh', cumulative: '0' },
  { reading_id: 'R302', revision: 1, meter_id: 'M3', sampled_at_utc: '2026-11-04T02:00:00Z', unit: 'Wh', cumulative: '5000' },
  // M4：R403 与 R404 同一时刻累计值不一致
  { reading_id: 'R401', revision: 1, meter_id: 'M4', sampled_at_utc: '2026-11-03T04:00:00Z', unit: 'Wh', cumulative: '200' },
  { reading_id: 'R402', revision: 1, meter_id: 'M4', sampled_at_utc: '2026-11-03T16:00:00Z', unit: 'Wh', cumulative: '8000' },
  { reading_id: 'R403', revision: 1, meter_id: 'M4', sampled_at_utc: '2026-11-03T22:00:00Z', unit: 'Wh', cumulative: '15000' },
  { reading_id: 'R404', revision: 1, meter_id: 'M4', sampled_at_utc: '2026-11-03T22:00:00Z', unit: 'Wh', cumulative: '14999' },
  // M5：R503 小于前值，累计回退
  { reading_id: 'R501', revision: 1, meter_id: 'M5', sampled_at_utc: '2026-11-03T17:00:00Z', unit: 'Wh', cumulative: '3000' },
  { reading_id: 'R502', revision: 1, meter_id: 'M5', sampled_at_utc: '2026-11-03T20:00:00Z', unit: 'Wh', cumulative: '5500' },
  { reading_id: 'R503', revision: 1, meter_id: 'M5', sampled_at_utc: '2026-11-04T01:00:00Z', unit: 'Wh', cumulative: '5000' },
  // M6：rev1 接电结束于北京 11-04 18:00（UTC 10:00）
  { reading_id: 'R601', revision: 1, meter_id: 'M6', sampled_at_utc: '2026-11-03T10:00:00Z', unit: 'Wh', cumulative: '0' },
  { reading_id: 'R602', revision: 1, meter_id: 'M6', sampled_at_utc: '2026-11-03T16:00:00Z', unit: 'kWh', cumulative: '12' },
  { reading_id: 'R603', revision: 1, meter_id: 'M6', sampled_at_utc: '2026-11-04T10:00:00Z', unit: 'Wh', cumulative: '30000' },
  // M7：缺北京时间 11-04 午夜（UTC 11-03T16:00）读数，run2 补发
  { reading_id: 'R701', revision: 1, meter_id: 'M7', sampled_at_utc: '2026-11-03T12:00:00Z', unit: 'Wh', cumulative: '100' },
  { reading_id: 'R702', revision: 1, meter_id: 'M7', sampled_at_utc: '2026-11-03T20:00:00Z', unit: 'Wh', cumulative: '7600' },
];

/** run2：接电记录更正（CONN-106 升 rev2，缩短窗口） */
const run2Connections: CsvRow[] = [
  { connection_id: 'CONN-106', revision: 2, vessel: '东海号', meter_id: 'M6', start_beijing: '2026-11-03 18:00:00', end_beijing: '2026-11-04 12:00:00' },
];

/** run2：读数更正/补发（含一条乱序命名的字节级重送文件） */
const run2Readings: JsonlRow[] = [
  // R101 修订更正：12000 Wh -> 11900 Wh（起点读数下调，11-03 用量增加 100 Wh）
  { reading_id: 'R101', revision: 2, meter_id: 'M1', sampled_at_utc: '2026-11-03T00:00:00Z', unit: 'Wh', cumulative: '11900' },
  // M6 新窗口结束点：北京 11-04 12:00 = UTC 04:00
  { reading_id: 'R604', revision: 1, meter_id: 'M6', sampled_at_utc: '2026-11-04T04:00:00Z', unit: 'Wh', cumulative: '24000' },
  // M7 补发午夜读数
  { reading_id: 'R703', revision: 1, meter_id: 'M7', sampled_at_utc: '2026-11-03T16:00:00Z', unit: 'Wh', cumulative: '4000' },
];

async function writeCsv(file: string, rows: CsvRow[]): Promise<void> {
  await writeFile(file, encodeGb18030(toCsv(rows)));
}

async function writeJsonl(file: string, rows: JsonlRow[]): Promise<void> {
  await writeFile(file, toJsonl(rows), 'utf-8');
}

async function main(): Promise<void> {
  const run1Dir = path.join(fixturesRoot, 'run1');
  const run2Dir = path.join(fixturesRoot, 'run2');
  const allDir = path.join(fixturesRoot, 'all-in-one');
  for (const d of [
    path.join(run1Dir, 'connections'),
    path.join(run1Dir, 'readings'),
    path.join(run2Dir, 'connections'),
    path.join(run2Dir, 'readings'),
    path.join(allDir, 'connections'),
    path.join(allDir, 'readings'),
  ]) {
    await mkdir(d, { recursive: true });
  }

  // run1
  await writeCsv(path.join(run1Dir, 'connections', '2026-11-03_connections.csv'), run1Connections);
  const run1Jsonl = toJsonl(run1Readings);
  await writeFile(path.join(run1Dir, 'readings', '2026-11-03_readings.jsonl'), run1Jsonl, 'utf-8');

  // run2：文件名故意带更早日期，模拟乱序重送；重送文件与 run1 字节一致
  await writeCsv(path.join(run2Dir, 'connections', '2026-11-02_connections_correction.csv'), run2Connections);
  await writeJsonl(path.join(run2Dir, 'readings', '2026-11-02_readings_late.jsonl'), run2Readings);
  await writeFile(path.join(run2Dir, 'readings', '2026-11-01_readings_resend.jsonl'), run1Jsonl, 'utf-8');

  // all-in-one：run1 + run2 合并视图（重送文件也保留，验证哈希去重）
  await writeCsv(path.join(allDir, 'connections', '2026-11-03_connections.csv'), run1Connections);
  await writeCsv(path.join(allDir, 'connections', '2026-11-02_connections_correction.csv'), run2Connections);
  await writeFile(path.join(allDir, 'readings', '2026-11-03_readings.jsonl'), run1Jsonl, 'utf-8');
  await writeJsonl(path.join(allDir, 'readings', '2026-11-02_readings_late.jsonl'), run2Readings);
  await writeFile(path.join(allDir, 'readings', '2026-11-01_readings_resend.jsonl'), run1Jsonl, 'utf-8');

  console.log('夹具已生成：');
  console.log(`  ${path.relative(process.cwd(), run1Dir)}`);
  console.log(`  ${path.relative(process.cwd(), run2Dir)}`);
  console.log(`  ${path.relative(process.cwd(), allDir)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
