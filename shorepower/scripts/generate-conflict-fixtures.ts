/**
 * 乱序双源修订冲突夹具生成器。
 *
 * 生成两阶段输入（全部由本项目自身生成，非外部下载数据）：
 *   fixtures/conflict/stage-a/  双源乱序到达：同主键同修订读数内容冲突
 *   fixtures/conflict/stage-b/  追加权威补发文件解除冲突；旧原件移走、候选换位
 *
 * 文件名日期故意与实际批次相反（2026-11-05 的文件按名字排在 2026-11-09 之前），
 * 配合递归按路径排序的扫描顺序，模拟“乱序双源”，验证合并结果与文件顺序无关。
 *
 * 场景一览（北京时间）：
 *   CONN-201 / M8  跨 11-09/11-10；R804 同修订两源累计值不一致 → 整笔隔离，
 *                  stage-b 补发等值权威读数后，重跑 11-09 解隔为两条日账
 *   CONN-202 / M9  11-09 单日，全程不参与任何冲突与重跑日 → 始终沿用
 *   CONN-203 / M10 stage-a 窗口在 11-10；stage-b 旧读数移走、候选整体换位到 11-12，
 *                  接电窗口同步移到 11-12：重跑 11-10（任一候选实际涉及的旧日）
 *                  仍能撤换整笔旧结果；重跑空档日 11-11 不纳入、结果沿用
 */

import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { encodeGb18030 } from '../src/encoding.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, '..', 'fixtures', 'conflict');

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
  return [csvHeader, ...rows.map((r) => [r.connection_id, r.revision, r.vessel, r.meter_id, r.start_beijing, r.end_beijing].join(','))].join('\n') + '\n';
}

function toJsonl(rows: JsonlRow[]): string {
  return rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
}

async function writeCsv(file: string, rows: CsvRow[]): Promise<void> {
  await writeFile(file, encodeGb18030(toCsv(rows)));
}

async function writeJsonl(file: string, rows: JsonlRow[]): Promise<void> {
  await writeFile(file, toJsonl(rows), 'utf-8');
}

const stageAConnections: CsvRow[] = [
  { connection_id: 'CONN-201', revision: 1, vessel: '远望号', meter_id: 'M8', start_beijing: '2026-11-09 08:00:00', end_beijing: '2026-11-10 08:00:00' },
  { connection_id: 'CONN-202', revision: 1, vessel: '西邻轮', meter_id: 'M9', start_beijing: '2026-11-09 09:00:00', end_beijing: '2026-11-09 17:00:00' },
  { connection_id: 'CONN-203', revision: 1, vessel: '北辰号', meter_id: 'M10', start_beijing: '2026-11-10 08:00:00', end_beijing: '2026-11-10 18:00:00' },
];

/**
 * 乱序双源读数：
 * - 2026-11-09_readings_batch2.jsonl 名字排序在前，先被扫描（“后发先至”）；
 * - 2026-11-05_readings_batch1.jsonl 名字日期更早、排序在后；
 * 两源对 R804 给出同修订但不一致的累计值（15000 vs 15500 Wh）。
 */
const stageABatch2Readings: JsonlRow[] = [
  // M8：接电起点（北京 11-09 08:00 = UTC 11-09T00:00）
  { reading_id: 'R801', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-09T00:00:00Z', unit: 'Wh', cumulative: '1000' },
  // M8：北京 11-10 午夜 = UTC 11-09T16:00（kWh 等值写法）
  { reading_id: 'R802', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-09T16:00:00Z', unit: 'kWh', cumulative: '20' },
  // M8：接电终点（北京 11-10 08:00 = UTC 11-10T00:00）
  { reading_id: 'R803', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-10T00:00:00Z', unit: 'Wh', cumulative: '26000' },
  // M8 区间内部点 R804：batch2 说法 15000 Wh
  { reading_id: 'R804', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-09T04:00:00Z', unit: 'Wh', cumulative: '15000' },
  // M9：CONN-202 单日起止
  { reading_id: 'R901', revision: 1, meter_id: 'M9', sampled_at_utc: '2026-11-09T01:00:00Z', unit: 'Wh', cumulative: '200' },
  { reading_id: 'R902', revision: 1, meter_id: 'M9', sampled_at_utc: '2026-11-09T09:00:00Z', unit: 'Wh', cumulative: '8200' },
  // M10：CONN-203（11-10 单日）起止
  { reading_id: 'R1001', revision: 1, meter_id: 'M10', sampled_at_utc: '2026-11-10T00:00:00Z', unit: 'Wh', cumulative: '0' },
  { reading_id: 'R1002', revision: 1, meter_id: 'M10', sampled_at_utc: '2026-11-10T10:00:00Z', unit: 'Wh', cumulative: '6000' },
];

const stageABatch1Readings: JsonlRow[] = [
  // 与 batch2 等值的重送（不同单位/同值），应合并出处而非冲突
  { reading_id: 'R801', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-09T00:00:00Z', unit: 'kWh', cumulative: '1.0' },
  { reading_id: 'R802', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-09T16:00:00Z', unit: 'Wh', cumulative: '20000' },
  { reading_id: 'R803', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-10T00:00:00Z', unit: 'Wh', cumulative: '26000' },
  // M8 区间内部点 R804：batch1 说法 15500 Wh —— 与 batch2 同修订冲突
  { reading_id: 'R804', revision: 1, meter_id: 'M8', sampled_at_utc: '2026-11-09T04:00:00Z', unit: 'Wh', cumulative: '15500' },
  { reading_id: 'R901', revision: 1, meter_id: 'M9', sampled_at_utc: '2026-11-09T01:00:00Z', unit: 'Wh', cumulative: '200' },
  { reading_id: 'R902', revision: 1, meter_id: 'M9', sampled_at_utc: '2026-11-09T09:00:00Z', unit: 'Wh', cumulative: '8200' },
  { reading_id: 'R1001', revision: 1, meter_id: 'M10', sampled_at_utc: '2026-11-10T00:00:00Z', unit: 'Wh', cumulative: '0' },
  { reading_id: 'R1002', revision: 1, meter_id: 'M10', sampled_at_utc: '2026-11-10T10:00:00Z', unit: 'Wh', cumulative: '6000' },
];

/**
 * stage-b：在 stage-a 全部文件保留的基础上追加/替换。
 * - R804 权威补发（新文件，rev2 单一来源）→ 同修订冲突被高修订覆盖，解隔；
 * - CONN-203 升 rev2 移到 11-12 窗口，旧 M10 读数 R1001/R1002 从输入移走，
 *   新候选 R1003/R1004 落在 11-12（与旧日 11-10 之间隔着空档 11-11）。
 */
const stageBConnections: CsvRow[] = [
  { connection_id: 'CONN-203', revision: 2, vessel: '北辰号', meter_id: 'M10', start_beijing: '2026-11-12 08:00:00', end_beijing: '2026-11-12 18:00:00' },
];

const stageBResolutionReadings: JsonlRow[] = [
  // R804 rev2：唯一权威值，两源旧 rev1 冲突不再参与裁决
  { reading_id: 'R804', revision: 2, meter_id: 'M8', sampled_at_utc: '2026-11-09T04:00:00Z', unit: 'Wh', cumulative: '15200' },
  // M10 新候选（11-12 单日，北京 08:00 = UTC 00:00；18:00 = UTC 10:00）
  { reading_id: 'R1003', revision: 1, meter_id: 'M10', sampled_at_utc: '2026-11-12T00:00:00Z', unit: 'Wh', cumulative: '7000' },
  { reading_id: 'R1004', revision: 1, meter_id: 'M10', sampled_at_utc: '2026-11-12T10:00:00Z', unit: 'Wh', cumulative: '14000' },
];

async function main(): Promise<void> {
  const dirs = [
    path.join(root, 'stage-a', 'connections'),
    path.join(root, 'stage-a', 'readings'),
    path.join(root, 'stage-b', 'connections'),
    path.join(root, 'stage-b', 'readings'),
  ];
  for (const d of dirs) await mkdir(d, { recursive: true });

  // stage-a
  await writeCsv(path.join(root, 'stage-a', 'connections', '2026-11-09_connections.csv'), stageAConnections);
  // 文件名乱序：batch2（实际后到的数据）命名日期更晚、排序反而在前
  await writeJsonl(path.join(root, 'stage-a', 'readings', '2026-11-09_readings_batch2.jsonl'), stageABatch2Readings);
  await writeJsonl(path.join(root, 'stage-a', 'readings', '2026-11-05_readings_batch1.jsonl'), stageABatch1Readings);

  // stage-b：继承 stage-a 的双源文件，再追加更正
  await writeCsv(path.join(root, 'stage-b', 'connections', '2026-11-09_connections.csv'), stageAConnections);
  await writeCsv(path.join(root, 'stage-b', 'connections', '2026-11-11_connections_correction.csv'), stageBConnections);
  await writeJsonl(path.join(root, 'stage-b', 'readings', '2026-11-09_readings_batch2.jsonl'), stageABatch2Readings);
  await writeJsonl(path.join(root, 'stage-b', 'readings', '2026-11-05_readings_batch1.jsonl'), stageABatch1Readings);
  await writeJsonl(path.join(root, 'stage-b', 'readings', '2026-11-11_readings_resolution.jsonl'), stageBResolutionReadings);

  console.log('乱序双源修订冲突夹具已生成：');
  console.log(`  ${path.relative(process.cwd(), root)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
