/**
 * 数据接入层。
 *
 * 职责：
 * 1. 扫描输入目录，按扩展名识别 GB18030 CSV（.csv）与 UTF-8 JSONL（.jsonl）；
 * 2. 按文件 SHA-256 识别完全重复的重送文件；
 * 3. 解析记录并按业务主键合并：同一来源记录以最高 revision 为准；
 * 4. 同主键同修订但内容不一致时产生告警（由管线决定隔离）；
 * 5. 所有记录保留原始出处（文件、行号、修订），支撑血缘。
 */

import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { decodeGb18030, decodeUtf8 } from './encoding.js';
import { parseBeijingLocal } from './time.js';
import { toWh, type EnergyUnit } from './units.js';
import type {
  ConnectionRecord,
  InputFileInfo,
  MeterReading,
  SourceRef,
} from './types.js';

export interface IngestResult {
  connections: ConnectionRecord[];
  readings: MeterReading[];
  files: InputFileInfo[];
  warnings: string[];
}

function sha256(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

/** 极简 CSV 行解析：支持双引号包裹与 "" 转义，逗号分隔 */
function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

interface RawConnection {
  connectionId: string;
  revision: number;
  vessel: string;
  meterId: string;
  startBeijingMs: number;
  endBeijingMs: number;
  source: SourceRef;
}

interface RawReading {
  readingId: string;
  revision: number;
  meterId: string;
  sampledAtMs: number;
  cumulativeWh: number;
  originalUnit: EnergyUnit;
  originalValue: string;
  source: SourceRef;
}

function parseConnectionCsv(
  text: string,
  file: string,
  hash: string,
): RawConnection[] {
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return [];
  const header = parseCsvLine(lines[0]!);
  const idx = Object.fromEntries(header.map((h, i) => [h, i]));
  const required = [
    'connection_id',
    'revision',
    'vessel',
    'meter_id',
    'start_beijing',
    'end_beijing',
  ];
  for (const col of required) {
    if (!(col in idx)) {
      throw new Error(`CSV ${file} 缺少必需列: ${col}（实际表头: ${header.join(',')}）`);
    }
  }
  const rows: RawConnection[] = [];
  for (let li = 1; li < lines.length; li++) {
    const cols = parseCsvLine(lines[li]!);
    const source: SourceRef = {
      file,
      fileSha256: hash,
      line: li + 1,
      recordId: cols[idx.connection_id!]!,
      revision: Number(cols[idx.revision!]!),
    };
    rows.push({
      connectionId: cols[idx.connection_id!]!,
      revision: Number(cols[idx.revision!]!),
      vessel: cols[idx.vessel!]!,
      meterId: cols[idx.meter_id!]!,
      startBeijingMs: parseBeijingLocal(cols[idx.start_beijing!]!),
      endBeijingMs: parseBeijingLocal(cols[idx.end_beijing!]!),
      source,
    });
  }
  return rows;
}

function parseReadingJsonl(
  text: string,
  file: string,
  hash: string,
): RawReading[] {
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const rows: RawReading[] = [];
  lines.forEach((line, i) => {
    let obj: Record<string, unknown>;
    try {
      obj = JSON.parse(line) as Record<string, unknown>;
    } catch (err) {
      throw new Error(`${file}:${i + 1} 不是合法 JSON: ${(err as Error).message}`);
    }
    const readingId = String(obj.reading_id);
    const revision = Number(obj.revision);
    const meterId = String(obj.meter_id);
    const sampledAt = String(obj.sampled_at_utc);
    const sampledAtMs = Date.parse(sampledAt);
    if (Number.isNaN(sampledAtMs)) {
      throw new Error(`${file}:${i + 1} sampled_at_utc 无法解析: ${sampledAt}`);
    }
    const unit = String(obj.unit) === 'kWh' ? 'kWh' : 'Wh';
    const value = Number(obj.cumulative);
    if (!Number.isFinite(value)) {
      throw new Error(`${file}:${i + 1} cumulative 不是数值: ${String(obj.cumulative)}`);
    }
    const source: SourceRef = {
      file,
      fileSha256: hash,
      line: i + 1,
      recordId: readingId,
      revision,
    };
    rows.push({
      readingId,
      revision,
      meterId,
      sampledAtMs,
      cumulativeWh: toWh(value, unit),
      originalUnit: unit,
      originalValue: String(obj.cumulative),
      source,
    });
  });
  return rows;
}

/**
 * 合并同主键接电记录：只按最高 revision 判定，与文件到达顺序无关。
 * - 低修订一律不参与裁决，即使它们彼此冲突也不会触发隔离（已被高修订覆盖）；
 * - 最高修订组内内容一致（等值）的多份来源合并出处；
 * - 最高修订组内内容不一致：整笔标记 revisionConflict，保留各方出处，
 *   由结算层将整次接电隔离（SOURCE_REVISION_CONFLICT）。
 */
function mergeConnections(
  raws: RawConnection[],
  warnings: string[],
): ConnectionRecord[] {
  const groups = new Map<string, RawConnection[]>();
  for (const r of raws) {
    const list = groups.get(r.connectionId);
    if (list) list.push(r);
    else groups.set(r.connectionId, [r]);
  }

  const out: ConnectionRecord[] = [];
  for (const [connectionId, rows] of groups) {
    const maxRevision = rows.reduce((m, r) => Math.max(m, r.revision), -Infinity);
    const top = rows.filter((r) => r.revision === maxRevision);

    // 按业务内容聚类：内容一致的来源归为同一候选
    const buckets: RawConnection[][] = [];
    for (const r of top) {
      const bucket = buckets.find(
        (b) =>
          b[0]!.vessel === r.vessel &&
          b[0]!.meterId === r.meterId &&
          b[0]!.startBeijingMs === r.startBeijingMs &&
          b[0]!.endBeijingMs === r.endBeijingMs,
      );
      if (bucket) bucket.push(r);
      else buckets.push([r]);
    }

    const conflict = buckets.length > 1;
    if (conflict) {
      const origins = top
        .map((r) => `${r.source.file}:${r.source.line}`)
        .sort()
        .join('、');
      warnings.push(
        `接电记录 ${connectionId} rev=${maxRevision} 存在 ${buckets.length} 份内容冲突的同修订来源（${origins}），无法确定权威版本，整次接电隔离`,
      );
    }

    // 代表记录按（文件、行号）确定选取，保证任何文件顺序下结果一致
    const representative = [...top].sort((a, b) =>
      a.source.file === b.source.file
        ? a.source.line - b.source.line
        : a.source.file.localeCompare(b.source.file),
    )[0]!;
    out.push({
      connectionId,
      revision: maxRevision,
      vessel: representative.vessel,
      meterId: representative.meterId,
      startBeijingMs: representative.startBeijingMs,
      endBeijingMs: representative.endBeijingMs,
      revisionConflict: conflict || undefined,
      sources: top.map((r) => r.source),
    });
  }

  return out.sort((a, b) => a.connectionId.localeCompare(b.connectionId));
}

/**
 * 合并同主键读数记录：只按最高 revision 判定，与文件到达顺序无关。
 * - 低修订一律不参与裁决；
 * - 最高修订组内按（电表、采样时刻、累计电量）聚类：
 *   同位置等值的多份来源合并出处（含不同单位的等值读数）；
 *   出现不同位置/不同累计值即整笔标记 revisionConflict，
 *   每个候选位置各产出一条读数并都带上最高修订组的各方出处；
 * - 冲突读数只可能被引用其（meterId, 采样时刻）的接电命中，
 *   因此只影响命中候选位置的接电，而不会波及同表其他接电。
 */
function mergeReadings(raws: RawReading[], warnings: string[]): MeterReading[] {
  const groups = new Map<string, RawReading[]>();
  for (const r of raws) {
    const list = groups.get(r.readingId);
    if (list) list.push(r);
    else groups.set(r.readingId, [r]);
  }

  const out: MeterReading[] = [];
  for (const [readingId, rows] of groups) {
    const maxRevision = rows.reduce((m, r) => Math.max(m, r.revision), -Infinity);
    const top = rows.filter((r) => r.revision === maxRevision);

    // 按候选位置（电表 + 采样时刻 + 归一化累计值）聚类
    const buckets: RawReading[][] = [];
    for (const r of top) {
      const bucket = buckets.find(
        (b) =>
          b[0]!.meterId === r.meterId &&
          b[0]!.sampledAtMs === r.sampledAtMs &&
          Math.abs(b[0]!.cumulativeWh - r.cumulativeWh) <= 1e-6,
      );
      if (bucket) bucket.push(r);
      else buckets.push([r]);
    }

    const conflict = buckets.length > 1;
    if (conflict) {
      const origins = top
        .map((r) => `${r.source.file}:${r.source.line}`)
        .sort()
        .join('、');
      warnings.push(
        `读数记录 ${readingId} rev=${maxRevision} 存在 ${buckets.length} 份内容冲突的同修订来源（${origins}），无法确定权威版本，命中候选位置的接电整笔隔离`,
      );
    }

    // 每个候选位置一条读数；出处保留最高修订组的全部各方来源
    for (const bucket of buckets) {
      const representative = [...bucket].sort((a, b) =>
        a.source.file === b.source.file
          ? a.source.line - b.source.line
          : a.source.file.localeCompare(b.source.file),
      )[0]!;
      out.push({
        readingId,
        revision: maxRevision,
        meterId: representative.meterId,
        sampledAtMs: representative.sampledAtMs,
        cumulativeWh: representative.cumulativeWh,
        originalUnit: representative.originalUnit,
        originalValue: representative.originalValue,
        revisionConflict: conflict || undefined,
        sources: top.map((r) => r.source),
      });
    }
  }

  return out.sort((a, b) =>
    a.meterId === b.meterId
      ? a.sampledAtMs === b.sampledAtMs
        ? a.readingId.localeCompare(b.readingId)
        : a.sampledAtMs - b.sampledAtMs
      : a.meterId.localeCompare(b.meterId),
  );
}

/** 扫描输入目录并接入全部数据 */
export async function ingest(inputDir: string): Promise<IngestResult> {
  const entries = await readdir(inputDir, { withFileTypes: true, recursive: true });
  const files: InputFileInfo[] = [];
  const rawConnections: RawConnection[] = [];
  const rawReadings: RawReading[] = [];
  const warnings: string[] = [];
  const seenHash = new Map<string, string>();

  // 文件名排序保证处理顺序确定（乱序重送不影响结果）
  const candidates = entries
    .filter((e) => e.isFile())
    .map((e) => path.join(e.parentPath ?? inputDir, e.name))
    .filter((p) => /\.(csv|jsonl)$/i.test(p))
    .sort();

  for (const abs of candidates) {
    const rel = path.relative(inputDir, abs);
    const buf = await readFile(abs);
    const hash = sha256(buf);
    const dupOf = seenHash.get(hash);
    if (dupOf) {
      // 完全相同的重送文件：登记但跳过解析
      files.push({
        file: rel,
        encoding: /\.csv$/i.test(rel) ? 'GB18030' : 'UTF-8',
        sha256: hash,
        sizeBytes: buf.length,
        duplicateOf: dupOf,
      });
      continue;
    }
    seenHash.set(hash, rel);

    if (/\.csv$/i.test(rel)) {
      const text = decodeGb18030(buf);
      rawConnections.push(...parseConnectionCsv(text, rel, hash));
      files.push({ file: rel, encoding: 'GB18030', sha256: hash, sizeBytes: buf.length });
    } else {
      const text = decodeUtf8(buf);
      rawReadings.push(...parseReadingJsonl(text, rel, hash));
      files.push({ file: rel, encoding: 'UTF-8', sha256: hash, sizeBytes: buf.length });
    }
  }

  return {
    connections: mergeConnections(rawConnections, warnings),
    readings: mergeReadings(rawReadings, warnings),
    // 按相对路径排序，保证重跑时输入文件清单（进而发布指纹）确定
    files: files.sort((a, b) => a.file.localeCompare(b.file)),
    warnings,
  };
}
