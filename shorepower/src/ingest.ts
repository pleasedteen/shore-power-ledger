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
 * 合并同主键记录：最高 revision 为准。
 * 同 revision 重复出现时，内容一致则合并出处，不一致则保留先到者并告警。
 */
function mergeConnections(
  raws: RawConnection[],
  warnings: string[],
): ConnectionRecord[] {
  const map = new Map<string, ConnectionRecord>();
  for (const r of raws) {
    const exist = map.get(r.connectionId);
    if (!exist || r.revision > exist.revision) {
      map.set(r.connectionId, {
        connectionId: r.connectionId,
        revision: r.revision,
        vessel: r.vessel,
        meterId: r.meterId,
        startBeijingMs: r.startBeijingMs,
        endBeijingMs: r.endBeijingMs,
        sources: [r.source],
      });
    } else if (r.revision === exist.revision) {
      const same =
        exist.vessel === r.vessel &&
        exist.meterId === r.meterId &&
        exist.startBeijingMs === r.startBeijingMs &&
        exist.endBeijingMs === r.endBeijingMs;
      if (same) {
        exist.sources.push(r.source);
      } else {
        warnings.push(
          `接电记录 ${r.connectionId} rev=${r.revision} 在 ${r.source.file}:${r.source.line} 与先前内容冲突，保留先到记录`,
        );
      }
    }
  }
  return [...map.values()].sort((a, b) => a.connectionId.localeCompare(b.connectionId));
}

function mergeReadings(raws: RawReading[], warnings: string[]): MeterReading[] {
  const map = new Map<string, MeterReading>();
  for (const r of raws) {
    const exist = map.get(r.readingId);
    if (!exist || r.revision > exist.revision) {
      map.set(r.readingId, {
        readingId: r.readingId,
        revision: r.revision,
        meterId: r.meterId,
        sampledAtMs: r.sampledAtMs,
        cumulativeWh: r.cumulativeWh,
        originalUnit: r.originalUnit,
        originalValue: r.originalValue,
        sources: [r.source],
      });
    } else if (r.revision === exist.revision) {
      const same =
        exist.meterId === r.meterId &&
        exist.sampledAtMs === r.sampledAtMs &&
        Math.abs(exist.cumulativeWh - r.cumulativeWh) <= 1e-6;
      if (same) {
        exist.sources.push(r.source);
      } else {
        warnings.push(
          `读数记录 ${r.readingId} rev=${r.revision} 在 ${r.source.file}:${r.source.line} 与先前内容冲突，保留先到记录`,
        );
      }
    }
  }
  return [...map.values()].sort((a, b) =>
    a.meterId === b.meterId
      ? a.sampledAtMs - b.sampledAtMs
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
