/**
 * 可撤销来源批次。
 *
 * 首次导入、补发、更正各自形成一个批次。批次台账与批次内的原始记录随发布一起
 * 落盘，因此：
 * - 发布成功后可以清走投递文件，批次内容仍完整保留；
 * - 撤销批次时按“剩余有效批次”重新选出每次接电的最高修订；
 * - 只重算真正受影响的接电，其余结果原样沿用；
 * - 撤销本身也是一次原子发布，批次状态与日账一起切换，中断后可重试。
 */

import { createHash } from 'node:crypto';
import type {
  BatchRecord,
  ConnectionRecord,
  InputFileInfo,
  MeterReading,
  SourceBatch,
  SourceBatchKind,
  SourceRef,
} from './types.js';

export function isActive(batch: SourceBatch): boolean {
  return batch.status === 'active';
}

/** 由批次种类与投递文件内容派生稳定批次 id，重送同样内容得到同一批次 */
export function deriveBatchId(kind: SourceBatchKind, files: InputFileInfo[]): string {
  const hash = createHash('sha256');
  hash.update(kind);
  for (const f of [...files].sort((a, b) => a.file.localeCompare(b.file))) {
    hash.update(`\0${f.file}\0${f.sha256}`);
  }
  return `B-${kind}-${hash.digest('hex').slice(0, 12)}`;
}

/** 生效批次的输入文件清单（去重、按路径排序） */
export function activeFiles(batches: SourceBatch[]): InputFileInfo[] {
  const map = new Map<string, InputFileInfo>();
  for (const b of batches) {
    if (!isActive(b)) continue;
    for (const f of b.files) {
      const key = `${f.file}#${f.sha256}`;
      if (!map.has(key)) map.set(key, { ...f });
    }
  }
  return [...map.values()].sort((a, b) => a.file.localeCompare(b.file));
}

function sourceKey(s: SourceRef): string {
  return `${s.file}#${s.line}#${s.recordId}#${s.revision}#${s.batchId ?? ''}`;
}

function mergeSources(...groups: SourceRef[][]): SourceRef[] {
  const map = new Map<string, SourceRef>();
  for (const group of groups) {
    for (const s of group) {
      const key = sourceKey(s);
      if (!map.has(key)) map.set(key, { ...s });
    }
  }
  return [...map.values()].sort((a, b) => (sourceKey(a) < sourceKey(b) ? -1 : sourceKey(a) > sourceKey(b) ? 1 : 0));
}

function sameConnectionContent(a: ConnectionRecord, b: ConnectionRecord): boolean {
  return (
    a.vessel === b.vessel &&
    a.meterId === b.meterId &&
    a.startBeijingMs === b.startBeijingMs &&
    a.endBeijingMs === b.endBeijingMs
  );
}

function sameReadingContent(a: MeterReading, b: MeterReading): boolean {
  return (
    a.meterId === b.meterId &&
    a.sampledAtMs === b.sampledAtMs &&
    Math.abs(a.cumulativeWh - b.cumulativeWh) <= 1e-6 &&
    a.originalUnit === b.originalUnit
  );
}

/**
 * 参与选取的记录范围。
 *
 * 撤销批次时只读“被撤批次涉及的主键版本”和“相关接电的电表”：
 * 接电按 connectionIds ∪ meterIds 纳入，读数按 meterIds 纳入，
 * 不遍历、也不重算无关数据。
 */
export interface SelectionScope {
  connectionIds?: Set<string>;
  meterIds?: Set<string>;
}

function connectionInScope(rec: ConnectionRecord, scope?: SelectionScope): boolean {
  if (!scope) return true;
  if (scope.connectionIds?.has(rec.connectionId)) return true;
  if (scope.meterIds?.has(rec.meterId)) return true;
  return false;
}

function readingInScope(rec: MeterReading, scope?: SelectionScope): boolean {
  if (!scope) return true;
  return scope.meterIds?.has(rec.meterId) ?? false;
}

export interface EffectiveSelection {
  connections: ConnectionRecord[];
  readings: MeterReading[];
  /** 同主键同修订内容冲突、需要整次隔离的接电 id */
  connectionConflictIds: Set<string>;
  warnings: string[];
}

/**
 * 在所有有效批次中按主键选取最高修订。
 * 同主键同修订且内容一致时合并出处；内容冲突时保留先到者并告警。
 */
export function selectEffective(
  batches: SourceBatch[],
  records: BatchRecord[],
  scope?: SelectionScope,
): EffectiveSelection {
  const active = new Set(batches.filter(isActive).map((b) => b.batchId));
  const connectionGroups = new Map<string, ConnectionRecord[]>();
  const readingGroups = new Map<string, MeterReading[]>();

  for (const br of records) {
    if (!active.has(br.batchId)) continue;
    if (br.kind === 'connection') {
      const rec = br.record as ConnectionRecord;
      if (!connectionInScope(rec, scope)) continue;
      const list = connectionGroups.get(rec.connectionId);
      if (list) list.push(rec);
      else connectionGroups.set(rec.connectionId, [rec]);
    } else {
      const rec = br.record as MeterReading;
      if (!readingInScope(rec, scope)) continue;
      const list = readingGroups.get(rec.readingId);
      if (list) list.push(rec);
      else readingGroups.set(rec.readingId, [rec]);
    }
  }

  const warnings: string[] = [];
  const connectionConflictIds = new Set<string>();
  const connections: ConnectionRecord[] = [];

  for (const [id, list] of connectionGroups) {
    const maxRevision = Math.max(...list.map((r) => r.revision));
    const top = list.filter((r) => r.revision === maxRevision);
    // 复制记录：选取过程会合并出处，绝不能修改随发布落盘的批次原始记录
    const first = { ...top[0]!, sources: mergeSources(top[0]!.sources) };
    let conflict = false;
    for (const other of top.slice(1)) {
      if (sameConnectionContent(first, other)) {
        first.sources = mergeSources(first.sources, other.sources);
      } else {
        conflict = true;
        warnings.push(
          `接电记录 ${id} rev=${maxRevision} 在 ${other.sources[0]?.file ?? '?'}:${other.sources[0]?.line ?? '?'} 与先前内容冲突，保留先到记录`,
        );
      }
    }
    if (conflict) connectionConflictIds.add(id);
    connections.push(first);
  }

  const readings: MeterReading[] = [];
  for (const list of readingGroups.values()) {
    const maxRevision = Math.max(...list.map((r) => r.revision));
    const top = list.filter((r) => r.revision === maxRevision);
    const first = { ...top[0]!, sources: mergeSources(top[0]!.sources) };
    for (const other of top.slice(1)) {
      if (sameReadingContent(first, other)) {
        first.sources = mergeSources(first.sources, other.sources);
      } else {
        warnings.push(
          `读数记录 ${first.readingId} rev=${maxRevision} 在 ${other.sources[0]?.file ?? '?'}:${other.sources[0]?.line ?? '?'} 与先前内容冲突，保留先到记录`,
        );
      }
    }
    readings.push(first);
  }

  connections.sort((a, b) => a.connectionId.localeCompare(b.connectionId));
  readings.sort((a, b) =>
    a.meterId === b.meterId ? a.sampledAtMs - b.sampledAtMs : a.meterId.localeCompare(b.meterId),
  );
  return { connections, readings, connectionConflictIds, warnings };
}

/** 规范化比较用形态（包含出处，保证血缘变化也能被识别） */
function canonical(value: unknown): string {
  const walk = (v: unknown): unknown => {
    if (Array.isArray(v)) return v.map(walk);
    if (v !== null && typeof v === 'object') {
      const out: Record<string, unknown> = {};
      for (const k of Object.keys(v as Record<string, unknown>).sort()) {
        const val = (v as Record<string, unknown>)[k];
        if (val !== undefined) out[k] = walk(val);
      }
      return out;
    }
    return v;
  };
  return JSON.stringify(walk(value));
}

/** 某次接电在给定读数集合中实际用到的读数（按其电表与窗口） */
export function readingsForConnection(
  conn: ConnectionRecord,
  readings: MeterReading[],
): MeterReading[] {
  return readings.filter(
    (r) =>
      r.meterId === conn.meterId &&
      r.sampledAtMs >= conn.startBeijingMs &&
      r.sampledAtMs <= conn.endBeijingMs,
  );
}

function sameSelectionEntry<T>(a: T | undefined, b: T | undefined): boolean {
  if (!a && !b) return true;
  if (!a || !b) return false;
  return canonical(a) === canonical(b);
}

/**
 * 计算撤销批次后真正受影响的接电：
 * - 该接电被选中的接电记录（含出处）发生变化；
 * - 或该接电窗口内的读数集合（含出处）发生变化；
 * - 或修订冲突状态发生变化。
 */
export function affectedConnectionIds(
  before: EffectiveSelection,
  after: EffectiveSelection,
  candidates: Set<string>,
): string[] {
  const beforeById = new Map(before.connections.map((c) => [c.connectionId, c]));
  const afterById = new Map(after.connections.map((c) => [c.connectionId, c]));
  const affected: string[] = [];

  for (const id of candidates) {
    const b = beforeById.get(id);
    const a = afterById.get(id);
    if (!sameSelectionEntry(b, a)) {
      affected.push(id);
      continue;
    }
    const conn = b ?? a!;
    const beforeReadings = readingsForConnection(conn, before.readings);
    const afterReadings = readingsForConnection(conn, after.readings);
    if (!sameSelectionEntry(beforeReadings, afterReadings)) {
      affected.push(id);
      continue;
    }
    if (before.connectionConflictIds.has(id) !== after.connectionConflictIds.has(id)) {
      affected.push(id);
    }
  }
  return affected.sort();
}

/**
 * 撤销某批次时只影响这些主键：
 * - 被撤批次里出现过的接电主键；
 * - 被撤批次里出现过的电表上、或这些接电所用电表上的全部接电。
 */
export function revokeScope(
  records: BatchRecord[],
  revokedBatchId: string,
): { connectionIds: Set<string>; meterIds: Set<string> } {
  const connectionIds = new Set<string>();
  const meterIds = new Set<string>();
  for (const r of records) {
    if (r.batchId !== revokedBatchId) continue;
    if (r.kind === 'connection') {
      const rec = r.record as ConnectionRecord;
      connectionIds.add(rec.connectionId);
      meterIds.add(rec.meterId);
    } else {
      meterIds.add((r.record as MeterReading).meterId);
    }
  }
  // 这些接电在其他批次里的其它修订可能用不同电表，也一并纳入
  for (const r of records) {
    if (r.kind !== 'connection') continue;
    const rec = r.record as ConnectionRecord;
    if (connectionIds.has(rec.connectionId)) meterIds.add(rec.meterId);
  }
  // 相关电表上的所有接电都可能受读数变化影响
  for (const r of records) {
    if (r.kind !== 'connection') continue;
    const rec = r.record as ConnectionRecord;
    if (meterIds.has(rec.meterId)) connectionIds.add(rec.connectionId);
  }
  return { connectionIds, meterIds };
}

/** 批次台账排序，保证发布指纹确定 */
export function sortBatches(batches: SourceBatch[]): SourceBatch[] {
  return [...batches].sort((a, b) => a.batchId.localeCompare(b.batchId));
}

/** 批次记录排序，保证发布指纹确定 */
export function sortBatchRecords(records: BatchRecord[]): BatchRecord[] {
  const recordId = (r: BatchRecord): string =>
    r.kind === 'connection'
      ? (r.record as ConnectionRecord).connectionId
      : (r.record as MeterReading).readingId;
  return [...records].sort((a, b) => {
    const byBatch = a.batchId.localeCompare(b.batchId);
    if (byBatch !== 0) return byBatch;
    const byKind = a.kind.localeCompare(b.kind);
    if (byKind !== 0) return byKind;
    return recordId(a).localeCompare(recordId(b));
  });
}

/** 新批次状态：active。创建时间由调用方提供，便于测试与重试确定 */
export function makeBatch(
  batchId: string,
  kind: SourceBatchKind,
  files: InputFileInfo[],
  nowMs: number,
): SourceBatch {
  return {
    batchId,
    kind,
    status: 'active',
    files: files.map((f) => ({ ...f, batchId })),
    createdAtMs: nowMs,
    updatedAtMs: nowMs,
  };
}
