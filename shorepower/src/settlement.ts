/**
 * 结算引擎。
 *
 * 对每次接电：
 * 1. 校验接电起止窗口；
 * 2. 取该电表在接电区间内（含端点）的全部读数，同一时刻读数冲突或累计电量回退即整次隔离；
 * 3. 检查接电起点、终点与区间内每个北京时间午夜是否都有读数，缺失即整次隔离；
 * 4. 通过校验后按北京时间自然日拆分日账，以起止/午夜累计读数之差结算各日用量。
 *
 * 不同单位换算后的等值读数（容差内）视为一致，不构成冲突。
 */

import { beijingMidnightMs, enumerateBeijingDays } from './time.js';
import { energyEqual, roundKwh } from './units.js';
import type {
  BoundaryPoint,
  ConnectionRecord,
  DailyAccount,
  MeterReading,
  QuarantineEntry,
  QuarantineReason,
  SourceRef,
} from './types.js';

export interface ConnectionSettlement {
  connection: ConnectionRecord;
  accounts: Omit<DailyAccount, 'publicationId'>[];
  quarantine?: Omit<QuarantineEntry, 'publicationId'>;
  /** 本接电区间内参与判断的全部读数出处（供血缘使用） */
  readingSources: SourceRef[];
}

interface PointAtTime {
  sampledAtMs: number;
  cumulativeWh: number;
  readings: MeterReading[];
}

/** 同一时刻的多条读数归并；累计值不一致时通过 hasConflict 报告 */
function groupByTimestamp(
  readings: MeterReading[],
): {
  points: PointAtTime[];
  hasConflict: boolean;
  conflictEvidence: QuarantineEntry['evidence'];
} {
  const map = new Map<number, PointAtTime>();
  let hasConflict = false;
  const conflictEvidence: QuarantineEntry['evidence'] = [];

  for (const r of readings) {
    const exist = map.get(r.sampledAtMs);
    if (!exist) {
      map.set(r.sampledAtMs, {
        sampledAtMs: r.sampledAtMs,
        cumulativeWh: r.cumulativeWh,
        readings: [r],
      });
      continue;
    }
    exist.readings.push(r);
    if (!energyEqual(exist.cumulativeWh, r.cumulativeWh)) {
      hasConflict = true;
      conflictEvidence.push({
        sampledAtMs: r.sampledAtMs,
        note: `同一采样时刻存在不一致累计读数：${exist.cumulativeWh} Wh vs ${r.cumulativeWh} Wh`,
        // 保留该时刻全部各方读数的出处，而非只留触发者
        sources: [...exist.readings.map((x) => x.sources[0]!), r.sources[0]!],
      });
    }
  }

  const points = [...map.values()].sort((a, b) => a.sampledAtMs - b.sampledAtMs);
  return { points, hasConflict, conflictEvidence };
}

export function settleConnection(
  conn: ConnectionRecord,
  allReadings: MeterReading[],
): ConnectionSettlement {
  const reasons: QuarantineReason[] = [];
  const messages: string[] = [];
  const evidence: QuarantineEntry['evidence'] = [];

  /** 接电自身的同修订冲突：整笔隔离，证据保留各方出处 */
  const noteConnectionConflict = (): void => {
    if (!conn.revisionConflict || reasons.includes('SOURCE_REVISION_CONFLICT')) return;
    reasons.push('SOURCE_REVISION_CONFLICT');
    messages.push('同一接电记录的相同修订版本存在内容冲突，无法确定权威版本');
    evidence.push({
      note: `接电记录 ${conn.connectionId} rev=${conn.revision} 同修订内容冲突，保留各方出处`,
      sources: [...conn.sources],
    });
  };

  // 1. 窗口合法性（早退，但同修订冲突原因仍要随整笔隔离一并保留）
  if (!(conn.endBeijingMs > conn.startBeijingMs)) {
    noteConnectionConflict();
    const allReasons = [...new Set<QuarantineReason>([
      'INVALID_CONNECTION_WINDOW',
      ...reasons,
    ])];
    return {
      connection: conn,
      accounts: [],
      readingSources: [],
      quarantine: {
        docType: 'quarantine',
        docId: `QUAR-${conn.connectionId}`,
        connectionId: conn.connectionId,
        revision: conn.revision,
        vessel: conn.vessel,
        meterId: conn.meterId,
        startBeijingMs: conn.startBeijingMs,
        endBeijingMs: conn.endBeijingMs,
        reasons: allReasons,
        messages: [
          `接电起止时间非法：结束时间(${conn.endBeijingMs})必须晚于起始时间(${conn.startBeijingMs})`,
          ...messages,
        ],
        evidence,
      },
    };
  }

  noteConnectionConflict();

  const quarantine = (): ConnectionSettlement => ({
    connection: conn,
    accounts: [],
    readingSources,
    quarantine: {
      docType: 'quarantine',
      docId: `QUAR-${conn.connectionId}`,
      connectionId: conn.connectionId,
      revision: conn.revision,
      vessel: conn.vessel,
      meterId: conn.meterId,
      startBeijingMs: conn.startBeijingMs,
      endBeijingMs: conn.endBeijingMs,
      reasons: [...new Set(reasons)],
      messages,
      evidence,
    },
  });

  // 2. 取本电表在接电区间内（含端点）的读数
  const inWindow = allReadings
    .filter(
      (r) =>
        r.meterId === conn.meterId &&
        r.sampledAtMs >= conn.startBeijingMs &&
        r.sampledAtMs <= conn.endBeijingMs,
    )
    .sort((a, b) => a.sampledAtMs - b.sampledAtMs);

  const readingSources: SourceRef[] = inWindow.flatMap((r) => r.sources);

  const { points, hasConflict, conflictEvidence } = groupByTimestamp(inWindow);

  if (hasConflict) {
    reasons.push('CONFLICTING_READING');
    messages.push('接电期间同一采样时刻存在互相冲突的累计读数');
    evidence.push(...conflictEvidence);
  }

  // 2.1 读数自身的同修订冲突：只有候选位置（电表+采样时刻）落在本接电窗口内才会命中，
  //     因此只影响命中候选位置的接电；整笔隔离，证据保留各方出处。
  const conflictingReadings = inWindow.filter((r) => r.revisionConflict);
  if (conflictingReadings.length > 0) {
    reasons.push('SOURCE_REVISION_CONFLICT');
    messages.push('接电期间存在相同修订版本内容冲突的电表读数，无法确定权威版本');
    for (const r of conflictingReadings) {
      evidence.push({
        sampledAtMs: r.sampledAtMs,
        cumulativeWh: r.cumulativeWh,
        note: `读数记录 ${r.readingId} rev=${r.revision} 同修订内容冲突，候选位置之一落在本接电窗口，保留各方出处`,
        sources: [...r.sources],
      });
    }
  }

  // 3. 累计电量回退（按时刻升序必须单调不减）
  for (let i = 1; i < points.length; i++) {
    const prev = points[i - 1]!;
    const cur = points[i]!;
    if (cur.cumulativeWh + 1e-6 < prev.cumulativeWh) {
      reasons.push('CUMULATIVE_REGRESSION');
      messages.push(
        `累计电量回退：${new Date(prev.sampledAtMs).toISOString()} = ${prev.cumulativeWh} Wh，` +
          `${new Date(cur.sampledAtMs).toISOString()} = ${cur.cumulativeWh} Wh`,
      );
      evidence.push({
        sampledAtMs: cur.sampledAtMs,
        cumulativeWh: cur.cumulativeWh,
        note: '该读数小于前一时刻累计读数',
        source: cur.readings[0]!.sources[0],
      });
    }
  }

  // 4. 必需边界：起点、终点、区间内严格位于两端之间的北京时间午夜
  const pointIndex = new Map<number, PointAtTime>();
  for (const p of points) pointIndex.set(p.sampledAtMs, p);

  const requiredBoundaries: Array<{
    kind: BoundaryPoint['kind'];
    at: number;
    day?: string;
  }> = [{ kind: 'CONNECTION_START', at: conn.startBeijingMs }];

  const days = enumerateBeijingDays(conn.startBeijingMs, conn.endBeijingMs);
  for (const day of days) {
    const midnight = beijingMidnightMs(day) + 24 * 60 * 60 * 1000;
    if (midnight > conn.startBeijingMs && midnight < conn.endBeijingMs) {
      requiredBoundaries.push({ kind: 'BEIJING_MIDNIGHT', at: midnight, day });
    }
  }
  requiredBoundaries.push({ kind: 'CONNECTION_END', at: conn.endBeijingMs });

  const resolvedBoundaries: BoundaryPoint[] = [];
  for (const b of requiredBoundaries) {
    const p = pointIndex.get(b.at);
    if (!p) {
      const reason: QuarantineReason =
        b.kind === 'CONNECTION_START'
          ? 'MISSING_START_READING'
          : b.kind === 'CONNECTION_END'
            ? 'MISSING_END_READING'
            : 'MISSING_MIDNIGHT_READING';
      reasons.push(reason);
      messages.push(
        b.kind === 'BEIJING_MIDNIGHT'
          ? `缺少北京时间 ${b.day} 午夜（${new Date(b.at).toISOString()}）的电表读数`
          : `缺少接电${b.kind === 'CONNECTION_START' ? '起始' : '结束'}时刻 ${new Date(b.at).toISOString()} 的电表读数`,
      );
      evidence.push({ sampledAtMs: b.at, note: `${b.kind} 边界读数缺失` });
    } else {
      resolvedBoundaries.push({
        kind: b.kind,
        day: b.day,
        sampledAtMs: b.at,
        cumulativeWh: p.cumulativeWh,
        source: p.readings[0]!.sources[0]!,
      });
    }
  }

  if (reasons.length > 0) {
    return quarantine();
  }

  // 5. 按北京自然日拆账
  const accounts: Omit<DailyAccount, 'publicationId'>[] = [];
  const boundaryAt = (at: number): BoundaryPoint =>
    resolvedBoundaries.find((b) => b.sampledAtMs === at)!;

  for (const day of days) {
    const dayStart = beijingMidnightMs(day);
    const dayEnd = dayStart + 24 * 60 * 60 * 1000;
    const windowStart = Math.max(conn.startBeijingMs, dayStart);
    const windowEnd = Math.min(conn.endBeijingMs, dayEnd);
    const startPoint = boundaryAt(windowStart);
    const endPoint = boundaryAt(windowEnd);
    const usageWh = endPoint.cumulativeWh - startPoint.cumulativeWh;

    const readingsUsed = points
      .filter((p) => p.sampledAtMs >= windowStart && p.sampledAtMs <= windowEnd)
      .flatMap((p) =>
        p.readings.map((r) => ({
          sampledAtMs: p.sampledAtMs,
          cumulativeWh: p.cumulativeWh,
          source: r.sources[0]!,
        })),
      );

    accounts.push({
      docType: 'daily-account',
      docId: `DAILY-${conn.connectionId}-${day}`,
      connectionId: conn.connectionId,
      revision: conn.revision,
      vessel: conn.vessel,
      meterId: conn.meterId,
      beijingDay: day,
      windowStartMs: windowStart,
      windowEndMs: windowEnd,
      usageWh,
      usageKwh: roundKwh(usageWh),
      startBoundary: startPoint,
      endBoundary: endPoint,
      readingsUsed,
    });
  }

  return { connection: conn, accounts, readingSources };
}
