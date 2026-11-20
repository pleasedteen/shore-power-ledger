/**
 * 本地 HTTP 查询接口（分片按需读取）。
 *
 * 快照隔离：每个请求开始时只读取一次 current 指针（或校验一次 publicationId），
 * 整个请求内的日账、隔离、血缘、清单全部来自同一个发布目录。发布中途退出不会
 * 改动指针，因此查询要么看到上一完整发布，要么看到新的完整发布。
 *
 * 明细读取范围：
 * - /accounts?day=D          只读该日的日账分片；
 * - /accounts?connectionId=C 只读该接电的日账分片；
 * - /quarantines?connectionId 只读该接电的隔离分片；
 * - /lineage?docId=          只读该文档所属接电的血缘分片；
 * - /connection?id=          只读该接电的日账 + 隔离 + 血缘分片；
 * - 不带过滤条件时才会读取全部明细（与原语义一致）。
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { URL } from 'node:url';
import { DetailIoCounter, type DetailIoSnapshot } from './metrics.js';
import {
  listPublicationIds,
  openCurrentPublication,
  openPublication,
  type PublicationReader,
} from './storage.js';
import { formatBeijing, formatUtc } from './time.js';
import type { DailyAccount, QuarantineEntry } from './types.js';

export interface HttpServerOptions {
  /** 每次请求结束时回传该请求的明细读取量（用于验证读写范围，不影响响应） */
  onDetailIo?: (snapshot: DetailIoSnapshot, pathName: string) => void;
}

export interface HttpServerHandle {
  server: Server;
  port: number;
  close(): Promise<void>;
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body, null, 2);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(text + '\n');
}

async function resolveReader(
  dataDir: string,
  publicationId: string | null,
  counter: DetailIoCounter,
): Promise<PublicationReader | { error: string }> {
  if (publicationId) {
    const ids = await listPublicationIds(dataDir);
    if (!ids.includes(publicationId)) return { error: `发布 ${publicationId} 不存在` };
    const reader = await openPublication(dataDir, publicationId);
    if (!reader) return { error: `发布 ${publicationId} 无法读取` };
    return reader.useCounter(counter);
  }
  const current = await openCurrentPublication(dataDir);
  if (!current) return { error: '尚无完整发布，请先运行 run / rerun' };
  return current.useCounter(counter);
}

/** 给输出补充北京时间/UTC 的可读时间字段，不改变存储结构 */
function decorateAccount(a: DailyAccount) {
  return {
    ...a,
    beijingDayLabel: a.beijingDay,
    windowStartBeijing: formatBeijing(a.windowStartMs),
    windowEndBeijing: formatBeijing(a.windowEndMs),
    startBoundaryUtc: formatUtc(a.startBoundary.sampledAtMs),
    endBoundaryUtc: formatUtc(a.endBoundary.sampledAtMs),
    usageLabel: `${a.usageWh} Wh / ${a.usageKwh} kWh`,
  };
}

function decorateQuarantine(q: QuarantineEntry) {
  return {
    ...q,
    startBeijing: formatBeijing(q.startBeijingMs),
    endBeijing: formatBeijing(q.endBeijingMs),
  };
}

export async function startHttpServer(
  dataDir: string,
  requestedPort: number,
  options: HttpServerOptions = {},
): Promise<HttpServerHandle> {
  const server = createServer((req, res) => {
    void handle(req, res);
  });

  function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const counter = new DetailIoCounter();
    let pathName = '';
    return (async () => {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const p = url.searchParams;
      pathName = url.pathname;

      if (req.method !== 'GET') {
        sendJson(res, 405, { error: '仅支持 GET' });
        return;
      }

      // 单次请求固定一个发布读取器，请求内全部数据来自同一发布目录；
      // 读取器附带计量器，仅用于观测本次请求实际读了多少明细。
      const readerOrErr = await resolveReader(dataDir, p.get('publicationId'), counter);
      if ('error' in readerOrErr) {
        sendJson(res, 404, readerOrErr);
        return;
      }
      const reader = readerOrErr;
      const snapshot = {
        publicationId: reader.manifest.publicationId,
        createdAt: new Date(reader.manifest.createdAtMs).toISOString(),
        mode: reader.manifest.mode,
        rerunDay: reader.manifest.rerunDay ?? null,
      };

      switch (url.pathname) {
        case '/health':
          sendJson(res, 200, { ok: true, snapshot });
          return;
        case '/publications':
          sendJson(res, 200, {
            snapshot,
            allPublicationIds: await listPublicationIds(dataDir),
            manifest: reader.manifest,
          });
          return;
        case '/accounts': {
          const day = p.get('day');
          const connectionId = p.get('connectionId');
          const vessel = p.get('vessel');
          const meterId = p.get('meterId');

          // 先按索引能定位的维度收窄候选，只有必要时才读明细
          let rows: DailyAccount[];
          if (connectionId) {
            rows = await reader.accountsForConnection(connectionId);
          } else if (day) {
            rows = await reader.accountsForDay(day);
          } else {
            rows = await reader.allAccounts();
          }
          if (day) rows = rows.filter((r) => r.beijingDay === day);
          if (connectionId) rows = rows.filter((r) => r.connectionId === connectionId);
          if (vessel) rows = rows.filter((r) => r.vessel === vessel);
          if (meterId) rows = rows.filter((r) => r.meterId === meterId);
          sendJson(res, 200, {
            snapshot,
            count: rows.length,
            accounts: rows.map(decorateAccount),
          });
          return;
        }
        case '/quarantines': {
          const connectionId = p.get('connectionId');
          const rows = connectionId
            ? [await reader.quarantineForConnection(connectionId)].filter(
                (q): q is QuarantineEntry => q !== null,
              )
            : await reader.allQuarantines();
          sendJson(res, 200, {
            snapshot,
            count: rows.length,
            quarantines: rows.map(decorateQuarantine),
          });
          return;
        }
        case '/lineage': {
          const subjectDocId = p.get('docId');
          const rows = subjectDocId
            ? await reader.lineageForDoc(subjectDocId)
            : await reader.allLineage();
          sendJson(res, 200, { snapshot, count: rows.length, lineage: rows });
          return;
        }
        case '/connection': {
          const id = p.get('id');
          if (!id) {
            sendJson(res, 400, { error: '缺少查询参数 id' });
            return;
          }
          const [accounts, quarantine, lineage] = await Promise.all([
            reader.accountsForConnection(id),
            reader.quarantineForConnection(id),
            reader.lineageForConnection(id),
          ]);
          sendJson(res, 200, {
            snapshot,
            connectionId: id,
            accounts: accounts.map(decorateAccount),
            quarantine: quarantine ? decorateQuarantine(quarantine) : null,
            lineage,
          });
          return;
        }
        default:
          sendJson(res, 404, {
            error: '未知路径',
            endpoints: [
              '/health',
              '/publications',
              '/accounts?day=&connectionId=&vessel=&meterId=',
              '/quarantines?connectionId=',
              '/lineage?docId=',
              '/connection?id=',
            ],
          });
      }
    })().catch((err) => {
      sendJson(res, 500, { error: (err as Error).message });
    }).finally(() => {
      // 请求结束（成功或失败）后回传该请求的明细读取量
      options.onDetailIo?.(counter.snapshot(), pathName);
    });
  }

  await new Promise<void>((resolve) => {
    server.listen(requestedPort, () => resolve());
  });
  const addr = server.address();
  const port = typeof addr === 'object' && addr ? addr.port : requestedPort;
  return {
    server,
    port,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  };
}
