/**
 * 本地 HTTP 查询接口。
 *
 * 快照隔离：每个请求开始时只读取一次 current 指针，整个请求内的日账、隔离、
 * 血缘、清单全部来自同一个发布目录。发布中途退出不会改动指针，因此查询要么
 * 看到上一完整发布，要么看到新的完整发布，绝不会看到半成品。
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { URL } from 'node:url';
import {
  listPublicationIds,
  loadCurrentPublication,
  loadPublication,
  type PublicationBundle,
} from './storage.js';
import { formatBeijing, formatUtc } from './time.js';
import type { DailyAccount, QuarantineEntry } from './types.js';

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

async function resolveBundle(
  dataDir: string,
  publicationId: string | null,
): Promise<PublicationBundle | { error: string }> {
  if (publicationId) {
    const ids = await listPublicationIds(dataDir);
    if (!ids.includes(publicationId)) return { error: `发布 ${publicationId} 不存在` };
    const bundle = await loadPublication(dataDir, publicationId);
    if (!bundle) return { error: `发布 ${publicationId} 无法读取` };
    return bundle;
  }
  const current = await loadCurrentPublication(dataDir);
  if (!current) return { error: '尚无完整发布，请先运行 run / rerun' };
  return current;
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

export async function startHttpServer(dataDir: string, requestedPort: number): Promise<HttpServerHandle> {
  const server = createServer((req, res) => {
    void handle(req, res);
  });

  function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    return (async () => {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const p = url.searchParams;

      if (req.method !== 'GET') {
        sendJson(res, 405, { error: '仅支持 GET' });
        return;
      }

      const bundleOrErr = await resolveBundle(dataDir, p.get('publicationId'));
      if ('error' in bundleOrErr) {
        sendJson(res, 404, bundleOrErr);
        return;
      }
      const bundle = bundleOrErr;
      const snapshot = {
        publicationId: bundle.manifest.publicationId,
        createdAt: new Date(bundle.manifest.createdAtMs).toISOString(),
        mode: bundle.manifest.mode,
        rerunDay: bundle.manifest.rerunDay ?? null,
      };

      switch (url.pathname) {
        case '/health':
          sendJson(res, 200, { ok: true, snapshot });
          return;
        case '/publications':
          sendJson(res, 200, {
            snapshot,
            allPublicationIds: await listPublicationIds(dataDir),
            manifest: bundle.manifest,
            batches: bundle.batches,
          });
          return;
        case '/batches': {
          // 批次台账（含已撤销批次），便于核对撤销依据与历史
          sendJson(res, 200, {
            snapshot,
            count: bundle.batches.length,
            activeBatchIds: bundle.manifest.activeBatchIds,
            revokedBatchIds: bundle.manifest.revokedBatchIds,
            batches: bundle.batches.map((b) => ({
              ...b,
              recordCount: bundle.batchRecords.filter((r) => r.batchId === b.batchId).length,
            })),
          });
          return;
        }
        case '/accounts': {
          let rows = bundle.accounts;
          const day = p.get('day');
          const connectionId = p.get('connectionId');
          const vessel = p.get('vessel');
          const meterId = p.get('meterId');
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
          let rows = bundle.quarantines;
          const connectionId = p.get('connectionId');
          if (connectionId) rows = rows.filter((r) => r.connectionId === connectionId);
          sendJson(res, 200, {
            snapshot,
            count: rows.length,
            quarantines: rows.map(decorateQuarantine),
          });
          return;
        }
        case '/lineage': {
          const subjectDocId = p.get('docId');
          let rows = bundle.lineage;
          if (subjectDocId) rows = rows.filter((r) => r.subjectDocId === subjectDocId);
          sendJson(res, 200, { snapshot, count: rows.length, lineage: rows });
          return;
        }
        case '/connection': {
          const id = p.get('id');
          if (!id) {
            sendJson(res, 400, { error: '缺少查询参数 id' });
            return;
          }
          sendJson(res, 200, {
            snapshot,
            connectionId: id,
            accounts: bundle.accounts.filter((r) => r.connectionId === id).map(decorateAccount),
            quarantine:
              bundle.quarantines
                .filter((r) => r.connectionId === id)
                .map(decorateQuarantine)[0] ?? null,
            lineage: bundle.lineage.filter((r) => r.connectionId === id),
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
              '/batches',
            ],
          });
      }
    })().catch((err) => {
      sendJson(res, 500, { error: (err as Error).message });
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
      new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve())),
      ),
  };
}
