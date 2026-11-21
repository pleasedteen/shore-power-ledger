/**
 * 本地 HTTP 查询接口。
 *
 * 快照隔离：每个请求开始时只解析一次 current 指针 / 指定发布号，整个请求内的
 * 日账、隔离、血缘、清单全部来自同一发布及其引用的内容寻址对象。
 * 发布中途退出不会改动指针，因此查询要么看到上一完整发布，要么看到新的
 * 完整发布，绝不会看到半成品。
 *
 * 查询路径直接走发布清单里的索引树根：按 docId 点查血缘只下钻一条路径，
 * 重启后也不需要逐版回放；按天/接电过滤复用索引里的字段。
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { URL } from 'node:url';
import {
  listPublicationIds,
  openCurrentPublication,
  openPublication,
  readAccounts,
  readAllLineage,
  readLineageByConnection,
  readLineageBySubject,
  readQuarantines,
  type PublicationRef,
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

async function resolveRef(
  dataDir: string,
  publicationId: string | null,
): Promise<PublicationRef | { error: string }> {
  if (publicationId) {
    const ids = await listPublicationIds(dataDir);
    if (!ids.includes(publicationId)) return { error: `发布 ${publicationId} 不存在` };
    const ref = await openPublication(dataDir, publicationId);
    if (!ref) return { error: `发布 ${publicationId} 无法读取` };
    return ref;
  }
  const current = await openCurrentPublication(dataDir);
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

export async function startHttpServer(
  dataDir: string,
  requestedPort: number,
): Promise<HttpServerHandle> {
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

      const refOrErr = await resolveRef(dataDir, p.get('publicationId'));
      if ('error' in refOrErr) {
        sendJson(res, 404, refOrErr);
        return;
      }
      const ref = refOrErr;
      const snapshot = {
        publicationId: ref.manifest.publicationId,
        createdAt: new Date(ref.manifest.createdAtMs).toISOString(),
        mode: ref.manifest.mode,
        rerunDay: ref.manifest.rerunDay ?? null,
      };

      switch (url.pathname) {
        case '/health':
          sendJson(res, 200, { ok: true, snapshot });
          return;
        case '/publications':
          sendJson(res, 200, {
            snapshot,
            allPublicationIds: await listPublicationIds(dataDir),
            manifest: ref.manifest,
          });
          return;
        case '/accounts': {
          const rows = await readAccounts(dataDir, ref, {
            day: p.get('day') ?? undefined,
            connectionId: p.get('connectionId') ?? undefined,
            vessel: p.get('vessel') ?? undefined,
            meterId: p.get('meterId') ?? undefined,
          });
          sendJson(res, 200, {
            snapshot,
            count: rows.length,
            accounts: rows.map(decorateAccount),
          });
          return;
        }
        case '/quarantines': {
          const rows = await readQuarantines(
            dataDir,
            ref,
            p.get('connectionId') ?? undefined,
          );
          sendJson(res, 200, {
            snapshot,
            count: rows.length,
            quarantines: rows.map(decorateQuarantine),
          });
          return;
        }
        case '/lineage': {
          const subjectDocId = p.get('docId');
          if (subjectDocId) {
            const row = await readLineageBySubject(dataDir, ref, subjectDocId);
            const rows = row ? [row] : [];
            sendJson(res, 200, { snapshot, count: rows.length, lineage: rows });
            return;
          }
          const connectionId = p.get('connectionId');
          const rows = connectionId
            ? await readLineageByConnection(dataDir, ref, connectionId)
            : await readAllLineage(dataDir, ref);
          sendJson(res, 200, { snapshot, count: rows.length, lineage: rows });
          return;
        }
        case '/connection': {
          const id = p.get('id');
          if (!id) {
            sendJson(res, 400, { error: '缺少查询参数 id' });
            return;
          }
          const [accounts, quarantines, lineage] = await Promise.all([
            readAccounts(dataDir, ref, { connectionId: id }),
            readQuarantines(dataDir, ref, id),
            readLineageByConnection(dataDir, ref, id),
          ]);
          sendJson(res, 200, {
            snapshot,
            connectionId: id,
            accounts: accounts.map(decorateAccount),
            quarantine: quarantines.map(decorateQuarantine)[0] ?? null,
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
