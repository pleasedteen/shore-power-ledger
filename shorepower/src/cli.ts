/**
 * CLI 入口。
 *
 * 用法：
 *   tsx src/cli.ts run        --input <接电/读数目录> --data <发布存储目录>
 *   tsx src/cli.ts rerun      --day <YYYY-MM-DD> --input ... --data ...
 *   tsx src/cli.ts serve      --data ... --port 8080
 *   tsx src/cli.ts sync-pull  --upstream http://host:port --data ... --to <发布号> [--from <发布号>]
 */

import { runFull, rerunDay, type RunOutcome } from './pipeline.js';
import { startHttpServer } from './server.js';
import { cleanupStaging, loadCurrentPublication } from './storage.js';
import { materializeSyncedPublication, type PublicationDelta } from './sync.js';

interface CliArgs {
  command: string;
  input: string;
  data: string;
  day: string;
  port: number;
  upstream: string;
  from: string;
  to: string;
}

function parseArgs(argv: string[]): CliArgs {
  const [command, ...rest] = argv;
  const get = (name: string, fallback?: string): string => {
    const i = rest.indexOf(name);
    return i >= 0 ? rest[i + 1]! : (fallback ?? '');
  };
  return {
    command: command ?? 'help',
    input: get('--input', './input'),
    data: get('--data', './data'),
    day: get('--day', ''),
    port: Number(get('--port', '8080')),
    upstream: get('--upstream', 'http://localhost:8080'),
    from: get('--from', ''),
    to: get('--to', ''),
  };
}

function printOutcome(out: RunOutcome): void {
  console.log('────────────── 发布结果 ──────────────');
  if (out.skipped) {
    console.log(`⏭  跳过新发布：${out.reason}`);
    console.log(`   当前发布仍为：${out.publicationId}`);
  } else {
    console.log(`✅ 新发布完成：${out.publicationId}`);
  }
  console.log(`   日账文档：${out.accountCount} 条；隔离文档：${out.quarantineCount} 条`);
  console.log(`   重新结算接电：${out.recomputedConnectionIds.join(', ') || '（无）'}`);
  console.log(`   沿用既有结果：${out.carriedConnectionIds.join(', ') || '（无）'}`);
  console.log(`   撤下失效文档：${out.withdrawnDocIds.join(', ') || '（无）'}`);
  if (out.warnings.length > 0) {
    console.log('   告警：');
    for (const w of out.warnings) console.log(`     - ${w}`);
  }
}

/** 下游差量拉取：从上游 /sync 取净变更并物化到本地数据目录 */
async function syncPull(upstream: string, dataDir: string, from: string, to: string): Promise<void> {
  if (!to) throw new Error('sync-pull 需要 --to <目标发布号>');
  const url = new URL('/sync', upstream);
  url.searchParams.set('to', to);
  if (from) url.searchParams.set('from', from);

  const resp = await fetch(url);
  const body = (await resp.json()) as Record<string, unknown>;
  if (!resp.ok) {
    throw new Error(`上游差量导出失败（HTTP ${resp.status}）：${String(body.error ?? resp.statusText)}`);
  }
  const delta = body as unknown as PublicationDelta;
  const base = await loadCurrentPublication(dataDir);
  const outcome = await materializeSyncedPublication(dataDir, delta, base);

  console.log('────────────── 差量同步结果 ──────────────');
  console.log(`✅ 已同步到目标发布：${outcome.publicationId}`);
  if (outcome.alreadyPresent) console.log('   目标发布目录已存在：本次仅校准指针，未重复写盘');
  console.log(
    `   差量明细 ${delta.stats.deltaRows} 行 / 整版 ${delta.stats.fullRows} 行` +
      `（占比 ${(delta.stats.deltaRowRatio * 100).toFixed(2)}%）`,
  );
  console.log(`   物化后日账：${outcome.accountCount} 条；隔离：${outcome.quarantineCount} 条；血缘：${outcome.lineageCount} 条`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  await cleanupStaging(args.data);

  switch (args.command) {
    case 'run': {
      printOutcome(await runFull(args.input, args.data));
      break;
    }
    case 'rerun': {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(args.day)) {
        throw new Error('rerun 需要 --day YYYY-MM-DD（北京时间自然日）');
      }
      printOutcome(await rerunDay(args.input, args.data, args.day));
      break;
    }
    case 'serve': {
      const handle = await startHttpServer(args.data, args.port);
      console.log(`HTTP 查询服务已启动：http://localhost:${handle.port}`);
      console.log('  /health  /publications  /accounts  /quarantines  /lineage  /connection?id=  /sync?from=&to=');
      const shutdown = () => {
        void handle.close().then(() => process.exit(0));
      };
      process.on('SIGINT', shutdown);
      process.on('SIGTERM', shutdown);
      break;
    }
    case 'sync-pull': {
      await syncPull(args.upstream, args.data, args.from, args.to);
      break;
    }
    default:
      console.log(
        [
          '港口岸电数据管道',
          '',
          '命令：',
          '  run        --input <输入目录> --data <存储目录>              全量结算并发布',
          '  rerun      --day <YYYY-MM-DD> --input ... --data ...         指定北京日期重跑',
          '  serve      --data <存储目录> --port 8080                     启动查询接口',
          '  sync-pull  --upstream <URL> --data <存储目录> --to <发布号> [--from <发布号>]',
          '               下游按净差量同步：省略 --from 用于空目录引导，之后用最近同步号做基版',
        ].join('\n'),
      );
  }
}

main().catch((err) => {
  console.error('执行失败：', err instanceof Error ? err.message : err);
  process.exit(1);
});
