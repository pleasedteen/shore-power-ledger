/**
 * CLI 入口。
 *
 * 用法：
 *   tsx src/cli.ts run       --input <接电/读数目录> --data <发布存储目录>
 *   tsx src/cli.ts ingest    --input ... --data ...     补发/更正投递（自动登记批次）
 *   tsx src/cli.ts rerun     --day <YYYY-MM-DD> --input ... --data ...
 *   tsx src/cli.ts revoke    --batch <批次 id> [--dry-run] --data ...
 *   tsx src/cli.ts serve     --data ... --port 8080
 */

import { rerunDay, revokeBatch, runFull, supplement, type RunOutcome } from './pipeline.js';
import { startHttpServer } from './server.js';
import { cleanupStaging } from './storage.js';

interface CliArgs {
  command: string;
  input: string;
  data: string;
  day: string;
  batch: string;
  note: string;
  dryRun: boolean;
  cleanDelivery: boolean;
  port: number;
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
    batch: get('--batch', ''),
    note: get('--note', ''),
    dryRun: rest.includes('--dry-run'),
    cleanDelivery: !rest.includes('--keep-delivery'),
    port: Number(get('--port', '8080')),
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
  if (out.batchId) console.log(`   本次批次：${out.batchId}`);
  console.log(`   有效批次：${out.activeBatchIds.join(', ') || '（无）'}`);
  console.log(`   已撤销批次：${out.revokedBatchIds.join(', ') || '（无）'}`);
  console.log(`   重新结算接电：${out.recomputedConnectionIds.join(', ') || '（无）'}`);
  console.log(`   沿用既有结果：${out.carriedConnectionIds.join(', ') || '（无）'}`);
  console.log(`   撤下失效文档：${out.withdrawnDocIds.join(', ') || '（无）'}`);
  if (out.cleanedFiles.length > 0) {
    console.log(`   已清走投递文件：${out.cleanedFiles.join(', ')}`);
  }
  if (out.warnings.length > 0) {
    console.log('   告警：');
    for (const w of out.warnings) console.log(`     - ${w}`);
  }
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  await cleanupStaging(args.data);

  const runOptions = { cleanDelivery: args.cleanDelivery };

  switch (args.command) {
    case 'run': {
      printOutcome(await runFull(args.input, args.data, runOptions));
      break;
    }
    case 'ingest': {
      printOutcome(await supplement(args.input, args.data, runOptions));
      break;
    }
    case 'rerun': {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(args.day)) {
        throw new Error('rerun 需要 --day YYYY-MM-DD（北京时间自然日）');
      }
      printOutcome(await rerunDay(args.input, args.data, args.day, runOptions));
      break;
    }
    case 'revoke': {
      if (!args.batch) {
        throw new Error('revoke 需要 --batch <批次 id>（可用 serve 的 /batches 查询）');
      }
      const out = await revokeBatch(args.data, {
        batchId: args.batch,
        note: args.note || undefined,
        dryRun: args.dryRun,
      });
      console.log('────────────── 撤销批次 ──────────────');
      console.log(`批次：${out.batchId}${out.dryRun ? '（仅预览）' : ''}`);
      console.log(`受影响接电：${out.affectedConnectionIds.join(', ') || '（无）'}`);
      if (!out.dryRun) {
        console.log(`重算接电：${out.recomputedConnectionIds.join(', ') || '（无）'}`);
        console.log(`撤下文档：${out.withdrawnDocIds.join(', ') || '（无）'}`);
        console.log(
          out.skipped
            ? `⏭  批次状态未变化，发布保持不变：${out.publicationId}`
            : `✅ 撤销已随发布生效：${out.publicationId}`,
        );
      }
      for (const w of out.warnings) console.log(`   告警：${w}`);
      break;
    }
    case 'serve': {
      const handle = await startHttpServer(args.data, args.port);
      console.log(`HTTP 查询服务已启动：http://localhost:${handle.port}`);
      console.log('  /health  /publications  /batches  /accounts  /quarantines  /lineage  /connection?id=');
      const shutdown = () => {
        void handle.close().then(() => process.exit(0));
      };
      process.on('SIGINT', shutdown);
      process.on('SIGTERM', shutdown);
      break;
    }
    default:
      console.log(
        [
          '港口岸电数据管道',
          '',
          '命令：',
          '  run     --input <输入目录> --data <存储目录>        全量结算并发布（首次导入批次）',
          '  ingest  --input ... --data ...                    补发/更正投递，登记可撤销批次',
          '  rerun   --day <YYYY-MM-DD> --input ... --data ... 指定北京日期重跑',
          '  revoke  --batch <批次 id> [--dry-run] --data ...  撤销来源批次并按剩余批次重算',
          '  serve   --data <存储目录> --port 8080             启动查询接口',
          '',
          '  默认发布成功后清走投递文件；加 --keep-delivery 可保留。',
        ].join('\n'),
      );
  }
}

main().catch((err) => {
  console.error('执行失败：', err instanceof Error ? err.message : err);
  process.exit(1);
});
