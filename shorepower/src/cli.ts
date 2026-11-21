/**
 * CLI 入口。
 *
 * 用法：
 *   tsx src/cli.ts run          --input <接电/读数目录> --data <发布存储目录>
 *   tsx src/cli.ts rerun        --day <YYYY-MM-DD> --input ... --data ...
 *   tsx src/cli.ts rerun-range  --start <YYYY-MM-DD> --end <YYYY-MM-DD> --input ... --data ...
 *   tsx src/cli.ts serve        --data ... --port 8080
 *
 * 中断注入（仅测试/演练用）：
 *   BACKFILL_CRASH_AFTER=N   回补暂存完第 N 笔接电后以退出码 2 终止，模拟进程中断
 */

import { runFull, rerunDay, rerunRange, type RunOutcome } from './pipeline.js';
import { startHttpServer } from './server.js';
import { cleanupStaging } from './storage.js';

interface CliArgs {
  command: string;
  input: string;
  data: string;
  day: string;
  start: string;
  end: string;
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
    start: get('--start', ''),
    end: get('--end', ''),
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
  console.log(`   重新结算接电：${out.recomputedConnectionIds.join(', ') || '（无）'}`);
  console.log(`   沿用既有结果：${out.carriedConnectionIds.join(', ') || '（无）'}`);
  console.log(`   撤下失效文档：${out.withdrawnDocIds.join(', ') || '（无）'}`);
  if (out.resumed) {
    console.log('   ♻  断点续跑：已完成接电从暂存读回，未重复计算');
  }
  if (out.backfillStartDay) {
    console.log(`   回补范围（北京时间，含首尾）：${out.backfillStartDay} ~ ${out.backfillEndDay}`);
  }
  if (out.warnings.length > 0) {
    console.log('   告警：');
    for (const w of out.warnings) console.log(`     - ${w}`);
  }
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
    case 'rerun-range': {
      // 历史回补：按北京时间日期范围一次发布，支持断点续跑
      const crashEnv = process.env.BACKFILL_CRASH_AFTER;
      const crashAfter = crashEnv && crashEnv.trim() !== '' ? Number(crashEnv) : NaN;
      const outcome = await rerunRange(args.input, args.data, args.start, args.end, {
        onConnectionStaged: ({ completedCount }) => {
          if (Number.isFinite(crashAfter) && completedCount >= crashAfter) {
            console.error(
              `[中断注入] 已暂存 ${completedCount} 笔接电，进程退出（exitCode=2）以模拟中断`,
            );
            process.exit(2);
          }
        },
      });
      printOutcome(outcome);
      break;
    }
    case 'serve': {
      const handle = await startHttpServer(args.data, args.port);
      console.log(`HTTP 查询服务已启动：http://localhost:${handle.port}`);
      console.log('  /health  /publications  /accounts  /quarantines  /lineage  /connection?id=');
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
          '  run          --input <输入目录> --data <存储目录>              全量结算并发布',
          '  rerun        --day <YYYY-MM-DD> --input ... --data ...         指定北京日期重跑',
          '  rerun-range  --start <YYYY-MM-DD> --end <YYYY-MM-DD> ...       按北京日期范围历史回补（一次发布，可续跑）',
          '  serve        --data <存储目录> --port 8080                     启动查询接口',
        ].join('\n'),
      );
  }
}

main().catch((err) => {
  console.error('执行失败：', err instanceof Error ? err.message : err);
  process.exit(1);
});
