/**
 * CLI 入口。
 *
 * 用法：
 *   tsx src/cli.ts run      --input <接电/读数目录> --data <发布存储目录>
 *   tsx src/cli.ts rerun    --day <YYYY-MM-DD> --input ... --data ...
 *   tsx src/cli.ts resume   --token <暂存 token> --data ...
 *   tsx src/cli.ts cancel   --token <暂存 token> --data ...
 *   tsx src/cli.ts staging  --data ...                列出待提交暂存发布
 *   tsx src/cli.ts serve    --data ... --port 8080
 *
 * 暂停/恢复钩子：设置环境变量 SHOREPOWER_PAUSE_STATUS_FILE=<路径> 后，
 * run/rerun 会在“算完、提交之前”把暂停状态写入该文件并等待；
 * 外部把文件中的 state 改写为 resume 即继续提交，改写为 cancel 则放弃发布。
 * 暂停期间不持发布锁，同一库上的另一日期重跑可以正常发布。
 */

import {
  cancelStaging,
  listPendingStaging,
  resumePublication,
  runFull,
  rerunDay,
  type RunOutcome,
  type RunOptions,
} from './pipeline.js';
import { startHttpServer } from './server.js';
import { cleanupStaging } from './storage.js';

interface CliArgs {
  command: string;
  input: string;
  data: string;
  day: string;
  port: number;
  token: string;
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
    token: get('--token', ''),
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

/** 环境变量驱动的提交前暂停钩子（供并发/中断复现与运维编排使用） */
function resolveRunOptions(): RunOptions | undefined {
  const statusFile = process.env.SHOREPOWER_PAUSE_STATUS_FILE;
  if (!statusFile) return undefined;
  return { pause: { statusFile } };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  await cleanupStaging(args.data);

  switch (args.command) {
    case 'run': {
      printOutcome(await runFull(args.input, args.data, resolveRunOptions()));
      break;
    }
    case 'rerun': {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(args.day)) {
        throw new Error('rerun 需要 --day YYYY-MM-DD（北京时间自然日）');
      }
      printOutcome(await rerunDay(args.input, args.data, args.day, resolveRunOptions()));
      break;
    }
    case 'resume': {
      if (!args.token) throw new Error('resume 需要 --token <暂存 token>');
      printOutcome(await resumePublication(args.data, args.token));
      break;
    }
    case 'cancel': {
      if (!args.token) throw new Error('cancel 需要 --token <暂存 token>');
      await cancelStaging(args.data, args.token);
      console.log(`🧹 已取消并删除暂存发布：${args.token}（当前发布指针未改动）`);
      break;
    }
    case 'staging': {
      const pending = await listPendingStaging(args.data);
      if (pending.length === 0) {
        console.log('没有待提交的暂存发布');
      } else {
        console.log(`待提交暂存发布（${pending.length} 个）：`);
        for (const s of pending) {
          console.log(
            `  - token=${s.token} publicationId=${s.publicationId} ` +
              `mode=${s.plan.mode}${s.plan.rerunDay ? ` day=${s.plan.rerunDay}` : ''} ` +
              `base=${s.basePublicationId ?? '（无）'} created=${new Date(s.createdAtMs).toISOString()}`,
          );
        }
      }
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
          '  run      --input <输入目录> --data <存储目录>        全量结算并发布',
          '  rerun    --day <YYYY-MM-DD> --input ... --data ...   指定北京日期重跑',
          '  resume   --token <暂存 token> --data ...             恢复暂停/中断的暂存发布',
          '  cancel   --token <暂存 token> --data ...             取消暂存发布（指针不变）',
          '  staging  --data <存储目录>                           列出待提交暂存发布',
          '  serve    --data <存储目录> --port 8080               启动查询接口',
          '',
          '环境变量：',
          '  SHOREPOWER_PAUSE_STATUS_FILE=<路径>  提交前暂停，文件 state 改为 resume/cancel 控制恢复',
        ].join('\n'),
      );
  }
}

main().catch((err) => {
  console.error('执行失败：', err instanceof Error ? err.message : err);
  process.exit(1);
});
