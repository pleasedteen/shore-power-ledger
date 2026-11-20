# 港口岸电用量日账数据管道

本地运行的 TypeScript / Node.js 数据管道，每天按**北京时间（UTC+8，固定偏移，不依赖机器时区）**
核对船舶靠泊期间的岸电用量。

- **接电记录**：GB18030 编码 CSV，时间为北京时间，标明船舶、电表、接电起止时刻。
- **电表读数**：UTF-8 编码 JSON Lines，采样时间为 UTC，累计电量可用 Wh 或小数 kWh。
- **产物**：按「每次接电 × 北京时间自然日」结算的日账、隔离结果、血缘，全部以本地 JSON Lines 落盘。
- **查询**：本地 HTTP 接口，单次查询只能看到同一次完整发布的快照结果。

## 1. 安装与 80 秒内可读结果

要求 Node.js ≥ 20（开发验证于 Node 22）。

```bash
npm install
npm run demo        # 端到端演示：生成夹具 → 发布 → 补发更正重跑 → 幂等 → HTTP 查询
```

`npm run demo` 在普通笔记本上约 1 秒完成（远低于 80 秒要求），全程使用系统临时目录，
不污染工作区，并对每一步打印 `✔/✘` 可读中文核对结果。

其它常用命令：

```bash
npm test            # node:test 单元 + 集成测试（23 个用例，含并发/中断恢复）
npm run build       # TypeScript 类型检查并编译到 dist/
npm run gen:fixtures  # 重新生成双源夹具到 fixtures/
npm run repro:concurrent  # 本地子进程复现并发重跑、暂停与 SIGKILL 中断恢复
```

## 2. 输入数据格式

输入目录递归扫描，`.csv` 按 GB18030 解析、`.jsonl` 按 UTF-8 解析；文件名可乱序，重送无影响。

### 接电记录 CSV（GB18030）

```csv
connection_id,revision,vessel,meter_id,start_beijing,end_beijing
CONN-101,1,远洋之星,M1,2026-11-03 08:00:00,2026-11-04 08:00:00
```

| 列 | 含义 |
| --- | --- |
| connection_id | 接电业务主键，同 id 以最高 `revision` 为准 |
| revision | 修订版本号，越大越新 |
| vessel / meter_id | 船舶 / 电表标识 |
| start_beijing / end_beijing | 北京时间 `YYYY-MM-DD HH:mm:ss`，按 UTC+8 解释 |

### 电表读数 JSONL（UTF-8）

```json
{"reading_id":"R101","revision":1,"meter_id":"M1","sampled_at_utc":"2026-11-03T00:00:00Z","unit":"Wh","cumulative":"12000"}
{"reading_id":"R102","revision":1,"meter_id":"M1","sampled_at_utc":"2026-11-03T16:00:00Z","unit":"kWh","cumulative":"28"}
```

- `unit` 为 `Wh` 或 `kWh`，内部统一归一化为 Wh；不同单位的**等值读数在容差内视为一致**。
- 同 `reading_id` 以最高 `revision` 为准；字节完全相同的重送文件按 SHA-256 自动去重。

## 3. 结算与隔离规则

对每次接电，取本电表在接电区间内（含端点）的全部读数：

1. 接电起止窗口非法（结束不晚于起始）→ `INVALID_CONNECTION_WINDOW`；
2. 同一采样时刻存在不一致累计读数 → `CONFLICTING_READING`（等值不同单位不算冲突）；
3. 累计电量按时间回退 → `CUMULATIVE_REGRESSION`；
4. 缺少接电起点 / 终点 / 区间内北京午夜读数 →
   `MISSING_START_READING` / `MISSING_END_READING` / `MISSING_MIDNIGHT_READING`；
5. 同主键同修订但内容冲突、无法确定权威版本 → `SOURCE_REVISION_CONFLICT`。

任一规则命中，**整次接电进入隔离区**并附原因与证据；全部通过才按北京自然日拆账，
每日用量 = 窗口末端累计读数 − 窗口起端累计读数（起端为接电起点或北京午夜，末端为午夜或接电终点）。

## 4. 运行与发布

```bash
# 全量结算输入目录中的全部接电并发布
npx tsx src/cli.ts run --input ./input --data ./data

# 指定北京时间自然日重跑
npx tsx src/cli.ts rerun --day 2026-11-04 --input ./input --data ./data

# 恢复暂停/中断（如进程被杀）后待提交的暂存发布
npx tsx src/cli.ts resume --token <暂存 token> --data ./data
# 取消暂存发布（当前指针不变）、列出全部待提交暂存
npx tsx src/cli.ts cancel --token <暂存 token> --data ./data
npx tsx src/cli.ts staging --data ./data

# 启动查询服务
npx tsx src/cli.ts serve --data ./data --port 8080
```

### 并发重跑与中断恢复

同一数据目录上的多次发布通过 `publications/pipeline.lock` 跨进程互斥串行化；持锁进程崩溃后，
后来者依据“持有者进程已不存在 / 锁龄超期”自动接管，不会死锁。

- **提交前暂停**：设置环境变量 `SHOREPOWER_PAUSE_STATUS_FILE=<路径>` 后，run/rerun 会在
  “算完、提交之前”把暂停状态（含暂存 token、暂存发布 id、基准发布 id）写入该文件并等待。
  把文件中的 `state` 改写为 `resume` 即继续提交，改写为 `cancel` 则清理暂存、放弃发布。
  暂停期间**不持锁**，同一库上另一个日期的重跑可以正常发布。
- **暂存持久化**：算完的完整产物先落到 `publications/.staging/<token>.json` 与 `<token>.dir/`
  （临时文件 + 原子 rename），进程在提交前退出（含 `SIGKILL`）也不丢失；用
  `staging` 查看、`resume --token` 恢复提交。
- **并发更正分区合并**：若暂存期间另一笔重跑已抢先发布，恢复提交时按**接电分区**合并——
  本笔重算的接电集合覆盖对应分区（其所有相关日期的日账/隔离/血缘），分区外沿用对方已发布
  结果；两笔互不相干的更正都会生效。撤下墓碑取并集只增不减，输入文件清单与告警去重合并，
  合并事件写入清单告警。
- **查询视图**：暂存发布既不进入历史发布清单，也无法凭发布号查到；查询始终只读到完整发布。
  提交前退出的版本在任何时刻都不可见，恢复后才以新发布号一次性物化。

本地复现（真实子进程 + SIGKILL，含暂停、并发发布、恢复合并、重试幂等）：

```bash
npm run repro:concurrent
```

### 重跑语义

指定日期 D 重跑时，纳入：

- 最新接电时段与 D 重叠的接电；**或**
- 上一版发布中接电时段与 D 重叠的接电。

对纳入接电重新结算，在其**所有相关日期**替换日账/隔离/血缘；未纳入接电沿用各自已发布结果。
上一版中被新结果取代或已从输入消失的文档进入持久墓碑（`withdrawnDocIds`）并被撤下。

### 发布的可靠性与幂等

- 每次发布写入不可变目录 `publications/pub-<mode>-<时间戳>-<随机>/`，
  先写临时目录再原子 `rename`，最后原子切换 `current.json` 指针；
- 发布中途退出不会改动指针，最近一次完整发布始终可查；算完待提交的产物在
  `.staging/` 暂存持久化，可凭 token `resume` 恢复（见「并发重跑与中断恢复」）；
- 发布内容带规范化 SHA-256 指纹，**相同输入重跑指纹一致，自动跳过、不产生重复发布**；
- 撤下记录（墓碑）随指针持久化、只增不减，并发合并时取并集，保证重复重跑结果确定。

## 5. HTTP 查询接口

每个请求开始时读取一次当前指针，请求内全部数据来自同一发布目录，实现单发布快照隔离；
加 `publicationId=<id>` 可查询指定历史发布。

| 路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查与当前快照信息 |
| `GET /publications` | 当前发布清单 + 全部历史发布 id |
| `GET /accounts?day=&connectionId=&vessel=&meterId=` | 日账查询，可跨日按条件过滤 |
| `GET /quarantines?connectionId=` | 隔离区查询 |
| `GET /lineage?docId=` | 按结果文档 id 查血缘 |
| `GET /connection?id=` | 一次接电的全部日账 + 隔离 + 血缘 |

示例：

```bash
curl -s "http://localhost:8080/accounts?day=2026-11-04"
curl -s "http://localhost:8080/connection?id=CONN-106"
```

## 6. 产物与血缘

```
data/publications/
├── current.json                     # 指针 + 累积墓碑
├── pipeline.lock                    # 跨进程发布互斥锁（持有者崩溃自动接管）
├── .staging/
│   ├── <token>.json                 # 待提交暂存发布的编排状态（暂停/中断恢复依据）
│   └── <token>.dir/                 # 暂存发布的完整产物（提交时原子物化）
└── pub-full-20261118xxxxxx-xxxxxx/
    ├── daily-accounts.jsonl         # 日账（每日用量 Wh/kWh、边界读数、读数清单）
    ├── quarantines.jsonl            # 隔离结果（原因码、中文说明、证据）
    ├── lineage.jsonl                # 血缘（接电/读数出处、输入文件清单与哈希）
    └── manifest.json                # 发布清单（模式、计数、撤下、告警、指纹）
```

每条日账与隔离结果都有对应血缘文档，记录所依据的原始文件名、行号、记录 id、修订版本及
文件 SHA-256，可完整追溯到原始记录及其修订版本。

## 7. 目录结构

```
src/
  encoding.ts    GB18030 / UTF-8 解码
  time.ts        北京时间自然日、午夜、跨日枚举
  units.ts       Wh/kWh 归一化与等值比较
  ingest.ts      双源扫描、哈希去重、修订合并、出处采集
  settlement.ts  边界校验、隔离判定、按自然日拆账
  storage.ts     不可变发布、原子指针、跨进程锁、暂存/恢复、墓碑、幂等指纹
  pipeline.ts    全量/重跑编排、并发分区合并、暂停/恢复、血缘构建
  server.ts      HTTP 快照查询接口
  cli.ts         命令行入口（run/rerun/resume/cancel/staging/serve）
scripts/         夹具生成器、端到端演示、并发与中断恢复复现
test/            node:test 测试（含并发合并/僵死锁/暂存恢复用例）
fixtures/        双源夹具（run1 / run2 / all-in-one）
```
