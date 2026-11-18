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
npm test            # node:test 单元 + 集成测试（18 个用例）
npm run build       # TypeScript 类型检查并编译到 dist/
npm run gen:fixtures  # 重新生成双源夹具到 fixtures/
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

# 启动查询服务
npx tsx src/cli.ts serve --data ./data --port 8080
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
- 发布中途退出不会改动指针，最近一次完整发布始终可查，重跑可完成更新；
- 发布内容带规范化 SHA-256 指纹，**相同输入重跑指纹一致，自动跳过、不产生重复发布**；
- 撤下记录（墓碑）随指针持久化、只增不减，保证重复重跑结果确定。

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
  storage.ts     不可变发布、原子指针、墓碑、幂等指纹
  pipeline.ts    全量/重跑编排、血缘构建
  server.ts      HTTP 快照查询接口
  cli.ts         命令行入口
scripts/         夹具生成器与端到端演示
test/            node:test 测试
fixtures/        双源夹具（run1 / run2 / all-in-one）
```
