# 离线训练资料装配器（防时间穿越）

从**样本锚点、带修订的行为事实、结局接收覆盖记录**装配特征表、标签表与
逐样本解释。只做资料装配，不训练模型，也不估计任何统计效应。

核心防穿越原则：

1. **先选版本，再筛窗口**。对锚点 cutoff，只在 `available_time <= cutoff`
   的版本中逐 `fact_id` 取最高 `revision`；然后才按实体、类别、事件窗口
   `[cutoff-window, cutoff)` 过滤。绝不先筛事件区间再挑版本，也不用文件
   到达次序替代可用时间。
2. **撤回（retract）使该 fact_id 整体不可用**；低修订晚到不会反向改写
   已选高修订；新修订可以改实体和事件时间。
3. **特征用各自 cutoff，标签用固定资料截至时刻 K**，两条链路独立，
   不共用一个“最新版本”。
4. **标签必须覆盖闭合**：`received_at <= K` 的覆盖区间并集完整覆盖
   `(cutoff, cutoff+H]`（离散整数即 `[cutoff+1, cutoff+H+1)`）才能给
   0/1；即使已看到正结局，未闭合也只能 `pending` 并保留已观察信息。
   “没有结局行”不等于负例。
5. **失败不交付**：任何输入/定义/端点溢出错误都会删除半成品库。

## 目录

```
assembler/            生产装配器（仅用 Python 标准库 + SQLite）
  store.py            落盘 schema、CSV 导入、唯一性/边界校验、输入哈希
  features.py         cutoff 增量版本扫描 + 窗口聚合 + 逐样本解释/来路
  labels.py           固定 K 的覆盖闭合判定、标签与证据、pending 统计
  pipeline.py         装配编排、构建绑定、按 example_id 回查
  integers.py         int64 边界检查、不溢出的精确整数求和
acceptance/           独立验收（手算 oracle，不调用生产版本选择/窗口索引）
  manual_oracle.py    纯 Python 逐锚点/逐 K 枚举实现
  test_acceptance.py  17 组验收测试
benchmark/            十万锚点/百万事实生成与 256MiB 受限压测
examples/             题面主演练的可复跑脚本
data/                 运行产物（.gitignore 排除，大资料不入库）
```

## 输入文件（CSV，首行表头，列名固定）

| 文件 | 列 |
| --- | --- |
| `anchors.csv` | `example_id, entity, cutoff`（example_id 唯一，重复即拒绝） |
| `feature_defs.csv` | `feature_id, category, window, op, horizon`（window 正整数；op ∈ count/sum/latest） |
| `facts.csv` | `fact_id, revision, event_time, available_time, entity, category, value, op`（revision 正整数；value 留空表示 NULL；op ∈ upsert/retract） |
| `outcomes.csv` | `outcome_id, entity, event_time, available_time`（outcome 不可变，同 ID 异内容拒绝） |
| `coverage.csv` | `entity, a, b, received_at`（半开区间 `[a,b)`，要求 a < b） |

所有整数列必须落在有符号 64 位区间；窗口端点、标签观察区间端点计算
溢出时明确报错，不做静默回绕。

## 三种运算与三种“空”

* `count`：窗口内可用事实条数。无事实 → 值 `0`，状态 `COUNT_ZERO`。
* `sum`：只累加非空数值，Python 无界整数精确求和，**不转 float、
  不因 int64 单值相加溢出**。无事实 → `null / NO_ELIGIBLE_FACT`；
  有事实但数值全空 → `null / SUM_ALL_NULL`。
* `latest`：按 `(event_time, fact_id)` 取最大者，值为 NULL 时保留 NULL。
  无事实 → `null / LATEST_NO_ROW`。

参与事实的**精确修订**记录在独立可分页关系 `feature_provenance`
（含 `is_latest` 标记），不塞进一行 JSON。

## 快速开始（可从空目录复跑）

```bash
# 1) 题面主演练（自动造资料、装配、与手算 oracle 对账）
python3 -I examples/run_walkthrough.py

# 2) 独立验收（17 组小资料枚举测试，不调用生产版本选择/窗口索引）
python3 -I acceptance/test_acceptance.py

# 3) CLI：装配 / 回查 / pending 统计
python3 -I run_assembler.py assemble \
    --db data/run.db --horizon 5 --as-of-k 20 \
    --anchors data/walkthrough/anchors.csv \
    --feature-defs data/walkthrough/feature_defs.csv \
    --facts data/walkthrough/facts.csv \
    --outcomes data/walkthrough/outcomes.csv \
    --coverage data/walkthrough/coverage.csv
python3 -I run_assembler.py explain --db data/run.db --example-id E1
python3 -I run_assembler.py pending-stats --db data/run.db

# 4) 大规模压测（默认 100,000 锚点 / 1,200,000 事实，RLIMIT_AS=256MiB）
python3 -I benchmark/run_benchmark.py
```

建议始终用 `python3 -I` 运行，避免当前目录/站点包中的同名模块被植入加载。

## 落盘结构与批量处理

* 事实流式入 `facts` 表（Python 侧只保留一个 5000 行批次），建
  `(entity,category,event_time)` 与 `(fact_id,available_time,revision)`
  索引；不为每个样本重扫事实。
* 特征用**一条集合式查询**表达“先选版本（关联子查询取 `MAX(revision)`）
  后筛窗口（外层谓词）”，查询计划是锚点窗口表 SCAN + 事实索引
  SEARCH + 主键版本子查询；大排序走磁盘临时 B-Tree
  （`PRAGMA temp_store=FILE`），Python 侧按分组边界 flush，
  内存与锚点总数解耦。
* 标签侧实体逐个处理，覆盖区间在内存中只保留当前实体的合并结果；
  每条标签的覆盖依据只记录与 H 点观察窗口相交的合并段，证据量有界。
* `meta` 表保存输入文件 SHA-256、H、K 与 `build_status`，
  实现构建与输入资料/定义的绑定。

## 实测规模（默认压测，见 `benchmark/`）

| 指标 | 实测 |
| --- | --- |
| 锚点 / 事实 | 100,000 / 1,200,000 |
| 特征值 / 解释 / 来路 | 300,000 / 300,000 / 16,923 行 |
| 标签 / 标签证据 | 100,000 / 100,004 行（证据量有界） |
| 装配耗时 | 约 45 秒（本机，含资料导入与全部装配） |
| 峰值 RSS | 约 49 MiB，硬限制 `RLIMIT_AS=256 MiB` 下通过 |
| 精确求和 | 抽样锚点 Python 精确求和与库值逐位一致 |

`data/` 下的大资料与数据库由 `.gitignore` 排除，不入库。

## 输出表

`feature_values`、`feature_explanations`（三种空状态互异）、
`feature_provenance`（精确修订、可分页）、`labels`
（positive/negative/pending）、`label_evidence`
（`positive_outcome` / `coverage_interval` / `coverage_gap`）。
特征来路与标签依据分表存储，禁止混用。

## 验收覆盖点

撤回、改实体、版本把事件时间移出窗口、重复锚点、全空数值、
半开边界（下界含上界不含）、覆盖断裂与相邻拼接、迟到覆盖/结局、
错误修订（同 ID/revision 异内容拒绝）、极端 int64 与端点溢出、
输入行序置换不变性、特征/标签来路隔离、失败库清理。
