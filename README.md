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
splitter/             多折分组/时间隔离切分器（只读装配库，独立切分库）
  source.py           只读接入(mode=ro)、完整性校验、来源内容指纹
  inputs.py           切分计划/别名边的严格校验与两遍流式扫描
  graph.py            紧凑 array CSR、并查集、折段事件、确定性 BFS 路径
  store.py            切分库 schema、阶段 checkpoint、只读打开
  pipeline.py         流式构建、状态分类、导出/回查/统计/来源核对
  integers.py         int64 边界与不回绕的端点算术
splitter_acceptance/  独立逐折枚举 oracle + 12 组切分验收
  manual_oracle.py    纯 Python 邻接集合 + BFS，不调用生产分组/折调度
  test_splits.py      四端点/替代路径/embargo/pending/同名异类等
splitter_benchmark/   64 折、12 万样本、百万来路、25 万别名边压测
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

## 多折切分器（分组隔离 + 时间隔离）

在**只读**的已完成装配库上，按切分计划产出独立 SQLite 切分库。不训练、
不重算特征或标签；同一份装配库可服务不同切分计划。失败只丢弃
`.building` 临时库，旧切分库与来源库都不受影响。

### 关系证据（只信装配库实际记录的行）

* 样本连到自己的实体（同实体边）；同一 `fact_id` 连到所有在
  `feature_provenance` 引用它的样本，**哪怕引用的是不同修订**；
  关系按 `(样本,事实)` 去重，但回查逐跳点名两端真实的特征与修订，
  不会把两个修订合并成伪造的单条来路。
* 不从值相等、结局、原始事实全集或 `is_latest` 额外推断。
* 样本/实体/事实是三类命名空间，库内统一映射为带 `kind` 的整数节点。
* 折外、pending、隔离区样本仍能当关系中间点。

### 时效别名边

别名文件列 `link_id,left_entity,right_entity,valid_from,valid_to`，
无向、`link_id` 唯一、区间非空、平行边独立、实体可只出现在别名文件。
每折独立 `link_at`，仅 `valid_from <= link_at < valid_to` 的边生效，
在当折全部活跃边上求传递连通；一条边过期不一定拆开两组，只有不存在
替代路径时才拆开。重复 ID/空区间/非法输入一律拒绝。

### 每折每样本恰一态（折间独立）

`outside` / `pending` / `eval` / `embargo` / `related_eval` / `train`。
embargo 看观察末点 `cutoff+horizon`（含末点）是否不严格早于
`eval_from-gap`，标签为 0 也不豁免，且 embargo 优先于关联性判定；
pending 验证候选不充当对手但仍作中间点；不删验证样本挽救训练行。
所有时间按 int64 精确算术，端点越界报错，不回绕、不转浮点。

### CLI 与可脚本调用的操作

```bash
# 构建（先写 .building，成功后原子替换；失败不破坏旧库）
python3 -I run_splitter.py build --db data/splits.db \
    --assembly data/run.db --plan data/folds.csv --aliases data/links.csv
# 按折导出 train / eval 的 example_id
python3 -I run_splitter.py export --db data/splits.db --fold f0 --state train
# 按折按样本回查（embargo 给观察末点与界线；related_eval 给最小对手与真实路径）
python3 -I run_splitter.py lookup --db data/splits.db --fold f0 --example T
# 按折统计各状态（outside 也精确计数）
python3 -I run_splitter.py stats --db data/splits.db
# 核对装配资料/计划/别名输入（重算内容指纹，换库无法冒充旧清单来源）
python3 -I run_splitter.py verify --db data/splits.db \
    --assembly data/run.db --plan data/folds.csv --aliases data/links.csv
```

### 确定性与内存结构

* 节点/边/折均按字节序确定编号，换输入行序、折执行顺序或重开产物，
  清单、计数、选中对手与回查路径都不变（压测中做过逐行重建比对）。
* 邻接用紧凑 `array` CSR、并查集用 `array`，Python 侧不缓存全量边；
  来路与别名边流式两遍扫描；共享事实是星形，从不展开成样本两两配对。
* 别名边每条仅两次二分定位其命中折段，长期边只在首折 union 一次，
  不为每折重读装配来路或重扫全部长期活跃别名。
* 切分库自洽：查询/导出/回查不依赖原文件路径，未拷贝特征值、结局、
  原始事实；只保存切分必需的节点、去重关系、来路证据、计划、状态与绑定。

### 切分验收与 64 折压测

```bash
python3 -I examples/run_split_walkthrough.py     # 题面四端点+替代路径小演练
python3 -I splitter_acceptance/test_splits.py    # 12 组，独立 oracle 逐折枚举
python3 -I splitter_benchmark/run_split_benchmark.py  # 256MiB 受限 64 折压测
```


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

## 切分库输出表

`nodes`（example/entity/fact 三类命名空间）、`sample_info`（锚点+
标签快照）、`static_edges`（去重的同实体/来路边）、`provenance_refs`
（逐特征逐修订真实来路）、`alias_edges`（全部时效边含平行边）、
`folds`（计划）、`assignments`（每折每样本一行，outside 也落库）、
`source_binding` 与 `input_files`（来源内容指纹与文件哈希）。
不拷贝特征值、结局与原始事实。

## 切分验收覆盖点

共享事实的不同修订不被合并、时效边四端点（含半开边界）、
替代路径部分相交的断开/复连、embargo 数值边界与优先级、
pending 中间点换对手、同名异类编号、多对手选字节序最小、
平行边生效端点、折序打乱与重复 link_at、无可用验证样本、
非法输入拒绝、失败后旧库可用、未完成/孤儿来路库拒绝、
未知折/样本不冒充 outside、来源换库指纹失配、重建逐行确定性。

## 64 折压测实测（RLIMIT_AS=256 MiB）

| 指标 | 实测 |
| --- | --- |
| 样本 / 折数 | 120,000 / 64 |
| 特征来路行 / 别名边 | 1,204,000 / 250,000 |
| assignment 行数（样本×折） | 7,680,000 |
| 装配耗时 / 64 折切分耗时 | 约 54s / 约 118s |
| 峰值 RSS | 约 201 MiB（256 MiB 预算内） |
| 来路扫描次数 | 1（流式单遍，不按折重读） |
| 别名边二分次数 / 朴素重扫比较 | 每条 2 次 / 16,000,000 |
| 共享事实 F_SHARED 度数 | 4,000（=去重样本数，星形未两两展开） |
| 断连/复连三阶段探针 | 全部 OK；替代到期折恰好放行 100 个簇样本 |
| 重建确定性 | 全部表逐行一致 |

