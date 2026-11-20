"""特征装配：先按可用时间选版本，再按实体/类别/事件窗口筛选。

顺序是题目硬性要求，不可调换。本模块用一条集合式查询同时表达两步，
其语义经独立手算 oracle 逐字段对账：

1. **先选版本**：关联子查询在 ``available_time <= cutoff`` 的所有版本中
   逐 fact_id 取 ``MAX(revision)``，子查询不带任何实体/事件窗口条件，
   因此绝不可能“先筛事件区间再挑版本”；低修订晚到不会反向影响
   已选高修订（选择只看 revision 大小）。
2. **后筛窗口**：外层把窗口谓词作用在“该 fact_id 已选出的那个版本行”
   上；若最高修订是 retract 或其事件时间不在 [cutoff-window, cutoff)，
   该 fact_id 直接缺席，旧版本不会被重新选中。
3. 资料文件的行序不参与任何判断，唯一时间依据是 available_time。

可重用落盘结构与批量处理：

* 事实落 ``facts`` 表并建 (entity,category,event_time) 与
  (fact_id,available_time,revision) 索引；查询计划为锚点窗口表
  SCAN + 事实索引 SEARCH + 主键关联子查询，绝不为每个样本重扫事实；
* 结果排序使用磁盘临时 B-Tree（PRAGMA temp_store=FILE），
  Python 侧只缓冲当前分组（窗口内行数有界）并按批 flush；
* 空组通过 NOT EXISTS 反连接在 SQL 侧补齐，内存与锚点总数解耦。
"""

from .integers import checked_sub, exact_sum
from .store import NULL_SENTINEL

# 三种“空结果”使用互不相同的解释状态
STATUS_OK = "OK"
STATUS_COUNT_ZERO = "COUNT_ZERO"              # count=0
STATUS_SUM_ALL_NULL = "SUM_ALL_NULL"          # sum=null：有事实但数值全空
STATUS_NO_ELIGIBLE_FACT = "NO_ELIGIBLE_FACT"  # sum=null：根本无可用事实
STATUS_LATEST_NO_ROW = "LATEST_NO_ROW"        # latest 缺行

# 单批落盘行数，控制 Python 侧缓冲
_FLUSH_AT = 20000


def _decode_value(value_enc):
    if value_enc == NULL_SENTINEL:
        return None
    return int(value_enc)


def build_features(conn):
    """执行特征装配，写入 feature_values / explanations / provenance。"""
    # 窗口嵌套循环索引与版本子查询索引
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_facts_ent "
        "ON facts(entity, category, event_time)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_facts_ver "
        "ON facts(fact_id, available_time, revision)")

    defs = conn.execute(
        "SELECT feature_id, category, window, op "
        "FROM feature_defs ORDER BY feature_id").fetchall()

    # ---- 锚点 × 定义 的窗口表；下界在 Python 侧做溢出检查 ----
    conn.execute("DROP TABLE IF EXISTS temp.anchor_windows")
    conn.execute(
        "CREATE TEMP TABLE anchor_windows ("
        "example_id TEXT NOT NULL, feature_id TEXT NOT NULL, "
        "entity TEXT NOT NULL, cutoff INTEGER NOT NULL, "
        "category TEXT NOT NULL, lower INTEGER NOT NULL, "
        "op TEXT NOT NULL, "
        "PRIMARY KEY (example_id, feature_id)) WITHOUT ROWID")
    aw_rows = []
    for example_id, entity, cutoff in conn.execute(
            "SELECT example_id, entity, cutoff FROM anchors"):
        for feature_id, category, window, op in defs:
            lower = checked_sub(cutoff, window,
                                f"特征 {feature_id} 窗口下界")
            aw_rows.append((example_id, feature_id, entity, cutoff,
                            category, lower, op))
            if len(aw_rows) >= _FLUSH_AT:
                conn.executemany(
                    "INSERT INTO anchor_windows VALUES (?, ?, ?, ?, ?, ?, ?)",
                    aw_rows)
                aw_rows.clear()
    if aw_rows:
        conn.executemany(
            "INSERT INTO anchor_windows VALUES (?, ?, ?, ?, ?, ?, ?)",
            aw_rows)
        aw_rows.clear()

    # ---- 集合式开窗：先版本（子查询）后窗口（外层谓词）----
    # 排序使每个 (锚点,定义) 分组内行按 latest 优先次序排列；
    # 排序走磁盘临时 B-Tree，不占进程内存预算。
    scan_sql = """
        SELECT aw.example_id, aw.feature_id, aw.op,
               f.fact_id, f.revision, f.event_time, f.value_enc
        FROM anchor_windows aw
        JOIN facts f
          ON f.entity = aw.entity
         AND f.category = aw.category
         AND f.available_time <= aw.cutoff
         AND f.event_time >= aw.lower
         AND f.event_time < aw.cutoff
        WHERE f.op = 'upsert'
          AND f.revision = (
                SELECT MAX(g.revision)
                FROM facts g
                WHERE g.fact_id = f.fact_id
                  AND g.available_time <= aw.cutoff
          )
        ORDER BY aw.example_id, aw.feature_id,
                 f.event_time DESC, f.fact_id DESC
    """

    value_rows = []
    explanation_rows = []
    provenance_rows = []
    cur = conn.execute(scan_sql)

    cur_key = None
    acc = None

    def flush_buffers():
        if provenance_rows:
            conn.executemany(
                "INSERT INTO feature_provenance "
                "VALUES (?, ?, ?, ?, ?, ?)", provenance_rows)
        if value_rows:
            conn.executemany(
                "INSERT INTO feature_values VALUES (?, ?, ?)", value_rows)
            conn.executemany(
                "INSERT INTO feature_explanations "
                "VALUES (?, ?, ?, ?, ?, ?)", explanation_rows)
        provenance_rows.clear()
        value_rows.clear()
        explanation_rows.clear()

    def emit_group(key, accum):
        example_id, feature_id, op = key
        _emit_group(example_id, feature_id, op, accum,
                    value_rows, explanation_rows, provenance_rows)

    for example_id, feature_id, op, fact_id, revision, event_time, value_enc in cur:
        key = (example_id, feature_id, op)
        if key != cur_key:
            # 分组边界：上一组完整落盘后再开始下一组，杜绝跨批重复/断裂
            if cur_key is not None:
                emit_group(cur_key, acc)
                flush_buffers()
            cur_key = key
            acc = {"count": 0, "nonnull": 0, "sum": 0,
                   "latest_key": None, "latest": None,
                   "rows": [], "first": True}
        acc["count"] += 1
        if value_enc != NULL_SENTINEL:
            acc["nonnull"] += 1
            acc["sum"] += int(value_enc)  # Python 无界整数，精确累加
        latest_key = (event_time, fact_id)
        if acc["latest_key"] is None or latest_key > acc["latest_key"]:
            acc["latest_key"] = latest_key
            acc["latest"] = (fact_id, revision, event_time,
                             _decode_value(value_enc))
        acc["rows"].append(
            (fact_id, revision, event_time, 1 if acc["first"] else 0))
        acc["first"] = False

    if cur_key is not None:
        emit_group(cur_key, acc)
        flush_buffers()

    # ---- 空组补齐：NOT EXISTS 反连接，内存与锚点数解耦 ----
    empty_sql = """
        SELECT aw.example_id, aw.feature_id, aw.op
        FROM anchor_windows aw
        WHERE NOT EXISTS (
            SELECT 1 FROM feature_values v
            WHERE v.example_id = aw.example_id
              AND v.feature_id = aw.feature_id
        )
        ORDER BY aw.example_id, aw.feature_id
    """
    for example_id, feature_id, op in conn.execute(empty_sql):
        _emit_empty(example_id, feature_id, op,
                    value_rows, explanation_rows)
        if len(value_rows) >= _FLUSH_AT:
            flush_buffers()

    flush_buffers()
    conn.execute("DROP TABLE IF EXISTS temp.anchor_windows")
    conn.execute("DROP INDEX IF EXISTS idx_facts_ent")
    conn.execute("DROP INDEX IF EXISTS idx_facts_ver")


def _emit_group(example_id, feature_id, op, acc,
                value_rows, explanation_rows, provenance_rows):
    """写出单个非空组的值、解释与精确修订来路。"""
    eligible = acc["count"]
    latest_fact = acc["latest"][0] if acc["latest"] else None

    for fact_id, revision, event_time, order_flag in acc["rows"]:
        # order_flag=1 的首行即 latest；非 latest 运算不打标记
        is_latest = order_flag if op == "latest" else 0
        provenance_rows.append(
            (example_id, feature_id, fact_id, revision,
             event_time, is_latest))

    if op == "count":
        value_rows.append((example_id, feature_id, str(eligible)))
        explanation_rows.append((
            example_id, feature_id, STATUS_OK, eligible, acc["nonnull"],
            "count=窗口内可用事实条数（精确修订见 feature_provenance）"))
    elif op == "sum":
        if acc["nonnull"] == 0:
            value_rows.append((example_id, feature_id, None))
            explanation_rows.append((
                example_id, feature_id, STATUS_SUM_ALL_NULL, eligible, 0,
                "窗口内存在可用事实但数值全部为空，sum=null（与 count=0 区分）"))
        else:
            total = exact_sum([acc["sum"]])  # 已精确累加，此处仅语义封装
            value_rows.append((example_id, feature_id, str(total)))
            explanation_rows.append((
                example_id, feature_id, STATUS_OK, eligible, acc["nonnull"],
                "sum 仅累加非空数值，精确整数求和，无 float 转换"))
    else:  # latest
        _fid, _rev, _et, latest_value = acc["latest"]
        value_rows.append((
            example_id, feature_id,
            None if latest_value is None else str(latest_value)))
        explanation_rows.append((
            example_id, feature_id, STATUS_OK, eligible, acc["nonnull"],
            f"latest 取 (event_time, fact_id) 最大者 "
            f"fact_id={acc['latest'][0]} revision={acc['latest'][1]}，"
            f"其值为 {'NULL（保留空值）' if latest_value is None else latest_value}"))


def _emit_empty(example_id, feature_id, op, value_rows, explanation_rows):
    """无合格行时三种运算给出互异的空状态。"""
    if op == "count":
        value_rows.append((example_id, feature_id, "0"))
        explanation_rows.append((
            example_id, feature_id, STATUS_COUNT_ZERO, 0, 0,
            "窗口内无任何可用事实（已先按 available_time<=cutoff 选版本，"
            "retract 不可用），count=0"))
    elif op == "sum":
        value_rows.append((example_id, feature_id, None))
        explanation_rows.append((
            example_id, feature_id, STATUS_NO_ELIGIBLE_FACT, 0, 0,
            "窗口内无可用事实，sum 无对象可取，置 null"))
    else:
        value_rows.append((example_id, feature_id, None))
        explanation_rows.append((
            example_id, feature_id, STATUS_LATEST_NO_ROW, 0, 0,
            "窗口内无可用事实，latest 缺行（与 count=0、sum=null 语义区分）"))
