"""切分编排：只读装配库 -> 自洽 SQLite 切分库（流式，256 MiB 预算）。

构建期对装配来路与别名边全部采用流式扫描 + 紧凑 ``array``/SQL 落盘，
不把百万级来路行或全量别名行同时放进 Python 内存：

* 样本与节点按命名空间分块编号，映射字典在图构建后即释放；
* 静态边、来路证据、别名边都分批写入切分库，CSR 用两遍扫描构建；
* 每折只持有 bytearray 状态与两个紧凑数组，DSU 在静态组件号上复位复用。

失败时只丢弃 ``.building`` 临时库，旧切分库与只读来源库均不受影响。
"""

import gc
import os
from array import array

from .errors import SplitError
from .graph import (DSU, bfs_path, build_csr_streaming, plan_edge_activity)
from .inputs import (hash_inputs, iter_alias_links, load_fold_plan,
                     scan_alias_entities, sha256_file)
from .integers import checked_add, checked_sub
from .source import open_source_readonly, source_fingerprint, validate_source
from .store import (ALL_STATES, STATE_EMBARGO, STATE_EVAL, STATE_OUTSIDE,
                    STATE_PENDING, STATE_RELATED, STATE_TRAIN,
                    checkpoint_and_close, connect_split,
                    open_split_readonly)

_FLUSH = 50000
KIND_EXAMPLE, KIND_ENTITY, KIND_FACT = 0, 1, 2
KIND_NAME = ("example", "entity", "fact")
# 标签状态压缩码
ST_PENDING, ST_CLOSED = 0, 1


# ============================ 构建 ============================

def build_splits(out_db, *, assembly_db, fold_plan_csv, alias_csv,
                 replace=True):
    """构建多折切分库，返回构建绑定信息。

    先写入 ``out_db.building`` 临时库，全部成功提交后才原子替换；
    任何失败都删除临时库，旧库保持可用。
    """
    plan = load_fold_plan(fold_plan_csv)
    # 第一遍扫描别名文件：只校验并收集实体名，不保留边行
    alias_entities = scan_alias_entities(alias_csv)

    src = open_source_readonly(assembly_db)
    tmp_path = out_db + ".building"
    for suffix in ("", "-wal", "-shm"):
        p = tmp_path + suffix
        if os.path.exists(p):
            os.remove(p)
    conn = connect_split(tmp_path)
    failed = False
    try:
        try:
            binding = validate_source(src)
            fingerprint = source_fingerprint(src)
            plan_hash = hash_inputs([fold_plan_csv])
            alias_hash = hash_inputs([alias_csv])

            _build(conn, src, alias_entities, alias_csv, plan,
                   binding, fingerprint, plan_hash, alias_hash,
                   assembly_db, fold_plan_csv)
            conn.execute(
                "INSERT INTO meta VALUES ('build_status', 'complete')")
            conn.commit()
        except Exception:
            conn.rollback()
            failed = True
            raise
    finally:
        checkpoint_and_close(conn)
        src.close()
        if failed:
            for p in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
                if os.path.exists(p):
                    os.remove(p)

    if replace and os.path.exists(out_db):
        os.remove(out_db)
        for suffix in ("-wal", "-shm"):
            p = out_db + suffix
            if os.path.exists(p):
                os.remove(p)
    # checkpoint(TRUNCATE) 已把 WAL 合并进主库并删除侧车文件，
    # 因此这里重命名的是自洽的单一主库文件
    os.rename(tmp_path, out_db)
    for suffix in ("-wal", "-shm"):
        p = out_db + suffix
        if os.path.exists(p):
            os.remove(p)
    n_alias = _count_file_lines(alias_csv)
    return {"db_path": out_db, "source_fingerprint": fingerprint,
            "n_folds": len(plan), "n_alias_edges": n_alias}


def _count_file_lines(path):
    with open(path, "rb") as fh:
        return max(0, sum(1 for _ in fh) - 1)


# ============================ 构建内部 ============================

def _stream_samples(src):
    """流式读取样本锚点+标签，返回紧凑列（按 example_id 字节序）。"""
    cur = src.execute(
        """
        SELECT a.example_id, a.entity, a.cutoff, l.horizon, l.status
        FROM anchors a JOIN labels l ON l.example_id = a.example_id
        ORDER BY a.example_id
        """)
    ex_names, ex_entity, label_names = [], [], []
    cutoff_arr, obs_end_arr = array('q'), array('q')
    status_codes = bytearray()
    horizon_seen = set()
    while True:
        rows = cur.fetchmany(_FLUSH)
        if not rows:
            break
        for example_id, entity, cutoff, horizon, status in rows:
            horizon_seen.add(horizon)
            ex_names.append(example_id)
            ex_entity.append(entity)
            label_names.append(status)  # 保留真实 positive/negative/pending
            cutoff_arr.append(cutoff)
            obs_end_arr.append(checked_add(
                cutoff, horizon,
                f"观察末点 cutoff+horizon (example {example_id})"))
            status_codes.append(
                ST_PENDING if status == "pending" else ST_CLOSED)
    if len(horizon_seen) > 1:
        raise SplitError(
            f"labels.horizon 存在多个不同取值: {sorted(horizon_seen)}")
    if not ex_names:
        raise SplitError("装配库 anchors 为空，拒绝切分")
    return (ex_names, ex_entity, label_names, cutoff_arr, obs_end_arr,
            status_codes)


def _build(conn, src, alias_entities, alias_csv, plan, binding,
           fingerprint, plan_hash, alias_hash, assembly_db, fold_plan_csv):
    # ---- 样本列（流式）----
    (ex_names, ex_entity, label_names, cutoff_arr, obs_end_arr,
     status_codes) = _stream_samples(src)
    n_ex = len(ex_names)

    # ---- 节点编号：三类命名空间分块，块内按 name 字节序 ----
    anchor_entities = sorted(set(ex_entity))
    extra_entities = sorted(alias_entities - set(anchor_entities))
    entity_names = anchor_entities + extra_entities
    n_ent = len(entity_names)
    ent_base = n_ex
    fact_base = n_ex + n_ent

    # 事实名：让 SQLite 做 DISTINCT/排序，流式取回
    fact_names = [r[0] for r in src.execute(
        "SELECT DISTINCT fact_id FROM feature_provenance ORDER BY fact_id")]
    n_fact = len(fact_names)
    n_nodes = fact_base + n_fact

    ex_node = {name: i for i, name in enumerate(ex_names)}
    ent_node = {name: ent_base + i for i, name in enumerate(entity_names)}
    fact_node = {name: fact_base + i for i, name in enumerate(fact_names)}

    node_buf = []
    for i, name in enumerate(ex_names):
        node_buf.append((i, KIND_NAME[KIND_EXAMPLE], name))
    for i, name in enumerate(entity_names):
        node_buf.append((ent_base + i, KIND_NAME[KIND_ENTITY], name))
    for i, name in enumerate(fact_names):
        node_buf.append((fact_base + i, KIND_NAME[KIND_FACT], name))
    _batched(conn, "INSERT INTO nodes VALUES (?, ?, ?)", node_buf)

    info_buf = []
    for i in range(n_ex):
        info_buf.append((ex_names[i], i, ex_entity[i], cutoff_arr[i],
                         obs_end_arr[i] - cutoff_arr[i], obs_end_arr[i],
                         label_names[i]))
    _batched(conn,
             "INSERT INTO sample_info VALUES (?, ?, ?, ?, ?, ?, ?)",
             info_buf)

    # ---- 静态边第一批：样本--同实体（按样本节点确定序）----
    edge_id = 0
    edge_buf = []
    for i in range(n_ex):
        edge_buf.append((edge_id, "entity", i, ent_node[ex_entity[i]]))
        edge_id += 1
        if len(edge_buf) >= _FLUSH:
            _batched(conn,
                     "INSERT INTO static_edges VALUES (?, ?, ?, ?)",
                     edge_buf)
    _batched(conn, "INSERT INTO static_edges VALUES (?, ?, ?, ?)", edge_buf)

    # ---- 来路证据全量流式落盘 + 去重 (样本,事实) 静态边第二批 ----
    prov_cur = src.execute(
        "SELECT example_id, feature_id, fact_id, revision, event_time, "
        "is_latest FROM feature_provenance "
        "ORDER BY example_id, fact_id, feature_id, revision")
    ref_id = 0
    ref_buf, pair_buf = [], []
    last_pair = None
    while True:
        rows = prov_cur.fetchmany(_FLUSH)
        if not rows:
            break
        for example_id, feature_id, fact_id, revision, event_time, is_latest in rows:
            ref_buf.append((ref_id, example_id, feature_id, fact_id,
                            revision, event_time, is_latest))
            ref_id += 1
            pair = (example_id, fact_id)
            if pair != last_pair:  # 输入已排序，相邻去重即全集去重
                pair_buf.append(
                    (edge_id, "fact", ex_node[example_id],
                     fact_node[fact_id]))
                edge_id += 1
                last_pair = pair
            if len(ref_buf) >= _FLUSH:
                _batched(
                    conn,
                    "INSERT INTO provenance_refs VALUES "
                    "(?, ?, ?, ?, ?, ?, ?)", ref_buf)
            if len(pair_buf) >= _FLUSH:
                _batched(
                    conn,
                    "INSERT INTO static_edges VALUES (?, ?, ?, ?)",
                    pair_buf)
    _batched(conn,
             "INSERT INTO provenance_refs VALUES (?, ?, ?, ?, ?, ?, ?)",
             ref_buf)
    _batched(conn, "INSERT INTO static_edges VALUES (?, ?, ?, ?)",
             pair_buf)
    n_static_edges = edge_id

    # 阶段提交：节点/样本/静态边/来路证据已完整，截断 WAL 控制规模
    _checkpoint(conn)

    # 映射字典不再需要，及时释放给后续 CSR/DSU 腾出内存
    del ex_node, fact_node
    gc.collect()

    # ---- 别名边：第二遍流式扫描，先落暂存表，再按 link_id 字节序编号 ----
    conn.execute(
        "CREATE TEMP TABLE alias_stage ("
        "link_id TEXT NOT NULL, left_node INTEGER NOT NULL, "
        "right_node INTEGER NOT NULL, valid_from INTEGER NOT NULL, "
        "valid_to INTEGER NOT NULL)")
    stage_buf = []
    for link in iter_alias_links(alias_csv):
        stage_buf.append((
            link["link_id"], ent_node[link["left_entity"]],
            ent_node[link["right_entity"]],
            link["valid_from"], link["valid_to"]))
        if len(stage_buf) >= _FLUSH:
            _batched(conn,
                     "INSERT INTO alias_stage VALUES (?, ?, ?, ?, ?)",
                     stage_buf)
    _batched(conn, "INSERT INTO alias_stage VALUES (?, ?, ?, ?, ?)",
             stage_buf)
    del ent_node
    gc.collect()
    # 窗口函数按 link_id 字节序分配确定 edge_id，与输入行序无关
    conn.execute(
        "INSERT INTO alias_edges "
        "SELECT ROW_NUMBER() OVER (ORDER BY link_id) - 1 AS edge_id, "
        "link_id, left_node, right_node, valid_from, valid_to "
        "FROM alias_stage")
    conn.execute("DROP TABLE alias_stage")
    n_alias = conn.execute("SELECT COUNT(*) FROM alias_edges").fetchone()[0]
    _checkpoint(conn)

    # ---- 紧凑 CSR（两遍 SQL 扫描，不缓存 Python 边列表）----
    static_csr = build_csr_streaming(
        n_nodes,
        lambda: ((r[0], r[1]) for r in conn.execute(
            "SELECT a_node, b_node FROM static_edges ORDER BY edge_id")))
    alias_csr = build_csr_streaming(
        n_nodes,
        lambda: ((r[0], r[1], r[2]) for r in conn.execute(
            "SELECT left_node, right_node, edge_id FROM alias_edges "
            "ORDER BY edge_id")),
        with_edge_ids=True)
    # 时间用有符号 64 位数组（'q'），极端端点不回绕、不转 float；
    # 端点用 32 位节点号数组，全部别名数组合计每条边仅 16 字节
    alias_from = array('q', (r[0] for r in conn.execute(
        "SELECT valid_from FROM alias_edges ORDER BY edge_id")))
    alias_to = array('q', (r[0] for r in conn.execute(
        "SELECT valid_to FROM alias_edges ORDER BY edge_id")))
    alias_left = array('i', (r[0] for r in conn.execute(
        "SELECT left_node FROM alias_edges ORDER BY edge_id")))
    alias_right = array('i', (r[0] for r in conn.execute(
        "SELECT right_node FROM alias_edges ORDER BY edge_id")))

    # ---- 静态并查集（流式 union）：折外/pending/隔离样本同样传递关系 ----
    static_dsu = DSU(n_nodes)
    for a, b in conn.execute(
            "SELECT a_node, b_node FROM static_edges"):
        static_dsu.union(a, b)
    static_root = [static_dsu.find(i) for i in range(n_nodes)]
    root_order = sorted(set(static_root))
    comp_index = {root: idx for idx, root in enumerate(root_order)}
    component_of = array('i', (comp_index[r] for r in static_root))
    n_components = len(root_order)
    del static_dsu, static_root, root_order, comp_index
    gc.collect()

    # ---- 折计划：按 fold_id 字节序执行，输入行序不影响产物 ----
    folds_sorted = sorted(plan, key=lambda f: f["fold_id"].encode("utf-8"))
    fold_rows = [(f["fold_id"], order, f["link_at"], f["train_from"],
                  f["train_to"], f["eval_from"], f["eval_to"], f["gap"])
                 for order, f in enumerate(folds_sorted)]
    _batched(conn, "INSERT INTO folds VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
             fold_rows)

    # 每条别名边仅两次二分定位折段，不逐折重扫长期活跃边
    starts, _ends = plan_edge_activity(
        folds_sorted, n_alias, alias_from, alias_to)

    # 大批量装载期间先移除 assignments 的二级索引，折全部算完再重建，
    # 显著降低 768 万行的 B-Tree 维护成本（主键索引仍保留）
    conn.execute("DROP INDEX IF EXISTS idx_assign_state")
    conn.execute("DROP INDEX IF EXISTS idx_assign_example")
    _checkpoint(conn)

    _run_folds(conn, folds_sorted, ex_names, cutoff_arr, obs_end_arr,
               status_codes, component_of, n_components,
               alias_left, alias_right, alias_from, alias_to, starts)

    conn.execute(
        "CREATE INDEX idx_assign_state "
        "ON assignments (fold_id, state, example_id)")
    conn.execute(
        "CREATE INDEX idx_assign_example "
        "ON assignments (example_id, fold_id)")
    _checkpoint(conn)

    # ---- 来源绑定与输入文件哈希 ----
    conn.execute("INSERT INTO meta VALUES ('source_fingerprint', ?)",
                 (fingerprint,))
    binding_rows = [
        ("assembly_input_hash", binding["input_hash"]),
        ("assembly_horizon", binding["horizon"]),
        ("assembly_as_of_k", binding["as_of_k"]),
        ("assembly_n_anchors", binding["n_anchors"]),
        ("fold_plan_hash", plan_hash),
        ("alias_hash", alias_hash),
    ]
    _batched(conn, "INSERT INTO source_binding VALUES (?, ?)",
             [(k, "" if v is None else str(v)) for k, v in binding_rows])
    file_rows = [
        ("assembly", os.path.abspath(assembly_db),
         sha256_file(assembly_db)),
        ("fold_plan", os.path.abspath(fold_plan_csv),
         sha256_file(fold_plan_csv)),
        ("alias_links", os.path.abspath(alias_csv),
         sha256_file(alias_csv)),
    ]
    _batched(conn, "INSERT INTO input_files VALUES (?, ?, ?)", file_rows)
    conn.commit()


def _run_folds(conn, folds_sorted, ex_names, cutoff_arr, obs_end_arr,
               status_codes, component_of, n_components,
               alias_left, alias_right, alias_from, alias_to, starts):
    n_ex = len(ex_names)
    # 每个样本的静态组件号（紧凑数组），折内不再逐样本索引大 component_of
    sample_comp = array('i', (component_of[i] for i in range(n_ex)))
    fold_dsu = DSU(n_components)
    # 复用缓冲：每折复位，避免重复分配
    state_codes = bytearray(n_ex)
    min_eval = array('i', [-1]) * n_components
    root_of = array('i', [0]) * n_components
    insert_buf = []
    # 状态码 -> 落库词汇（下标即状态码，list 比 dict 更快且确定）
    code_to_state = ("", STATE_OUTSIDE, STATE_PENDING, STATE_EVAL,
                     STATE_EMBARGO, STATE_RELATED, STATE_TRAIN)

    for fold_idx, fold in enumerate(folds_sorted):
        fold_id = fold["fold_id"]
        # 组件级 DSU 复位为静态组件单点，再 union 本折段首次生效的边；
        # 长期边只在首折处理一次，失效由复位天然处理。
        fold_dsu.reset()
        for edge_id in starts[fold_idx]:
            fold_dsu.union(
                component_of[alias_left[edge_id]],
                component_of[alias_right[edge_id]])
        # 一次性算出每个静态组件在当折的并查集根，折内只做数组索引
        for c in range(n_components):
            root_of[c] = fold_dsu.find(c)

        embargo_line = checked_sub(
            fold["eval_from"], fold["gap"],
            f"隔离界线 eval_from-gap (fold {fold_id})")
        tr_from, tr_to = fold["train_from"], fold["train_to"]
        ev_from, ev_to = fold["eval_from"], fold["eval_to"]

        eval_nodes = []
        for i in range(n_ex):
            c = cutoff_arr[i]
            if ev_from <= c < ev_to:
                if status_codes[i] == ST_PENDING:
                    state_codes[i] = 2  # pending
                else:
                    state_codes[i] = 3  # eval
                    eval_nodes.append(i)
            elif tr_from <= c < tr_to:
                # embargo 优先于关联性，0 标签不豁免
                state_codes[i] = (
                    4 if obs_end_arr[i] >= embargo_line else 0)
            else:
                state_codes[i] = 1  # outside（两段范围都不含）

        # 组件 -> 字节序最小 eval 样本；eval_nodes 已按节点序（=字节序）
        for c in range(n_components):
            min_eval[c] = -1
        for node in eval_nodes:
            root = root_of[sample_comp[node]]
            if min_eval[root] == -1:
                min_eval[root] = node

        for i in range(n_ex):
            code = state_codes[i]
            opponent = None
            line = None
            if code == 0:
                root = root_of[sample_comp[i]]
                opp_node = min_eval[root]
                if opp_node != -1:
                    code = 5  # related_eval
                    opponent = ex_names[opp_node]
                else:
                    code = 6  # train
            elif code == 4:
                line = embargo_line
            insert_buf.append((
                fold_id, ex_names[i], code_to_state[code],
                cutoff_arr[i], obs_end_arr[i], line, opponent))
            if len(insert_buf) >= _FLUSH:
                _batched(
                    conn,
                    "INSERT INTO assignments VALUES (?, ?, ?, ?, ?, ?, ?)",
                    insert_buf)
        # 每 8 折阶段提交并截断 WAL，避免 64 折累积出巨型回滚段
        if (fold_idx + 1) % 8 == 0:
            _batched(
                conn,
                "INSERT INTO assignments VALUES (?, ?, ?, ?, ?, ?, ?)",
                insert_buf)
            _checkpoint(conn)
    _batched(conn,
             "INSERT INTO assignments VALUES (?, ?, ?, ?, ?, ?, ?)",
             insert_buf)


def _batched(conn, sql, buf):
    if not buf:
        return
    conn.executemany(sql, buf)
    buf.clear()


def _checkpoint(conn):
    """阶段提交并把 WAL 合并截断，约束磁盘 WAL 与回滚段规模。

    半成品只存在于 .building 库；若之后任一步失败，整个 .building 库
    都会被删除，因此阶段提交不影响“失败不交付”的原子性。
    """
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")


# ============================ 查询 / 导出 / 回查 ============================

def export_examples(db_path, fold_id, state):
    """按折导出 train/eval 的 example_id（字节序确定）。"""
    if state not in (STATE_TRAIN, STATE_EVAL):
        raise SplitError(f"导出状态仅支持 train/eval，收到 {state!r}")
    conn = open_split_readonly(db_path)
    try:
        _require_fold(conn, fold_id)
        rows = conn.execute(
            "SELECT example_id FROM assignments "
            "WHERE fold_id=? AND state=? ORDER BY example_id",
            (fold_id, state)).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def fold_stats(db_path, fold_id=None):
    """按折统计各状态计数；fold_id=None 返回全部折。outside 也计入。"""
    conn = open_split_readonly(db_path)
    try:
        fold_ids = [r[0] for r in conn.execute(
            "SELECT fold_id FROM folds ORDER BY fold_id")]
        if fold_id is not None:
            _require_fold(conn, fold_id)
        counts = {f: {st: 0 for st in ALL_STATES} for f in fold_ids}
        where, params = "", ()
        if fold_id is not None:
            where, params = "WHERE fold_id=?", (fold_id,)
        for fid, state, n in conn.execute(
                f"SELECT fold_id, state, COUNT(*) FROM assignments "
                f"{where} GROUP BY fold_id, state", params):
            counts[fid][state] = n
        return counts[fold_id] if fold_id is not None else counts
    finally:
        conn.close()


def lookup_example(db_path, fold_id, example_id, *, with_path=True):
    """按折按样本回查状态与原因；related_eval 附一条真实连接路径。

    未知折/未知样本明确报错，绝不冒充 outside；outside 也可精确回查。
    """
    conn = open_split_readonly(db_path)
    try:
        _require_fold(conn, fold_id)
        node = conn.execute(
            "SELECT node_id FROM nodes WHERE kind='example' AND name=?",
            (example_id,)).fetchone()
        if node is None:
            raise SplitError(f"未知样本 example_id={example_id}")
        start_node = node[0]
        row = conn.execute(
            "SELECT state, cutoff, obs_end, embargo_line, opponent_id "
            "FROM assignments WHERE fold_id=? AND example_id=?",
            (fold_id, example_id)).fetchone()
        if row is None:
            raise SplitError(
                f"折 {fold_id} 中没有样本 {example_id} 的状态记录")
        state, cutoff, obs_end, embargo_line, opponent = row
        fold = conn.execute(
            "SELECT link_at, train_from, train_to, eval_from, eval_to, gap "
            "FROM folds WHERE fold_id=?", (fold_id,)).fetchone()
        report = {
            "fold_id": fold_id, "example_id": example_id, "state": state,
            "cutoff": cutoff, "obs_end": obs_end,
            "fold": {"link_at": fold[0],
                     "train_range": [fold[1], fold[2]],
                     "eval_range": [fold[3], fold[4]], "gap": fold[5]},
        }
        if state == STATE_EMBARGO:
            report["reason"] = {
                "obs_end": obs_end, "embargo_line": embargo_line,
                "rule": "obs_end(cutoff+horizon) 不严格早于 "
                        "eval_from-gap，触发时间隔离（标签为 0 也不豁免）"}
        elif state == STATE_RELATED:
            report["reason"] = {"opponent_id": opponent}
            if with_path:
                report["reason"]["path"] = _explain_path(
                    conn, fold_id, start_node, opponent)
        elif state == STATE_PENDING:
            report["reason"] = {
                "rule": "落在验证范围但装配标签为 pending，"
                        "不充当验证对手，但仍可作为关系中间点"}
        elif state == STATE_OUTSIDE:
            report["reason"] = {
                "rule": "cutoff 既不在训练范围也不在验证范围"}
        elif state == STATE_EVAL:
            report["reason"] = {"rule": "落在验证范围且标签已闭合"}
        else:
            report["reason"] = {
                "rule": "训练候选且与任一 eval 样本均不连通"}
        return report
    finally:
        conn.close()


def _explain_path(conn, fold_id, start_node, opponent):
    """构造并格式化真实连接路径，逐跳区分同实体/来路/时效边。"""
    link_at = conn.execute(
        "SELECT link_at FROM folds WHERE fold_id=?",
        (fold_id,)).fetchone()[0]
    n_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    static_csr, alias_csr, alias_from, alias_to = _load_graph_from_db(
        conn, n_nodes)
    target = conn.execute(
        "SELECT node_id FROM nodes WHERE kind='example' AND name=?",
        (opponent,)).fetchone()[0]
    path, via = bfs_path(static_csr, alias_csr, alias_from, alias_to,
                         link_at, start_node, target)
    hops = []
    for i in range(1, len(path)):
        kind, edge_id = via[i - 1]
        hops.append(_format_hop(conn, kind, edge_id,
                                path[i - 1], path[i]))
    return {"nodes": [_node_brief(conn, n) for n in path], "hops": hops}


def _node_brief(conn, node_id):
    kind, name = conn.execute(
        "SELECT kind, name FROM nodes WHERE node_id=?",
        (node_id,)).fetchone()
    return {"kind": kind, "id": name}


def _format_hop(conn, kind, edge_id, a, b):
    a_kind, a_name = conn.execute(
        "SELECT kind, name FROM nodes WHERE node_id=?", (a,)).fetchone()
    b_kind, b_name = conn.execute(
        "SELECT kind, name FROM nodes WHERE node_id=?", (b,)).fetchone()
    if kind == 1:  # 时效别名边（实体 -- 实体）
        link_id, vf, vt = conn.execute(
            "SELECT link_id, valid_from, valid_to FROM alias_edges "
            "WHERE edge_id=?", (edge_id,)).fetchone()
        left_name = _entity_name(conn, a)
        right_name = _entity_name(conn, b)
        return {"type": "alias_link", "link_id": link_id,
                "left_entity": left_name, "right_entity": right_name,
                "valid_from": vf, "valid_to": vt}
    if {a_kind, b_kind} == {"example", "entity"}:
        ex = a_name if a_kind == "example" else b_name
        ent = b_name if b_kind == "entity" else a_name
        return {"type": "same_entity", "example_id": ex, "entity": ent}
    # 来路跳（样本 -- 事实）：给出该端实际用到的特征与修订
    ex = a_name if a_kind == "example" else b_name
    fact = b_name if b_kind == "fact" else a_name
    refs = conn.execute(
        "SELECT feature_id, revision, event_time, is_latest "
        "FROM provenance_refs WHERE example_id=? AND fact_id=? "
        "ORDER BY feature_id, revision", (ex, fact)).fetchall()
    return {"type": "provenance", "example_id": ex, "fact_id": fact,
            "used_records": [
                {"feature_id": r[0], "revision": r[1],
                 "event_time": r[2], "is_latest": r[3]} for r in refs]}


def _load_graph_from_db(conn, n_nodes):
    """从切分库重建紧凑 CSR；回查不依赖原装配库或输入文件。"""
    static_csr = build_csr_streaming(
        n_nodes,
        lambda: ((r[0], r[1]) for r in conn.execute(
            "SELECT a_node, b_node FROM static_edges ORDER BY edge_id")))
    alias_csr = build_csr_streaming(
        n_nodes,
        lambda: ((r[0], r[1], r[2]) for r in conn.execute(
            "SELECT left_node, right_node, edge_id FROM alias_edges "
            "ORDER BY edge_id")),
        with_edge_ids=True)
    alias_from = array('q', (r[0] for r in conn.execute(
        "SELECT valid_from FROM alias_edges ORDER BY edge_id")))
    alias_to = array('q', (r[0] for r in conn.execute(
        "SELECT valid_to FROM alias_edges ORDER BY edge_id")))
    return static_csr, alias_csr, alias_from, alias_to


def _entity_name(conn, node_id):
    return conn.execute(
        "SELECT name FROM nodes WHERE node_id=? AND kind='entity'",
        (node_id,)).fetchone()[0]


# ============================ 来源核对 ============================

def verify_source(db_path, *, assembly_db=None, fold_plan_csv=None,
                  alias_csv=None):
    """重算来源指纹/文件哈希并与切分库记录比对。

    换一份装配库（即使路径相同）内容指纹不同，无法冒充旧清单来源。
    """
    conn = open_split_readonly(db_path)
    try:
        stored_fp = conn.execute(
            "SELECT value FROM meta WHERE key='source_fingerprint'"
        ).fetchone()[0]
        result = {"source_fingerprint": stored_fp, "checks": []}
        if assembly_db is not None:
            src = open_source_readonly(assembly_db)
            try:
                validate_source(src)
                actual_fp = source_fingerprint(src)
            finally:
                src.close()
            result["checks"].append({
                "role": "assembly", "path": assembly_db,
                "match": actual_fp == stored_fp,
                "expected": stored_fp, "actual": actual_fp})
        for role, path in (("fold_plan", fold_plan_csv),
                           ("alias_links", alias_csv)):
            if path is None:
                continue
            actual = sha256_file(path)
            rows = conn.execute(
                "SELECT sha256 FROM input_files WHERE role=?",
                (role,)).fetchall()
            ok = bool(rows) and any(r[0] == actual for r in rows)
            result["checks"].append({
                "role": role, "path": path, "match": ok,
                "actual": actual, "expected": [r[0] for r in rows]})
        result["ok"] = (all(c["match"] for c in result["checks"])
                        and assembly_db is not None)
        return result
    finally:
        conn.close()


def _require_fold(conn, fold_id):
    row = conn.execute("SELECT 1 FROM folds WHERE fold_id=?",
                       (fold_id,)).fetchone()
    if row is None:
        raise SplitError(f"未知折 fold_id={fold_id}")
