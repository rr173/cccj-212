"""装配编排：绑定输入资料与定义，失败不交付完整训练表。

一次 ``assemble`` 调用：

1. 全新库导入全部输入，任一校验失败立刻抛错并删除半成品库；
2. 记录输入文件哈希与装配参数（构建绑定）；
3. 特征使用各锚点自己的 cutoff 选版本；标签使用固定 K，
   两条链路各自独立，绝不共用一个“最新版本”；
4. 全部产物在同一事务内提交。
"""

import os

from .errors import AssemblyError
from .features import build_features
from .labels import build_labels
from .store import (connect, hash_inputs, ingest_anchors, ingest_coverage,
                    ingest_facts, ingest_feature_defs, ingest_outcomes,
                    reject_fact_conflicts, validate_ranges)


def assemble(db_path, *, anchors_csv, feature_defs_csv, facts_csv,
             outcomes_csv, coverage_csv, horizon, as_of_k,
             replace=True):
    """运行完整离线装配，返回构建绑定信息字典。"""
    if replace and os.path.exists(db_path):
        os.remove(db_path)
        for suffix in ("-wal", "-shm"):
            sidecar = db_path + suffix
            if os.path.exists(sidecar):
                os.remove(sidecar)

    inputs = [anchors_csv, feature_defs_csv, facts_csv,
              outcomes_csv, coverage_csv]
    for path in inputs:
        if not os.path.isfile(path):
            raise AssemblyError(f"输入文件不存在: {path}")
    input_hash = hash_inputs(inputs)

    conn = connect(db_path)
    failed = False
    try:
        try:
            # ---- 导入与校验（任何一步失败都不会留下完整产物表）----
            ingest_anchors(conn, anchors_csv)
            ingest_feature_defs(conn, feature_defs_csv)
            ingest_facts(conn, facts_csv)
            reject_fact_conflicts(conn)
            ingest_outcomes(conn, outcomes_csv)
            ingest_coverage(conn, coverage_csv)
            validate_ranges(conn)

            if conn.execute("SELECT COUNT(*) FROM anchors").fetchone()[0] == 0:
                raise AssemblyError("锚点表为空，拒绝装配")
            if conn.execute("SELECT COUNT(*) FROM feature_defs").fetchone()[0] == 0:
                raise AssemblyError("特征定义表为空，拒绝装配")

            # ---- 构建绑定：输入哈希 + 参数 ----
            conn.execute(
                "INSERT INTO meta VALUES ('input_hash', ?)", (input_hash,))
            conn.execute(
                "INSERT INTO meta VALUES ('horizon', ?)", (str(horizon),))
            conn.execute(
                "INSERT INTO meta VALUES ('as_of_k', ?)", (str(as_of_k),))

            # ---- 特征链路：各自 cutoff；标签链路：固定 K，互不共用版本 ----
            build_features(conn)
            build_labels(conn, horizon, as_of_k)

            _assert_complete(conn)
            conn.execute(
                "INSERT INTO meta VALUES ('build_status', 'complete')")
            conn.commit()
        except Exception:
            conn.rollback()
            failed = True
            raise
    finally:
        conn.close()
        if failed:
            # 失败不交付：关闭后删除半成品库，避免被误当成完整训练表
            for path in (db_path, db_path + "-wal", db_path + "-shm"):
                if os.path.exists(path):
                    os.remove(path)

    return {"db_path": db_path, "input_hash": input_hash,
            "horizon": horizon, "as_of_k": as_of_k}


def _assert_complete(conn):
    """产物完整性断言：每个锚点对每个定义都有值、解释和标签。"""
    n_anchors = conn.execute("SELECT COUNT(*) FROM anchors").fetchone()[0]
    n_defs = conn.execute("SELECT COUNT(*) FROM feature_defs").fetchone()[0]
    n_values = conn.execute(
        "SELECT COUNT(*) FROM feature_values").fetchone()[0]
    n_expl = conn.execute(
        "SELECT COUNT(*) FROM feature_explanations").fetchone()[0]
    n_labels = conn.execute("SELECT COUNT(*) FROM labels").fetchone()[0]
    expected = n_anchors * n_defs
    if n_values != expected or n_expl != expected:
        raise AssemblyError(
            f"特征产物不完整：期望 {expected} 行，实际 "
            f"values={n_values}, explanations={n_expl}")
    if n_labels != n_anchors:
        raise AssemblyError(
            f"标签产物不完整：锚点 {n_anchors}，标签 {n_labels}")
    orphan = conn.execute(
        """
        SELECT COUNT(*) FROM feature_provenance p
        WHERE NOT EXISTS (
            SELECT 1 FROM feature_values v
            WHERE v.example_id = p.example_id
              AND v.feature_id = p.feature_id)
        """
    ).fetchone()[0]
    if orphan:
        raise AssemblyError(f"存在 {orphan} 行无主特征来路")


def explain_example(conn, example_id, provenance_limit=100):
    """按 example_id 回查装配依据：锚点、特征、解释、来路、标签与证据。"""
    anchor = conn.execute(
        "SELECT example_id, entity, cutoff FROM anchors "
        "WHERE example_id = ?", (example_id,)).fetchone()
    if anchor is None:
        raise AssemblyError(f"未知 example_id: {example_id}")

    features = []
    for fid, category, window, op in conn.execute(
        """
        SELECT d.feature_id, d.category, d.window, d.op
        FROM feature_defs d ORDER BY d.feature_id
        """
    ):
        value = conn.execute(
            "SELECT value_text FROM feature_values "
            "WHERE example_id = ? AND feature_id = ?",
            (example_id, fid)).fetchone()[0]
        expl = conn.execute(
            "SELECT status, eligible_fact_cnt, nonnull_value_cnt, note "
            "FROM feature_explanations WHERE example_id = ? AND feature_id = ?",
            (example_id, fid)).fetchone()
        prov_total = conn.execute(
            "SELECT COUNT(*) FROM feature_provenance "
            "WHERE example_id = ? AND feature_id = ?",
            (example_id, fid)).fetchone()[0]
        provenance = conn.execute(
            """
            SELECT fact_id, revision, event_time, is_latest
            FROM feature_provenance
            WHERE example_id = ? AND feature_id = ?
            ORDER BY fact_id
            LIMIT ?
            """, (example_id, fid, provenance_limit)).fetchall()
        features.append({
            "feature_id": fid, "category": category, "window": window,
            "op": op, "value": value,
            "status": expl[0], "eligible_fact_cnt": expl[1],
            "nonnull_value_cnt": expl[2], "note": expl[3],
            "provenance_total": prov_total,
            "provenance_page": provenance,
        })

    label = conn.execute(
        "SELECT status, horizon, obs_a, obs_b FROM labels "
        "WHERE example_id = ?", (example_id,)).fetchone()
    evidence = conn.execute(
        "SELECT evidence_id, kind, ref_id, a, b, detail "
        "FROM label_evidence WHERE example_id = ? "
        "ORDER BY evidence_id", (example_id,)).fetchall()

    return {"anchor": anchor, "features": features,
            "label": label, "label_evidence": evidence}
