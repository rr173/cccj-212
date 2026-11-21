"""装配库的只读接入与完整性校验。

切分器绝不写入来源库：统一以 ``mode=ro`` 打开，SQLite 层面即拒绝任何
写操作。以下情况一律拒绝切分（会导致错误分组/错误时间隔离）：

1. 库未完成装配（``meta.build_status != 'complete'``）；
2. 缺少 ``labels`` 行或标签状态不合法；
3. ``feature_provenance`` 中存在找不到归属样本的行；
4. 锚点与标签的 cutoff / horizon 对不上，或关键表/列缺失。
"""

import os
import sqlite3

from .errors import SplitError

REQUIRED_TABLES = ("meta", "anchors", "feature_provenance", "labels")


def open_source_readonly(db_path):
    """以严格只读模式打开装配库；文件不存在或无法识别时拒绝。"""
    if not os.path.isfile(db_path):
        raise SplitError(f"装配库不存在: {db_path}")
    uri = "file:" + os.path.abspath(db_path).replace("?", "%3f") + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError as exc:
        raise SplitError(f"无法只读打开装配库 {db_path}: {exc}") from exc
    conn.execute("PRAGMA query_only = ON")
    return conn


def validate_source(conn):
    """对只读装配库做完整性校验，返回构建绑定信息字典。"""
    names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = [t for t in REQUIRED_TABLES if t not in names]
    if missing:
        raise SplitError(f"装配库缺少必要表: {', '.join(missing)}")

    status = conn.execute(
        "SELECT value FROM meta WHERE key='build_status'").fetchone()
    if status is None or status[0] != "complete":
        raise SplitError("该装配库未完成装配（build_status 不是 complete），"
                         "拒绝切分")

    n_anchors = conn.execute("SELECT COUNT(*) FROM anchors").fetchone()[0]
    if n_anchors == 0:
        raise SplitError("装配库 anchors 为空，拒绝切分")
    n_labels = conn.execute("SELECT COUNT(*) FROM labels").fetchone()[0]
    if n_labels != n_anchors:
        raise SplitError(
            f"标签不完整：锚点 {n_anchors} 个，labels 仅 {n_labels} 行")

    bad_status = conn.execute(
        "SELECT status FROM labels "
        "WHERE status NOT IN ('positive', 'negative', 'pending') LIMIT 1"
    ).fetchone()
    if bad_status is not None:
        raise SplitError(f"labels 出现未知状态: {bad_status[0]}")

    # 锚点与标签必须逐样本对得上（cutoff/horizon 一致），否则观察末点不可信
    mismatch = conn.execute(
        """
        SELECT a.example_id
        FROM anchors a
        JOIN labels l ON l.example_id = a.example_id
        WHERE a.cutoff != l.cutoff OR l.horizon <= 0
        LIMIT 1
        """
    ).fetchone()
    if mismatch is not None:
        raise SplitError(
            f"labels 与 anchors 不一致或 horizon 非法: "
            f"example_id={mismatch[0]}")
    orphan_label = conn.execute(
        """
        SELECT 1 FROM labels l
        WHERE NOT EXISTS (SELECT 1 FROM anchors a
                          WHERE a.example_id = l.example_id)
        LIMIT 1
        """
    ).fetchone()
    if orphan_label is not None:
        raise SplitError("labels 中存在 anchors 找不到归属的样本")

    # 来路行必须能找到归属样本（题目明确要求拒绝这种输入）
    orphan = conn.execute(
        """
        SELECT p.example_id FROM feature_provenance p
        WHERE NOT EXISTS (
            SELECT 1 FROM anchors a WHERE a.example_id = p.example_id)
        LIMIT 1
        """
    ).fetchone()
    if orphan is not None:
        raise SplitError(
            f"feature_provenance 存在找不到归属样本的行: "
            f"example_id={orphan[0]}")

    binding = {
        "input_hash": _meta(conn, "input_hash"),
        "horizon": _meta(conn, "horizon"),
        "as_of_k": _meta(conn, "as_of_k"),
        "n_anchors": n_anchors,
    }
    return binding


def _meta(conn, key):
    row = conn.execute("SELECT value FROM meta WHERE key=?",
                       (key,)).fetchone()
    return row[0] if row else None


def source_fingerprint(conn):
    """计算切分实际读到内容的指纹。

    指纹覆盖 anchors / labels / feature_provenance 的全部行（按确定性
    排序逐行喂哈希），以及 meta 绑定值。即使原路径被换成另一份库，
    指纹不同也无法冒充旧清单的来源。
    """
    import hashlib
    digest = hashlib.sha256()

    def feed(sql):
        for row in conn.execute(sql):
            digest.update(repr(tuple(row)).encode("utf-8"))
            digest.update(b"\n")

    feed("SELECT key, value FROM meta ORDER BY key")
    feed("SELECT example_id, entity, cutoff FROM anchors "
         "ORDER BY example_id")
    feed("SELECT example_id, entity, cutoff, horizon, obs_a, obs_b, status "
         "FROM labels ORDER BY example_id")
    # 来路按全列排序：同一 (example, fact) 的多特征/修订行都参与指纹
    feed("SELECT example_id, feature_id, fact_id, revision, event_time, "
         "is_latest FROM feature_provenance "
         "ORDER BY example_id, feature_id, fact_id, revision")
    return digest.hexdigest()
