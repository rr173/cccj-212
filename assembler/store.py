"""SQLite 落盘结构与资料导入。

所有输入（锚点、带修订事实、结局、覆盖证据、特征定义）都先落入磁盘库，
后续装配全部基于可重用的落盘索引批量处理，绝不为每个样本重扫事实。

库中既保留原始输入表，也保留装配产物表与构建绑定信息（输入哈希）。
"""

import csv
import hashlib
import os
import sqlite3

from .errors import AssemblyError
from .integers import INT64_MAX, INT64_MIN, check_int64

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 样本锚点
CREATE TABLE anchors (
    example_id TEXT PRIMARY KEY,
    entity     TEXT NOT NULL,
    cutoff     INTEGER NOT NULL
);

-- 特征定义：窗口 [cutoff-window, cutoff)，运算为 count/sum/latest
CREATE TABLE feature_defs (
    feature_id  TEXT PRIMARY KEY,
    category    TEXT NOT NULL,
    window      INTEGER NOT NULL CHECK (window > 0),
    op          TEXT NOT NULL CHECK (op IN ('count', 'sum', 'latest')),
    horizon     INTEGER
);

-- 带修订的行为事实；value 用 TEXT 存控制字符哨兵以区分 NULL
CREATE TABLE facts (
    fact_id        TEXT NOT NULL,
    revision       INTEGER NOT NULL CHECK (revision > 0),
    event_time     INTEGER NOT NULL,
    available_time INTEGER NOT NULL,
    entity         TEXT NOT NULL,
    category       TEXT NOT NULL,
    value_enc      TEXT,
    op             TEXT NOT NULL CHECK (op IN ('upsert', 'retract')),
    PRIMARY KEY (fact_id, revision)
) WITHOUT ROWID;

-- 事实导入暂存表：流式批量写入，去重/冲突检测全部在 SQL 侧完成，
-- Python 进程不持有全量事实（256MiB 预算要求）
CREATE TABLE facts_stage (
    fact_id        TEXT NOT NULL,
    revision       INTEGER NOT NULL,
    event_time     INTEGER NOT NULL,
    available_time INTEGER NOT NULL,
    entity         TEXT NOT NULL,
    category       TEXT NOT NULL,
    value_enc      TEXT,
    op             TEXT NOT NULL
);

-- 不可变结局行
CREATE TABLE outcomes (
    outcome_id     TEXT PRIMARY KEY,
    entity         TEXT NOT NULL,
    event_time     INTEGER NOT NULL,
    available_time INTEGER NOT NULL
) WITHOUT ROWID;

-- 结局接收覆盖证据：实体、区间 [a, b)、接收时刻。
-- 自然键 (entity,a,b,received_at) 确定性生成 coverage_ref，
-- 证据表引用该自然键而非自增 id，保证输入行序不影响产物
CREATE TABLE coverage (
    coverage_id INTEGER PRIMARY KEY,
    entity      TEXT NOT NULL,
    a           INTEGER NOT NULL,
    b           INTEGER NOT NULL CHECK (b > a),
    received_at INTEGER NOT NULL,
    UNIQUE (entity, a, b, received_at)
);

-- ===== 装配产物 =====
CREATE TABLE feature_values (
    example_id TEXT NOT NULL,
    feature_id TEXT NOT NULL,
    value_text TEXT,
    PRIMARY KEY (example_id, feature_id)
) WITHOUT ROWID;

-- 逐样本逐特征解释：count=0 / sum 为 null / latest 缺行使用不同状态
CREATE TABLE feature_explanations (
    example_id        TEXT NOT NULL,
    feature_id        TEXT NOT NULL,
    status            TEXT NOT NULL,
    eligible_fact_cnt INTEGER NOT NULL,
    nonnull_value_cnt INTEGER NOT NULL,
    note              TEXT NOT NULL,
    PRIMARY KEY (example_id, feature_id)
) WITHOUT ROWID;

-- 参与事实的精确修订（独立可分页关系，不塞进一行 JSON）
CREATE TABLE feature_provenance (
    example_id    TEXT NOT NULL,
    feature_id    TEXT NOT NULL,
    fact_id       TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    event_time    INTEGER NOT NULL,
    is_latest     INTEGER NOT NULL CHECK (is_latest IN (0, 1)),
    PRIMARY KEY (example_id, feature_id, fact_id)
) WITHOUT ROWID;

CREATE INDEX idx_prov_paging
    ON feature_provenance (example_id, feature_id, fact_id);

-- 标签表
CREATE TABLE labels (
    example_id TEXT PRIMARY KEY,
    entity     TEXT NOT NULL,
    cutoff     INTEGER NOT NULL,
    horizon    INTEGER NOT NULL,
    obs_a      INTEGER NOT NULL,
    obs_b      INTEGER NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('positive', 'negative', 'pending'))
) WITHOUT ROWID;

-- 标签依据（结局行 / 覆盖区间 / 缺口），与特征来路严格分表
CREATE TABLE label_evidence (
    evidence_id  INTEGER PRIMARY KEY,
    example_id   TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('positive_outcome',
                                               'coverage_interval',
                                               'coverage_gap')),
    ref_id       TEXT,
    a            INTEGER,
    b            INTEGER,
    detail       TEXT
);

CREATE INDEX idx_label_ev ON label_evidence (example_id, evidence_id);
"""

# 用控制字符哨兵编码 NULL，避免与任何合法十进制整数字面量冲突；
# 不使用真实 NUL，因为它不能出现在 executescript 的 SQL 文本中。
NULL_SENTINEL = "\x01NULL\x01"


def connect(db_path):
    """打开（必要时创建）装配库并设置保守的内存参数。"""
    fresh = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA cache_size=-8192")       # 页缓存上限约 8 MiB
    conn.execute("PRAGMA temp_store=FILE")        # 大排序/分组落临时文件
    conn.execute("PRAGMA mmap_size=0")            # 不计入内存预算的 mmap 关闭
    conn.execute("PRAGMA foreign_keys=ON")
    if fresh:
        conn.executescript(SCHEMA)
        conn.commit()
    return conn


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_inputs(paths):
    """对一组输入文件计算稳定的 SHA-256（路径排序后逐文件哈希）。"""
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(os.path.basename(path).encode("utf-8"))
        digest.update(b":")
        digest.update(_sha256_file(path).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _open_csv(path):
    handle = open(path, "r", encoding="utf-8", newline="")
    reader = csv.DictReader(handle)
    if reader.fieldnames is None:
        handle.close()
        raise AssemblyError(f"{path} 是空文件或缺少表头")
    return handle, reader


def _need(row, column, path, lineno):
    if column not in row or row[column] is None or row[column] == "":
        raise AssemblyError(f"{path}:{lineno} 缺少必填列 {column}")
    return row[column]


def _parse_int(raw, what, path, lineno):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise AssemblyError(f"{path}:{lineno} {what}={raw!r} 不是整数")
    if str(value) != str(raw).strip():
        raise AssemblyError(f"{path}:{lineno} {what}={raw!r} 不是合法整数字面量")
    return check_int64(value, f"{path}:{lineno} {what}")


def _parse_optional_int(raw, what, path, lineno):
    if raw is None or raw == "":
        return None
    return _parse_int(raw, what, path, lineno)


def _flush_batch(cursor, sql, buf):
    if not buf:
        return
    try:
        cursor.executemany(sql, buf)
    except sqlite3.IntegrityError as exc:
        raise AssemblyError(f"输入违反唯一性/约束: {exc}") from exc
    buf.clear()


def ingest_anchors(conn, path):
    """导入锚点；example_id 重复（无论内容是否一致）一律拒绝。

    流式读取并按批 flush，Python 侧只持有一个批次。
    """
    handle, reader = _open_csv(path)
    seen = set()
    cursor = conn.cursor()
    buf = []
    try:
        for lineno, row in enumerate(reader, start=2):
            example_id = _need(row, "example_id", path, lineno)
            entity = _need(row, "entity", path, lineno)
            cutoff = _parse_int(row.get("cutoff"), "cutoff", path, lineno)
            if example_id in seen:
                raise AssemblyError(
                    f"{path}:{lineno} 重复锚点 example_id={example_id}")
            seen.add(example_id)
            buf.append((example_id, entity, cutoff))
            if len(buf) >= 5000:
                _flush_batch(cursor,
                             "INSERT INTO anchors VALUES (?, ?, ?)", buf)
        _flush_batch(cursor, "INSERT INTO anchors VALUES (?, ?, ?)", buf)
    finally:
        handle.close()


def ingest_feature_defs(conn, path):
    """导入特征定义；窗口宽度必须为正整数，运算限定三种。"""
    handle, reader = _open_csv(path)
    seen = set()
    cursor = conn.cursor()
    buf = []
    try:
        for lineno, row in enumerate(reader, start=2):
            feature_id = _need(row, "feature_id", path, lineno)
            category = _need(row, "category", path, lineno)
            window = _parse_int(row.get("window"), "window", path, lineno)
            if window <= 0:
                raise AssemblyError(
                    f"{path}:{lineno} window={window} 必须为正整数")
            op = _need(row, "op", path, lineno)
            if op not in ("count", "sum", "latest"):
                raise AssemblyError(
                    f"{path}:{lineno} op={op!r} 不合法，仅支持 count/sum/latest")
            if feature_id in seen:
                raise AssemblyError(
                    f"{path}:{lineno} 重复特征定义 feature_id={feature_id}")
            seen.add(feature_id)
            horizon = _parse_optional_int(row.get("horizon"),
                                          "horizon", path, lineno)
            buf.append((feature_id, category, window, op, horizon))
            if len(buf) >= 5000:
                _flush_batch(
                    cursor,
                    "INSERT INTO feature_defs VALUES (?, ?, ?, ?, ?)", buf)
        _flush_batch(cursor,
                     "INSERT INTO feature_defs VALUES (?, ?, ?, ?, ?)", buf)
    finally:
        handle.close()


def ingest_facts(conn, path):
    """流式导入带修订事实到暂存表。

    不在 Python 侧缓存全量行：逐行读取、按批写入 ``facts_stage``，
    随后由 SQL 完成物理去重、异内容冲突检测与正式表搬运。
    """
    handle, reader = _open_csv(path)
    cursor = conn.cursor()
    buf = []
    try:
        for lineno, row in enumerate(reader, start=2):
            fact_id = _need(row, "fact_id", path, lineno)
            revision = _parse_int(row.get("revision"), "revision", path, lineno)
            if revision <= 0:
                raise AssemblyError(
                    f"{path}:{lineno} revision={revision} 必须为正整数")
            event_time = _parse_int(row.get("event_time"),
                                    "event_time", path, lineno)
            available_time = _parse_int(row.get("available_time"),
                                        "available_time", path, lineno)
            entity = _need(row, "entity", path, lineno)
            category = _need(row, "category", path, lineno)
            value = _parse_optional_int(row.get("value"), "value",
                                        path, lineno)
            op = _need(row, "op", path, lineno)
            if op not in ("upsert", "retract"):
                raise AssemblyError(
                    f"{path}:{lineno} op={op!r} 不合法，仅支持 upsert/retract")
            value_enc = NULL_SENTINEL if value is None else str(value)
            buf.append((fact_id, revision, event_time, available_time,
                        entity, category, value_enc, op))
            if len(buf) >= 5000:
                _flush_batch(
                    cursor,
                    "INSERT INTO facts_stage VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    buf)
        _flush_batch(
            cursor,
            "INSERT INTO facts_stage VALUES (?, ?, ?, ?, ?, ?, ?, ?)", buf)
    finally:
        handle.close()


def reject_fact_conflicts(conn):
    """同 ID/revision 异内容拒绝；无冲突则把去重后的事实搬入正式表。"""
    cur = conn.execute(
        """
        SELECT fact_id, revision, COUNT(*) AS variants
        FROM (
            SELECT DISTINCT fact_id, revision, event_time, available_time,
                            entity, category, value_enc, op
            FROM facts_stage
        )
        GROUP BY fact_id, revision
        HAVING variants > 1
        LIMIT 1
        """
    )
    hit = cur.fetchone()
    if hit is not None:
        raise AssemblyError(
            f"同一 fact_id/revision 存在异内容行，拒绝装配: "
            f"fact_id={hit[0]}, revision={hit[1]}")
    # 物理完全相同的重复行由 DISTINCT 折叠后再进正式表
    conn.execute(
        """
        INSERT INTO facts
        SELECT DISTINCT fact_id, revision, event_time, available_time,
                        entity, category, value_enc, op
        FROM facts_stage
        """
    )
    conn.execute("DROP TABLE facts_stage")


def ingest_outcomes(conn, path):
    """流式导入不可变结局；outcome_id 重复且异内容拒绝。"""
    handle, reader = _open_csv(path)
    seen = {}
    cursor = conn.cursor()
    buf = []
    try:
        for lineno, row in enumerate(reader, start=2):
            outcome_id = _need(row, "outcome_id", path, lineno)
            entity = _need(row, "entity", path, lineno)
            event_time = _parse_int(row.get("event_time"),
                                    "event_time", path, lineno)
            available_time = _parse_int(row.get("available_time"),
                                        "available_time", path, lineno)
            if outcome_id in seen and seen[outcome_id] != (
                    entity, event_time, available_time):
                raise AssemblyError(
                    f"{path}:{lineno} 结局 outcome_id={outcome_id} "
                    f"重复但内容不一致（结局不可变）")
            seen[outcome_id] = (entity, event_time, available_time)
            buf.append((outcome_id, entity, event_time, available_time))
            if len(buf) >= 5000:
                _flush_batch(
                    cursor,
                    "INSERT OR IGNORE INTO outcomes VALUES (?, ?, ?, ?)",
                    buf)
        _flush_batch(cursor,
                     "INSERT OR IGNORE INTO outcomes VALUES (?, ?, ?, ?)",
                     buf)
    finally:
        handle.close()


def ingest_coverage(conn, path):
    """流式导入覆盖证据；要求半开区间 a < b，自然键重复幂等忽略。"""
    handle, reader = _open_csv(path)
    cursor = conn.cursor()
    buf = []
    try:
        for lineno, row in enumerate(reader, start=2):
            entity = _need(row, "entity", path, lineno)
            a = _parse_int(row.get("a"), "a", path, lineno)
            b = _parse_int(row.get("b"), "b", path, lineno)
            received_at = _parse_int(row.get("received_at"),
                                     "received_at", path, lineno)
            if not (a < b):
                raise AssemblyError(
                    f"{path}:{lineno} 覆盖区间非法，要求 a < b: [{a}, {b})")
            buf.append((entity, a, b, received_at))
            if len(buf) >= 5000:
                _flush_batch(
                    cursor,
                    "INSERT OR IGNORE INTO coverage"
                    "(entity, a, b, received_at) VALUES (?, ?, ?, ?)", buf)
        _flush_batch(cursor,
                     "INSERT OR IGNORE INTO coverage"
                     "(entity, a, b, received_at) VALUES (?, ?, ?, ?)", buf)
    finally:
        handle.close()


def validate_ranges(conn):
    """跨表的最终边界校验。"""
    for table, column in (
        ("anchors", "cutoff"),
        ("facts", "event_time"), ("facts", "available_time"),
        ("outcomes", "event_time"), ("outcomes", "available_time"),
        ("coverage", "a"), ("coverage", "b"), ("coverage", "received_at"),
    ):
        cur = conn.execute(
            f"SELECT MIN({column}), MAX({column}) FROM {table}")
        lo, hi = cur.fetchone()
        if lo is not None and (lo < INT64_MIN or hi > INT64_MAX):
            raise AssemblyError(f"{table}.{column} 存在超出 int64 的值")
