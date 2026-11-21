"""切分库 SQLite 落盘结构。

切分库是自洽产物：查询/导出/回查只依赖本库，不需要原装配库或输入
CSV 仍在原路径。库内保存：

* 来源绑定（装配库指纹、输入哈希、构建参数）与输入文件自身哈希；
* 节点登记（样本/实体/事实三类，编号分属不同命名空间并以 kind 区分）；
* 去重后的静态关系边（同实体、装配来路）与全部别名时效边；
* 切分计划、每折每样本的状态及状态原因细节；
* 构建状态 ``build_status``，失败库永远不会出现 complete。

不拷贝特征值、结局和原始事实。
"""

import os
import sqlite3

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 来源绑定与输入指纹
CREATE TABLE source_binding (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE input_files (
    role      TEXT NOT NULL,           -- assembly / fold_plan / alias_links
    path_text TEXT NOT NULL,
    sha256    TEXT NOT NULL,
    PRIMARY KEY (role, path_text)
) WITHOUT ROWID;

-- 节点登记：三类 ID 分属不同命名空间，node_id 是切分库内的统一整数编号
CREATE TABLE nodes (
    node_id   INTEGER PRIMARY KEY,
    kind      TEXT NOT NULL CHECK (kind IN ('example', 'entity', 'fact')),
    name      TEXT NOT NULL,           -- example_id / 实体名 / fact_id
    UNIQUE (kind, name)
) WITHOUT ROWID;

-- 样本节点的锚点与标签快照（仅切分必需字段，不拷特征值/结局/原始事实）
CREATE TABLE sample_info (
    example_id  TEXT PRIMARY KEY,
    node_id     INTEGER NOT NULL,
    entity      TEXT NOT NULL,
    cutoff      INTEGER NOT NULL,
    horizon     INTEGER NOT NULL,
    obs_end     INTEGER NOT NULL,      -- cutoff + horizon（含末点）
    label_status TEXT NOT NULL
) WITHOUT ROWID;

-- 静态关系边（去重后）：
--   kind='entity'  样本 -- 同实体
--   kind='fact'    样本 -- 事实（来路，跨修订仍只连同一个事实节点）
CREATE TABLE static_edges (
    edge_id   INTEGER PRIMARY KEY,
    kind      TEXT NOT NULL CHECK (kind IN ('entity', 'fact')),
    a_node    INTEGER NOT NULL,
    b_node    INTEGER NOT NULL
);
CREATE INDEX idx_static_a ON static_edges (a_node, b_node);
CREATE INDEX idx_static_b ON static_edges (b_node, a_node);

-- 来路证据：每条 (样本,事实) 关系两端实际用到的特征与修订记录。
-- 同一事实在同一样本的多个特征/修订里出现，在此逐行保留，回查可点名。
CREATE TABLE provenance_refs (
    ref_id       INTEGER PRIMARY KEY,
    example_id   TEXT NOT NULL,
    feature_id   TEXT NOT NULL,
    fact_id      TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    event_time   INTEGER NOT NULL,
    is_latest    INTEGER NOT NULL
);
CREATE INDEX idx_prov_ref ON provenance_refs (example_id, fact_id, feature_id);

-- 别名时效边（平行边全部保留，独立生效）；edge_id 为库内确定编号，
-- BFS 选中的具体平行边用它回查 link_id 与生效区间
CREATE TABLE alias_edges (
    edge_id     INTEGER PRIMARY KEY,
    link_id     TEXT NOT NULL UNIQUE,
    left_node   INTEGER NOT NULL,     -- 实体节点
    right_node  INTEGER NOT NULL,
    valid_from  INTEGER NOT NULL,
    valid_to    INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX idx_alias_interval
    ON alias_edges (valid_from, valid_to);
CREATE INDEX idx_alias_left ON alias_edges (left_node, right_node);
CREATE INDEX idx_alias_right ON alias_edges (right_node, left_node);

-- 切分计划
CREATE TABLE folds (
    fold_id     TEXT PRIMARY KEY,
    fold_order  INTEGER NOT NULL,      -- 确定性执行序（按 fold_id 字节序）
    link_at     INTEGER NOT NULL,
    train_from  INTEGER NOT NULL,
    train_to    INTEGER NOT NULL,
    eval_from   INTEGER NOT NULL,
    eval_to     INTEGER NOT NULL,
    gap         INTEGER NOT NULL
) WITHOUT ROWID;

-- 每折每样本恰好一行状态。outside 也落库，保证可精确回查与计数。
CREATE TABLE assignments (
    fold_id     TEXT NOT NULL,
    example_id  TEXT NOT NULL,
    state       TEXT NOT NULL CHECK (state IN (
                    'outside', 'pending', 'eval', 'embargo',
                    'related_eval', 'train')),
    cutoff      INTEGER NOT NULL,
    obs_end     INTEGER NOT NULL,
    embargo_line INTEGER,              -- embargo: eval_from - gap
    opponent_id TEXT,                  -- related_eval: 同组件最小 eval 样本
    PRIMARY KEY (fold_id, example_id)
) WITHOUT ROWID;
CREATE INDEX idx_assign_state
    ON assignments (fold_id, state, example_id);
CREATE INDEX idx_assign_example
    ON assignments (example_id, fold_id);
"""

STATE_OUTSIDE = "outside"
STATE_PENDING = "pending"
STATE_EVAL = "eval"
STATE_EMBARGO = "embargo"
STATE_RELATED = "related_eval"
STATE_TRAIN = "train"
ALL_STATES = (STATE_OUTSIDE, STATE_PENDING, STATE_EVAL, STATE_EMBARGO,
              STATE_RELATED, STATE_TRAIN)


def connect_split(db_path):
    fresh = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA cache_size=-16384")   # 页缓存上限约 16 MiB
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA mmap_size=0")
    if fresh:
        conn.executescript(SCHEMA)
        conn.commit()
    return conn


def checkpoint_and_close(conn):
    """把 WAL 全部合并回主库并截断，保证主库文件自洽可单独移动。"""
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


def open_split_readonly(db_path):
    if not os.path.isfile(db_path):
        raise FileNotFoundError(db_path)
    uri = "file:" + os.path.abspath(db_path).replace("?", "%3f") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only = ON")
    status = conn.execute(
        "SELECT value FROM meta WHERE key='build_status'").fetchone()
    if status is None or status[0] != "complete":
        conn.close()
        raise sqlite3.DatabaseError(
            "该切分库未完成构建（build_status 不是 complete），拒绝查询")
    return conn
