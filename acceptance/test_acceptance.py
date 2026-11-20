"""验收程序：小资料直接枚举，逐锚点/逐 K 与手算 oracle 对账。

运行：
  python3 -I acceptance/test_acceptance.py

本测试的断言分两层：
  1. 生产装配结果必须与 acceptance.manual_oracle 的独立手算逐字段一致；
  2. 针对撤回、改实体、移出窗口、边界、覆盖断裂等情形的显式断言。
"""

import csv
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from assembler import assemble, explain_example  # noqa: E402
from assembler.errors import AssemblyError  # noqa: E402
from assembler.store import NULL_SENTINEL, connect  # noqa: E402
from acceptance.manual_oracle import (INT64_MAX, INT64_MIN, oracle_feature,
                                      oracle_label)  # noqa: E402

HEADERS = {
    "anchors": ["example_id", "entity", "cutoff"],
    "feature_defs": ["feature_id", "category", "window", "op", "horizon"],
    "facts": ["fact_id", "revision", "event_time", "available_time",
              "entity", "category", "value", "op"],
    "outcomes": ["outcome_id", "entity", "event_time", "available_time"],
    "coverage": ["entity", "a", "b", "received_at"],
}


def write_csv(path, kind, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADERS[kind])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class CaseBuilder:
    """在独立空目录中准备一组小资料并可反复装配。"""

    def __init__(self, root):
        self.root = root
        self.anchors = []
        self.defs = []
        self.facts = []
        self.outcomes = []
        self.coverage = []

    def paths(self, tag=""):
        return {kind: os.path.join(self.root, f"{kind}{tag}.csv")
                for kind in HEADERS}

    def write(self, tag=""):
        paths = self.paths(tag)
        write_csv(paths["anchors"], "anchors", self.anchors)
        write_csv(paths["feature_defs"], "feature_defs", self.defs)
        write_csv(paths["facts"], "facts", self.facts)
        write_csv(paths["outcomes"], "outcomes", self.outcomes)
        write_csv(paths["coverage"], "coverage", self.coverage)
        return paths

    def run(self, horizon, as_of_k, tag="", expect_failure=False):
        paths = self.write(tag)
        db_path = os.path.join(self.root, f"build{tag}.db")
        if expect_failure:
            try:
                assemble(db_path, anchors_csv=paths["anchors"],
                         feature_defs_csv=paths["feature_defs"],
                         facts_csv=paths["facts"],
                         outcomes_csv=paths["outcomes"],
                         coverage_csv=paths["coverage"],
                         horizon=horizon, as_of_k=as_of_k)
            except AssemblyError:
                pass
            else:
                raise AssertionError("预期装配失败，但实际成功了")
            assert not os.path.exists(db_path), "失败时半成品库必须被删除"
            return None
        info = assemble(db_path, anchors_csv=paths["anchors"],
                        feature_defs_csv=paths["feature_defs"],
                        facts_csv=paths["facts"],
                        outcomes_csv=paths["outcomes"],
                        coverage_csv=paths["coverage"],
                        horizon=horizon, as_of_k=as_of_k)
        conn = connect(db_path)
        return conn, info


def parse_db_value(text):
    return None if text is None else int(text)


class AcceptanceTest(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="acceptance_")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _oracle_facts(self):
        out = []
        for f in self.builder.facts:
            out.append({
                "fact_id": f["fact_id"],
                "revision": int(f["revision"]),
                "event_time": int(f["event_time"]),
                "available_time": int(f["available_time"]),
                "entity": f["entity"],
                "category": f["category"],
                "value": None if f["value"] == "" else int(f["value"]),
                "op": f["op"],
            })
        return out

    def _oracle_anchors(self):
        return [(a["example_id"], a["entity"], int(a["cutoff"]))
                for a in self.builder.anchors]

    def _oracle_defs(self):
        return [{"feature_id": d["feature_id"], "category": d["category"],
                 "window": int(d["window"]), "op": d["op"]}
                for d in self.builder.defs]

    def assert_features_match_oracle(self, conn):
        facts = self._oracle_facts()
        for anchor in self._oracle_anchors():
            for definition in self._oracle_defs():
                o_value, o_status, o_prov = oracle_feature(
                    anchor, definition, facts)
                example_id = anchor[0]
                db_value = parse_db_value(conn.execute(
                    "SELECT value_text FROM feature_values "
                    "WHERE example_id=? AND feature_id=?",
                    (example_id, definition["feature_id"])).fetchone()[0])
                db_status = conn.execute(
                    "SELECT status FROM feature_explanations "
                    "WHERE example_id=? AND feature_id=?",
                    (example_id, definition["feature_id"])).fetchone()[0]
                self.assertEqual(
                    db_value, o_value,
                    f"{example_id}/{definition['feature_id']} 值不一致")
                self.assertEqual(
                    db_status, o_status,
                    f"{example_id}/{definition['feature_id']} 状态不一致: "
                    f"db={db_status} oracle={o_status}")
                db_prov = {(r[0], r[1]) for r in conn.execute(
                    "SELECT fact_id, revision FROM feature_provenance "
                    "WHERE example_id=? AND feature_id=?",
                    (example_id, definition["feature_id"]))}
                self.assertEqual(db_prov, o_prov,
                                 f"{example_id} 来路修订与手算不一致")

    def assert_labels_match_oracle(self, conn, horizon, as_of_k):
        outcomes = [{"outcome_id": o["outcome_id"], "entity": o["entity"],
                     "event_time": int(o["event_time"]),
                     "available_time": int(o["available_time"])}
                    for o in self.builder.outcomes]
        coverage = [{"entity": c["entity"], "a": int(c["a"]),
                     "b": int(c["b"]), "received_at": int(c["received_at"])}
                    for c in self.builder.coverage]
        for anchor in self._oracle_anchors():
            o_status, o_ids, o_gaps, _ = oracle_label(
                anchor, outcomes, coverage, horizon, as_of_k)
            db_status = conn.execute(
                "SELECT status FROM labels WHERE example_id=?",
                (anchor[0],)).fetchone()[0]
            self.assertEqual(db_status, o_status)
            db_ids = [r[0] for r in conn.execute(
                "SELECT ref_id FROM label_evidence "
                "WHERE example_id=? AND kind='positive_outcome'",
                (anchor[0],))]
            self.assertEqual(sorted(db_ids), sorted(o_ids))
            db_gaps = [(r[0], r[1]) for r in conn.execute(
                "SELECT a, b FROM label_evidence "
                "WHERE example_id=? AND kind='coverage_gap' "
                "ORDER BY a", (anchor[0],))]
            self.assertEqual(sorted(db_gaps), sorted(o_gaps))

    # ---- 1. 题面主演练 ----
    def test_main_walkthrough(self):
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "E1", "entity": "A", "cutoff": 10},
            {"example_id": "E2", "entity": "A", "cutoff": 13},
        ]
        for op in ("count", "sum", "latest"):
            b.defs.append({"feature_id": f"f_{op}", "category": "X",
                           "window": 5, "op": op, "horizon": ""})
        b.facts = [
            # F：rev1 在 E1 窗口，rev2 改 event_time 到 12，E2 生效
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 8, "entity": "A", "category": "X",
             "value": 7, "op": "upsert"},
            {"fact_id": "F", "revision": 2, "event_time": 12,
             "available_time": 12, "entity": "A", "category": "X",
             "value": 9, "op": "upsert"},
            # G：available=11，E1(cutoff=10) 不可见，E2 可见，值为 null
            {"fact_id": "G", "revision": 1, "event_time": 9,
             "available_time": 11, "entity": "A", "category": "X",
             "value": "", "op": "upsert"},
            # H：event=7 在 E1 窗口内，但 available=14 晚于 cutoff，必须排除
            {"fact_id": "H", "revision": 1, "event_time": 7,
             "available_time": 14, "entity": "A", "category": "X",
             "value": 100, "op": "upsert"},
        ]
        b.outcomes = [
            {"outcome_id": "O1", "entity": "A", "event_time": 15,
             "available_time": 15},
        ]
        # 覆盖缺 [15,16)：E1/E2 的观察区间都未闭合
        b.coverage = [
            {"entity": "A", "a": 11, "b": 15, "received_at": 15},
        ]
        self.builder = b
        conn, _ = b.run(horizon=5, as_of_k=20)
        try:
            def val(example, feature):
                return parse_db_value(conn.execute(
                    "SELECT value_text FROM feature_values "
                    "WHERE example_id=? AND feature_id=?",
                    (example, feature)).fetchone()[0])

            # E1: 只有 F rev1（G 未到、H 未到、F rev2 未到）
            self.assertEqual(val("E1", "f_count"), 1)
            self.assertEqual(val("E1", "f_sum"), 7)
            self.assertEqual(val("E1", "f_latest"), 7)
            # E2: F rev2（event=12 在窗口 [8,13)）+ G（null），rev1 不残留
            self.assertEqual(val("E2", "f_count"), 2)
            self.assertEqual(val("E2", "f_sum"), 9)   # 仅 F=9，G 为 null
            self.assertEqual(val("E2", "f_latest"), 9)
            # rev1 不得残留在 E2 来路
            prov = dict(conn.execute(
                "SELECT fact_id, revision FROM feature_provenance "
                "WHERE example_id='E2'"))
            self.assertEqual(prov, {"F": 2, "G": 1})
            # G 全空：E2 的 sum 状态必须区别于 count=0
            self.assertEqual(conn.execute(
                "SELECT status FROM feature_explanations "
                "WHERE example_id='E2' AND feature_id='f_sum'"
            ).fetchone()[0], "OK")
            # 覆盖未闭合：即使 O1 已观察到，仍 pending 且保留结局信息
            self.assertEqual(conn.execute(
                "SELECT status FROM labels WHERE example_id='E1'"
            ).fetchone()[0], "pending")
            kept = conn.execute(
                "SELECT ref_id FROM label_evidence "
                "WHERE example_id='E1' AND kind='positive_outcome'"
            ).fetchall()
            self.assertEqual(kept, [("O1",)])
            self.assert_features_match_oracle(conn)
            self.assert_labels_match_oracle(conn, 5, 20)

            # 补上 [15,16)：覆盖闭合后正例成立
            b.coverage.append(
                {"entity": "A", "a": 15, "b": 16, "received_at": 16})
            conn.close()
            conn2, _ = b.run(horizon=5, as_of_k=20, tag="_closed")
            self.assertEqual(conn2.execute(
                "SELECT status FROM labels WHERE example_id='E1'"
            ).fetchone()[0], "positive")
            # 另一实体 B 覆盖齐全且无结局 -> 合法负例
            b.anchors.append(
                {"example_id": "E3", "entity": "B", "cutoff": 10})
            b.coverage.append(
                {"entity": "B", "a": 11, "b": 16, "received_at": 16})
            conn2.close()
            conn3, _ = b.run(horizon=5, as_of_k=20, tag="_neg")
            self.assertEqual(conn3.execute(
                "SELECT status FROM labels WHERE example_id='E3'"
            ).fetchone()[0], "negative")
            self.assert_labels_match_oracle(conn3, 5, 20)
            conn3.close()
        finally:
            conn.close()

    # ---- 2. 撤回使 ID 不可用 ----
    def test_retract(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 20,
                   "op": "count", "horizon": ""}]
        b.facts = [
            {"fact_id": "F", "revision": 1, "event_time": 5,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
            {"fact_id": "F", "revision": 2, "event_time": 5,
             "available_time": 9, "entity": "A", "category": "X",
             "value": "", "op": "retract"},
        ]
        self.builder = b
        conn, _ = b.run(horizon=3, as_of_k=20)
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values WHERE feature_id='c'"
        ).fetchone()[0]), 0)
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM feature_provenance").fetchone()[0], 0)
        self.assert_features_match_oracle(conn)
        conn.close()

    # ---- 3. 新修订改实体：旧实体上消失，新实体上出现 ----
    def test_revision_changes_entity(self):
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "EA", "entity": "A", "cutoff": 10},
            {"example_id": "EB", "entity": "B", "cutoff": 10},
        ]
        b.defs = [{"feature_id": "c", "category": "X", "window": 20,
                   "op": "count", "horizon": ""}]
        b.facts = [
            {"fact_id": "F", "revision": 1, "event_time": 5,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
            {"fact_id": "F", "revision": 2, "event_time": 5,
             "available_time": 8, "entity": "B", "category": "X",
             "value": 1, "op": "upsert"},
        ]
        self.builder = b
        conn, _ = b.run(horizon=3, as_of_k=20)
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values WHERE example_id='EA'"
        ).fetchone()[0]), 0)
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values WHERE example_id='EB'"
        ).fetchone()[0]), 1)
        self.assert_features_match_oracle(conn)
        conn.close()

    # ---- 4. 版本把事件时间移出窗口 ----
    def test_reversion_moves_event_out_of_window(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        b.facts = [
            # rev1 event=8 在 [5,10)；rev2 改到 event=4 移出窗口
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
            {"fact_id": "F", "revision": 2, "event_time": 4,
             "available_time": 9, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
        ]
        self.builder = b
        conn, _ = b.run(horizon=3, as_of_k=20)
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values").fetchone()[0]), 0)
        self.assert_features_match_oracle(conn)
        conn.close()

    # ---- 5. 半开边界：下界包含、上界排除 ----
    def test_half_open_boundaries(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        b.facts = [
            # event=5 == 下界，包含
            {"fact_id": "LO", "revision": 1, "event_time": 5,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
            # event=10 == cutoff，排除
            {"fact_id": "HI", "revision": 1, "event_time": 10,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
        ]
        self.builder = b
        conn, _ = b.run(horizon=3, as_of_k=20)
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values").fetchone()[0]), 1)
        self.assert_features_match_oracle(conn)
        conn.close()

    # ---- 6. 低修订晚到不反向改变已选高修订 ----
    def test_late_low_revision_keeps_high(self):
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "E1", "entity": "A", "cutoff": 10},
            {"example_id": "E2", "entity": "A", "cutoff": 20},
        ]
        b.defs = [{"feature_id": "v", "category": "X", "window": 100,
                   "op": "latest", "horizon": ""}]
        b.facts = [
            # rev2 先到（available=9），rev1 晚到（available=15）
            {"fact_id": "F", "revision": 2, "event_time": 8,
             "available_time": 9, "entity": "A", "category": "X",
             "value": 20, "op": "upsert"},
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 15, "entity": "A", "category": "X",
             "value": 10, "op": "upsert"},
        ]
        self.builder = b
        conn, _ = b.run(horizon=3, as_of_k=30)
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values WHERE example_id='E2'"
        ).fetchone()[0]), 20)
        rev = conn.execute(
            "SELECT revision FROM feature_provenance WHERE example_id='E2'"
        ).fetchone()[0]
        self.assertEqual(rev, 2)
        self.assert_features_match_oracle(conn)
        conn.close()

    # ---- 7. 全空数值：三种空解释互不相同 ----
    def test_all_null_values_distinct_explanations(self):
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "EMPTY", "entity": "A", "cutoff": 10},
            {"example_id": "NULLS", "entity": "A", "cutoff": 10},
        ]
        b.defs = [
            {"feature_id": "cnt", "category": "X", "window": 5,
             "op": "count", "horizon": ""},
            {"feature_id": "sm", "category": "X", "window": 5,
             "op": "sum", "horizon": ""},
            {"feature_id": "lt", "category": "X", "window": 5,
             "op": "latest", "horizon": ""},
        ]
        b.facts = [
            {"fact_id": "N1", "revision": 1, "event_time": 8,
             "available_time": 5, "entity": "A", "category": "X",
             "value": "", "op": "upsert"},
            {"fact_id": "N2", "revision": 1, "event_time": 9,
             "available_time": 5, "entity": "A", "category": "X",
             "value": "", "op": "upsert"},
        ]
        self.builder = b
        conn, _ = b.run(horizon=3, as_of_k=20)
        # EMPTY 实体不同：用实体 Z 制造无事实锚点
        b.anchors.append(
            {"example_id": "EMPTY2", "entity": "Z", "cutoff": 10})
        conn.close()
        conn, _ = b.run(horizon=3, as_of_k=20, tag="_z")
        statuses = {(r[0], r[1]): r[2] for r in conn.execute(
            "SELECT example_id, feature_id, status "
            "FROM feature_explanations")}
        self.assertEqual(statuses[("EMPTY2", "cnt")], "COUNT_ZERO")
        self.assertEqual(statuses[("EMPTY2", "sm")], "NO_ELIGIBLE_FACT")
        self.assertEqual(statuses[("EMPTY2", "lt")], "LATEST_NO_ROW")
        self.assertEqual(statuses[("NULLS", "sm")], "SUM_ALL_NULL")
        self.assertEqual(statuses[("NULLS", "lt")], "OK")  # latest 保留 null
        self.assertIsNone(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values "
            "WHERE example_id='NULLS' AND feature_id='lt'").fetchone()[0]))
        self.assert_features_match_oracle(conn)
        conn.close()

    # ---- 8. 极端整数：int64 边界、精确求和、端点溢出 ----
    def test_extreme_integers_and_exact_sum(self):
        b = CaseBuilder(self.root)
        # cutoff 取安全值，保证标签观察区间端点不溢出；
        # cutoff 本身取 int64 极值的溢出情形在本用例末尾单独验证
        b.anchors = [
            {"example_id": "E1", "entity": "A", "cutoff": 10 ** 12},
        ]
        b.defs = [
            {"feature_id": "sm", "category": "X", "window": 100,
             "op": "sum", "horizon": ""},
            {"feature_id": "lt", "category": "X", "window": 100,
             "op": "latest", "horizon": ""},
        ]
        b.facts = [
            {"fact_id": "P", "revision": 1,
             "event_time": 10 ** 12 - 1, "available_time": 1,
             "entity": "A", "category": "X",
             "value": INT64_MAX, "op": "upsert"},
            {"fact_id": "Q", "revision": 1,
             "event_time": 10 ** 12 - 2, "available_time": 1,
             "entity": "A", "category": "X",
             "value": INT64_MIN, "op": "upsert"},
        ]
        b.coverage = [
            {"entity": "A", "a": 10 ** 12 + 1,
             "b": 10 ** 12 + 2, "received_at": 10 ** 12}]
        self.builder = b
        conn, _ = b.run(horizon=1, as_of_k=10 ** 12 + 5)
        # 精确求和：INT64_MAX + INT64_MIN = -1（Python 无界，无 float）
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values WHERE feature_id='sm'"
        ).fetchone()[0]), -1)
        # latest 保留 INT64_MAX
        self.assertEqual(parse_db_value(conn.execute(
            "SELECT value_text FROM feature_values WHERE feature_id='lt'"
        ).fetchone()[0]), INT64_MAX)
        self.assert_features_match_oracle(conn)
        conn.close()

        # 窗口端点相减溢出：cutoff=INT64_MIN, window 为正 => 下界越界
        b2 = CaseBuilder(self.root)
        b2.anchors = [{"example_id": "X", "entity": "A",
                       "cutoff": INT64_MIN}]
        b2.defs = [{"feature_id": "c", "category": "X", "window": 1,
                    "op": "count", "horizon": ""}]
        b2.facts = []
        self.builder = b2
        b2.run(horizon=1, as_of_k=INT64_MAX, expect_failure=True)

        # 标签端点相加溢出：cutoff=INT64_MAX 时 cutoff+1 越界
        b3 = CaseBuilder(self.root)
        b3.anchors = [{"example_id": "X", "entity": "A",
                       "cutoff": INT64_MAX}]
        b3.defs = [{"feature_id": "c", "category": "X", "window": 1,
                    "op": "count", "horizon": ""}]
        b3.facts = []
        b3.coverage = []
        self.builder = b3
        b3.run(horizon=1, as_of_k=INT64_MAX, expect_failure=True)

    # ---- 9. 同 ID/revision 异内容拒绝 ----
    def test_conflicting_revision_rejected(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        b.facts = [
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"},
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 2, "op": "upsert"},  # value 不同
        ]
        self.builder = b
        b.run(horizon=3, as_of_k=20, expect_failure=True)

    # ---- 10. 重复锚点 example_id 拒绝；同实体多锚点允许 ----
    def test_duplicate_anchor_rejected_many_allowed(self):
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "E1", "entity": "A", "cutoff": 10},
            {"example_id": "E1", "entity": "A", "cutoff": 12},
        ]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        self.builder = b
        b.run(horizon=3, as_of_k=20, expect_failure=True)

        b.anchors = [
            {"example_id": "E1", "entity": "A", "cutoff": 10},
            {"example_id": "E2", "entity": "A", "cutoff": 12},
            {"example_id": "E3", "entity": "A", "cutoff": 14},
        ]
        conn, _ = b.run(horizon=3, as_of_k=20, tag="_ok")
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM anchors").fetchone()[0], 3)
        conn.close()

    # ---- 11. 覆盖断裂、相邻拼接、迟到覆盖（received_at>K 不可用）----
    def test_coverage_break_and_adjacent_merge(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        # 观察区间 [11, 16)
        b.coverage = [
            {"entity": "A", "a": 11, "b": 13, "received_at": 12},
            {"entity": "A", "a": 14, "b": 16, "received_at": 12},  # 13..14 断裂
        ]
        self.builder = b
        conn, _ = b.run(horizon=5, as_of_k=20)
        self.assertEqual(conn.execute(
            "SELECT status FROM labels").fetchone()[0], "pending")
        gaps = conn.execute(
            "SELECT a, b FROM label_evidence WHERE kind='coverage_gap'"
        ).fetchall()
        self.assertEqual(gaps, [(13, 14)])
        self.assert_labels_match_oracle(conn, 5, 20)
        conn.close()

        # 相邻拼接：[11,13)+[13,16) 闭合，无结局 => negative
        b.coverage = [
            {"entity": "A", "a": 11, "b": 13, "received_at": 12},
            {"entity": "A", "a": 13, "b": 16, "received_at": 12},
        ]
        conn, _ = b.run(horizon=5, as_of_k=20, tag="_merged")
        self.assertEqual(conn.execute(
            "SELECT status FROM labels").fetchone()[0], "negative")
        self.assert_labels_match_oracle(conn, 5, 20)
        conn.close()

        # 覆盖行 received_at>K 必须不可用 => 回到 pending
        b.coverage = [
            {"entity": "A", "a": 11, "b": 16, "received_at": 21},
        ]
        conn, _ = b.run(horizon=5, as_of_k=20, tag="_late")
        self.assertEqual(conn.execute(
            "SELECT status FROM labels").fetchone()[0], "pending")
        self.assert_labels_match_oracle(conn, 5, 20)
        conn.close()

    # ---- 12. 结局 available_time>K 不可用；无结局行不等于负例 ----
    def test_outcome_after_k_and_no_outcome_needs_coverage(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        b.outcomes = [
            {"outcome_id": "O_LATE", "entity": "A", "event_time": 12,
             "available_time": 25},  # K=20 时不可用
        ]
        # 无任何覆盖 => 即使没有“可见结局行”，也必须 pending，不是 negative
        self.builder = b
        conn, _ = b.run(horizon=5, as_of_k=20)
        self.assertEqual(conn.execute(
            "SELECT status FROM labels").fetchone()[0], "pending")
        self.assert_labels_match_oracle(conn, 5, 20)
        conn.close()

    # ---- 13. 非法 H 与 K 早于 cutoff ----
    def test_invalid_horizon_and_k_before_cutoff(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        self.builder = b
        b.run(horizon=0, as_of_k=20, expect_failure=True)
        b.run(horizon=-3, as_of_k=20, expect_failure=True)
        b.run(horizon=5, as_of_k=9, expect_failure=True)

    # ---- 14. 输入置换：行序打乱结果完全一致 ----
    def test_input_permutation_invariant(self):
        import random
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "E1", "entity": "A", "cutoff": 10},
            {"example_id": "E2", "entity": "A", "cutoff": 13},
            {"example_id": "E3", "entity": "B", "cutoff": 10},
        ]
        b.defs = [
            {"feature_id": "cnt", "category": "X", "window": 6,
             "op": "count", "horizon": ""},
            {"feature_id": "sm", "category": "X", "window": 6,
             "op": "sum", "horizon": ""},
            {"feature_id": "lt", "category": "X", "window": 6,
             "op": "latest", "horizon": ""},
        ]
        rng = random.Random(42)
        for i in range(20):
            b.facts.append({
                "fact_id": f"F{i % 7}", "revision": 1 + i // 7,
                "event_time": rng.randint(5, 14),
                "available_time": rng.randint(1, 14),
                "entity": rng.choice(["A", "B"]),
                "category": "X",
                "value": "" if rng.random() < 0.3 else rng.randint(-50, 50),
                "op": "upsert"})
        b.outcomes = [
            {"outcome_id": "O1", "entity": "A", "event_time": 12,
             "available_time": 12},
        ]
        b.coverage = [
            {"entity": "A", "a": 11, "b": 14, "received_at": 13},
            {"entity": "A", "a": 14, "b": 16, "received_at": 15},
            {"entity": "B", "a": 11, "b": 16, "received_at": 14},
        ]
        self.builder = b
        # 同字节重复构建必须得到相同哈希与相同快照（构建可复现）。
        # 注意：哈希绑定文件名+字节，复现必须复用同一路径集合
        conn0, info0 = b.run(horizon=5, as_of_k=20, tag="_a")
        snapshot0 = _snapshot(conn0)
        conn0.close()
        conn1, info1 = b.run(horizon=5, as_of_k=20, tag="_a")
        snapshot1 = _snapshot(conn1)
        self.assertEqual(snapshot1, snapshot0)
        self.assertEqual(info1["input_hash"], info0["input_hash"])
        conn1.close()

        shuffled = CaseBuilder(self.root)
        shuffled.anchors = b.anchors[:]
        shuffled.defs = b.defs[:]
        shuffled.facts = b.facts[:]
        shuffled.outcomes = b.outcomes[:]
        shuffled.coverage = b.coverage[:]
        rng.shuffle(shuffled.facts)
        rng.shuffle(shuffled.coverage)
        rng.shuffle(shuffled.anchors)
        self.builder = shuffled
        conn2, info2 = shuffled.run(horizon=5, as_of_k=20, tag="_b")
        snapshot2 = _snapshot(conn2)
        conn2.close()
        self.assertEqual(snapshot1, snapshot2)
        # 哈希绑定的是文件字节：行序变化后字节不同，哈希理应不同；
        # 内容等价性已由上面的全产物逐行快照相等证明
        self.assertNotEqual(info1["input_hash"], info2["input_hash"])

    # ---- 15. 特征来路与标签来路严格分表，且 explain 可分页回查 ----
    def test_provenance_separation_and_explain(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 20,
                   "op": "count", "horizon": ""}]
        b.facts = [
            {"fact_id": "F", "revision": 1, "event_time": 5,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 7, "op": "upsert"}]
        b.outcomes = [
            {"outcome_id": "O1", "entity": "A", "event_time": 12,
             "available_time": 12}]
        b.coverage = [
            {"entity": "A", "a": 11, "b": 16, "received_at": 13}]
        self.builder = b
        conn, _ = b.run(horizon=5, as_of_k=20)
        report = explain_example(conn, "E1", provenance_limit=1)
        self.assertEqual(report["anchor"][0], "E1")
        self.assertEqual(report["label"][0], "positive")
        self.assertEqual(report["features"][0]["provenance_total"], 1)
        self.assertEqual(report["features"][0]["provenance_page"][0][0], "F")
        kinds = {row[1] for row in report["label_evidence"]}
        self.assertIn("positive_outcome", kinds)
        self.assertIn("coverage_interval", kinds)
        # 特征来路表里绝不出现结局/覆盖实体
        bad = conn.execute(
            "SELECT COUNT(*) FROM feature_provenance "
            "WHERE fact_id IN (SELECT outcome_id FROM outcomes)"
        ).fetchone()[0]
        self.assertEqual(bad, 0)
        with self.assertRaises(AssemblyError):
            explain_example(conn, "NO_SUCH_ID")
        conn.close()

    # ---- 16. 构建绑定：输入变化后哈希变化；失败库不可查询 ----
    def test_build_binding_and_failure_not_delivered(self):
        b = CaseBuilder(self.root)
        b.anchors = [{"example_id": "E1", "entity": "A", "cutoff": 10}]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        b.facts = [
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 5, "entity": "A", "category": "X",
             "value": 1, "op": "upsert"}]
        self.builder = b
        conn, info1 = b.run(horizon=3, as_of_k=20)
        conn.close()
        b.facts[0]["value"] = 2
        conn, info2 = b.run(horizon=3, as_of_k=20, tag="_changed")
        conn.close()
        self.assertNotEqual(info1["input_hash"], info2["input_hash"])

    # ---- 17. 标签区间半开：cutoff 时刻结局不计，cutoff+H 时刻计入 ----
    def test_label_observation_endpoints(self):
        b = CaseBuilder(self.root)
        b.anchors = [
            {"example_id": "E0", "entity": "A", "cutoff": 10},
        ]
        b.defs = [{"feature_id": "c", "category": "X", "window": 5,
                   "op": "count", "horizon": ""}]
        b.coverage = [
            {"entity": "A", "a": 11, "b": 16, "received_at": 12}]
        # event=10 == cutoff：区间 (cutoff, cutoff+H] 不含
        b.outcomes = [
            {"outcome_id": "AT_CUTOFF", "entity": "A", "event_time": 10,
             "available_time": 10},
            {"outcome_id": "AT_END", "entity": "A", "event_time": 15,
             "available_time": 15},  # == cutoff+H，包含
        ]
        self.builder = b
        conn, _ = b.run(horizon=5, as_of_k=20)
        self.assertEqual(conn.execute(
            "SELECT status FROM labels").fetchone()[0], "positive")
        ids = sorted(r[0] for r in conn.execute(
            "SELECT ref_id FROM label_evidence "
            "WHERE kind='positive_outcome'"))
        self.assertEqual(ids, ["AT_END"])
        self.assert_labels_match_oracle(conn, 5, 20)
        conn.close()


def _snapshot(conn):
    """导出全部产物表的排序内容，用于置换不变性比较。"""
    out = {}
    for table in ("feature_values", "feature_explanations",
                  "feature_provenance", "labels", "label_evidence"):
        rows = conn.execute(
            f"SELECT * FROM {table} ORDER BY 1, 2, 3").fetchall()
        out[table] = rows
    return out


if __name__ == "__main__":
    unittest.main(verbosity=2)
