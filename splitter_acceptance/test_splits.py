"""多折切分器验收：小资料逐折枚举，与独立 oracle 全面对账。

运行：
  python3 -I splitter_acceptance/test_splits.py

覆盖点（对应任务要求）：
  1. T(rev1)/M(rev2) 共享事实 + B-C 时效边，link_at=99/100/199/200
     时 T 依次 train/related_eval/related_eval/train，路径必须经过 M，
     且两个修订是两条真实来路跳而不是被合并的伪造单跳；
  2. B-X-C 替代路径：直连过期后部分时间仍挡住 T，替代路径也过期才放行；
  3. embargo 数值边界（cutoff=12 可训练、cutoff=13 隔离），
     且 embargo 优先于 related_eval；
  4. pending 验证对手改作唯一中间点：不失去对手，只换对手；
  5. 同名异类编号、多个可选对手（选字节序最小）、平行边生效端点、
     折序打乱与重复 link_at、没有可用验证样本的折；
  6. 非法输入（重复 link_id、空区间、计划越界/范围非法）拒绝；
  7. 失败后旧切分库仍可查询；未完成/损坏来源库拒绝；
  8. 输入行序/折执行序置换不变性，重开产物结果一致；
  9. 未知折/未知样本不冒充 outside。
"""

import csv
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from assembler import assemble  # noqa: E402
from splitter import (SplitError, build_splits, export_examples, fold_stats,
                      lookup_example, verify_source)  # noqa: E402
from splitter.inputs import load_alias_links, load_fold_plan  # noqa: E402
from splitter_acceptance.manual_oracle import (  # noqa: E402
    oracle_min_opponent, oracle_states)

A_HEADERS = ["example_id", "entity", "cutoff"]
D_HEADERS = ["feature_id", "category", "window", "op", "horizon"]
F_HEADERS = ["fact_id", "revision", "event_time", "available_time",
             "entity", "category", "value", "op"]
O_HEADERS = ["outcome_id", "entity", "event_time", "available_time"]
C_HEADERS = ["entity", "a", "b", "received_at"]
PLAN_HEADERS = ["fold_id", "link_at", "train_from", "train_to",
                "eval_from", "eval_to", "gap"]
LINK_HEADERS = ["link_id", "left_entity", "right_entity",
                "valid_from", "valid_to"]


def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


class Fixture:
    """准备一组装配输入 + 计划 + 别名，并能构建装配库与切分库。"""

    def __init__(self, root):
        self.root = root
        self.anchors = []
        self.defs = [{"feature_id": "f1", "category": "X", "window": 1000,
                      "op": "count", "horizon": ""}]
        self.facts = []
        self.outcomes = []
        self.coverage = []
        self.folds = []
        self.links = []
        self.horizon = 5
        self.k = 1000

    def add_anchor(self, example_id, entity, cutoff, closed=True,
                   positive=False):
        self.anchors.append({"example_id": example_id, "entity": entity,
                             "cutoff": cutoff})
        if closed:
            a, b = cutoff + 1, cutoff + self.horizon + 1
            self.coverage.append(
                {"entity": entity, "a": a, "b": b, "received_at": b})
            if positive:
                self.outcomes.append({
                    "outcome_id": f"O_{example_id}", "entity": entity,
                    "event_time": cutoff + 1,
                    "available_time": cutoff + 1})

    def add_fact(self, fact_id, revision, entity, event_time,
                 available_time=None, value=1):
        self.facts.append({
            "fact_id": fact_id, "revision": revision,
            "event_time": event_time,
            "available_time": (event_time if available_time is None
                               else available_time),
            "entity": entity, "category": "X", "value": value,
            "op": "upsert"})

    def add_fold(self, fold_id, link_at, train=(0, 20), eval_=(20, 30),
                 gap=0):
        self.folds.append({
            "fold_id": fold_id, "link_at": link_at,
            "train_from": train[0], "train_to": train[1],
            "eval_from": eval_[0], "eval_to": eval_[1], "gap": gap})

    def add_link(self, link_id, left, right, vf, vt):
        self.links.append({"link_id": link_id, "left_entity": left,
                           "right_entity": right,
                           "valid_from": vf, "valid_to": vt})

    def _write_inputs(self, tag=""):
        p = lambda n: os.path.join(self.root, f"{n}{tag}.csv")
        write_csv(p("anchors"), A_HEADERS, self.anchors)
        write_csv(p("feature_defs"), D_HEADERS, self.defs)
        write_csv(p("facts"), F_HEADERS, self.facts)
        write_csv(p("outcomes"), O_HEADERS, self.outcomes)
        write_csv(p("coverage"), C_HEADERS, self.coverage)
        write_csv(p("folds"), PLAN_HEADERS, self.folds)
        write_csv(p("links"), LINK_HEADERS, self.links)
        return p

    def build(self, tag="", shuffle=False):
        p = self._write_inputs(tag)
        assembly_db = os.path.join(self.root, f"asm{tag}.db")
        split_db = os.path.join(self.root, f"split{tag}.db")
        assemble(assembly_db, anchors_csv=p("anchors"),
                 feature_defs_csv=p("feature_defs"),
                 facts_csv=p("facts"), outcomes_csv=p("outcomes"),
                 coverage_csv=p("coverage"),
                 horizon=self.horizon, as_of_k=self.k)
        if shuffle:
            import random
            rng = random.Random(7)
            rng.shuffle(self.links)
            rng.shuffle(self.folds)
            p2 = self._write_inputs(tag + "_shuf")
            plan_csv, link_csv = p2("folds"), p2("links")
        else:
            plan_csv, link_csv = p("folds"), p("links")
        info = build_splits(split_db, assembly_db=assembly_db,
                            fold_plan_csv=plan_csv, alias_csv=link_csv)
        return assembly_db, split_db, info

    def raw_inputs(self, assembly_db=None):
        """返回 oracle 输入；标签状态直接读装配库，避免夹具侧猜测。"""
        import sqlite3
        anchors = [(a["example_id"], a["entity"], a["cutoff"])
                   for a in self.anchors]
        if assembly_db is None:
            raise ValueError("raw_inputs 需要 assembly_db 以读取真实标签")
        conn = sqlite3.connect(f"file:{assembly_db}?mode=ro", uri=True)
        try:
            labels = dict(conn.execute(
                "SELECT example_id, status FROM labels"))
        finally:
            conn.close()
        return anchors, labels


def prov_pairs_from_db(assembly_db):
    import sqlite3
    conn = sqlite3.connect(f"file:{assembly_db}?mode=ro", uri=True)
    pairs = set(conn.execute(
        "SELECT DISTINCT example_id, fact_id FROM feature_provenance"))
    conn.close()
    return pairs


def conn_node_kinds(split_db):
    import sqlite3
    conn = sqlite3.connect(f"file:{split_db}?mode=ro", uri=True)
    rows = conn.execute("SELECT kind, name, node_id FROM nodes").fetchall()
    conn.close()
    return [((k, n), nid) for k, n, nid in rows]


class SplitAcceptanceTest(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="splitacc_")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _states(self, split_db, fold_id):
        stats = fold_stats(split_db, fold_id)
        return stats

    def _assert_oracle(self, fx, split_db, assembly_db):
        """对每折做全样本状态对账 + related_eval 对手与路径对账。"""
        import sqlite3
        anchors, labels = fx.raw_inputs(assembly_db)
        pairs = prov_pairs_from_db(assembly_db)
        conn = sqlite3.connect(f"file:{split_db}?mode=ro", uri=True)
        try:
            for fold in fx.folds:
                expected = oracle_states(
                    anchors, labels, fx.horizon, pairs, fx.links, fold)
                got = dict(conn.execute(
                    "SELECT example_id, state FROM assignments "
                    "WHERE fold_id=?", (fold["fold_id"],)).fetchall())
                self.assertEqual(got, expected,
                                 f"折 {fold['fold_id']} 状态与 oracle 不符")
                # 对账每个 related_eval 的对手与真实路径
                for eid, state in expected.items():
                    if state != "related_eval":
                        continue
                    opp, path = oracle_min_opponent(
                        anchors, labels, fx.horizon, pairs, fx.links,
                        fold, eid)
                    report = lookup_example(split_db, fold["fold_id"], eid)
                    self.assertEqual(report["reason"]["opponent_id"], opp)
                    got_path = [n["id"] for n in
                                report["reason"]["path"]["nodes"]]
                    self.assertEqual(got_path, [n[1] for n in path])
        finally:
            conn.close()

    # ---- 1. 题面主场景：rev1/rev2 共享事实 + 时效边四端点 ----
    def test_shared_fact_revisions_and_temporal_edge(self):
        fx = Fixture(self.root)
        # T@A(cutoff=5, 训练), M@B(cutoff=40, 折外), V@C(cutoff=22, 验证)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("M", "B", 40)
        fx.add_anchor("V", "C", 22)
        # F 的两个修订分别被 T 与 M 引用（不同实体、不同修订）
        fx.add_fact("F", 1, "A", event_time=4)
        fx.add_fact("F", 2, "B", event_time=30)
        # B-C 边只在 [100,200) 生效
        fx.add_link("L_BC", "B", "C", 100, 200)
        for link_at in (99, 100, 199, 200):
            fx.add_fold(f"f{link_at}", link_at)
        assembly_db, split_db, info = fx.build()
        expected = {99: "train", 100: "related_eval",
                    199: "related_eval", 200: "train"}
        for link_at, want in expected.items():
            report = lookup_example(split_db, f"f{link_at}", "T")
            self.assertEqual(report["state"], want,
                             f"link_at={link_at} 时期望 {want}")
        # 中间两折路径必须经过折外样本 M，且包含两个修订的真实来路
        for link_at in (100, 199):
            report = lookup_example(split_db, f"f{link_at}", "T")
            path = report["reason"]["path"]
            node_ids = [n["id"] for n in path["nodes"]]
            self.assertEqual(node_ids[0], "T")
            self.assertEqual(node_ids[-1], "V")
            self.assertIn("M", node_ids)
            types = [h["type"] for h in path["hops"]]
            self.assertIn("alias_link", types)
            # 两段来路跳分别点名 rev1（T 端）与 rev2（M 端）
            prov_hops = [h for h in path["hops"] if h["type"] ==
                         "provenance"]
            revs = sorted((h["example_id"],
                           tuple(sorted(r["revision"] for r in
                                        h["used_records"])))
                          for h in prov_hops)
            self.assertIn(("T", (1,)), revs)
            self.assertIn(("M", (2,)), revs)
            # 没有任何一跳把两个修订塞进同一条来路
            for h in prov_hops:
                self.assertEqual(len({r["revision"] for r in
                                      h["used_records"]}), 1)
        # V 在四折都是 eval；M 在四折都是 outside 但承担中间点
        for link_at in (99, 100, 199, 200):
            self.assertEqual(lookup_example(
                split_db, f"f{link_at}", "V")["state"], "eval")
            self.assertEqual(lookup_example(
                split_db, f"f{link_at}", "M")["state"], "outside")
        self._assert_oracle(fx, split_db, assembly_db)

    # ---- 2. 替代路径：直连过期后仍连通，替代路径也过期才放行 ----
    def test_alternative_path_keeps_block_then_releases(self):
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("M", "B", 40)
        fx.add_anchor("V", "C", 22)
        fx.add_fact("F", 1, "A", 4)
        fx.add_fact("F", 2, "B", 30)
        fx.add_link("L_BC", "B", "C", 100, 200)      # 直连
        # 替代路径 B-X-C：两段区间只部分相交
        fx.add_link("L_BX", "B", "X", 150, 300)
        fx.add_link("L_XC", "X", "C", 150, 250)
        for link_at in (99, 100, 149, 150, 199, 200, 249, 250, 299):
            fx.add_fold(f"f{link_at}", link_at)
        _, split_db, _ = fx.build()
        # 直连与替代都不活跃 -> 放行
        self.assertEqual(lookup_example(split_db, "f99", "T")["state"],
                         "train")
        # 仅直连活跃
        for t in (100, 149):
            self.assertEqual(
                lookup_example(split_db, f"f{t}", "T")["state"],
                "related_eval")
        # 直连+替代并存（150..199）
        for t in (150, 199):
            self.assertEqual(
                lookup_example(split_db, f"f{t}", "T")["state"],
                "related_eval")
        # 直连过期，替代路径仍挡住 T（200..249）
        for t in (200, 249):
            report = lookup_example(split_db, f"f{t}", "T")
            self.assertEqual(report["state"], "related_eval")
            node_ids = [n["id"] for n in
                        report["reason"]["path"]["nodes"]]
            self.assertIn("X", node_ids)
        # 替代路径也过期（XC 在 250 失效），放行
        for t in (250, 299):
            self.assertEqual(
                lookup_example(split_db, f"f{t}", "T")["state"], "train")

    # ---- 3. embargo 边界与优先级 ----
    def test_embargo_boundary_and_precedence(self):
        fx = Fixture(self.root)
        # 训练 [0,14)，验证 [20,25)，gap=2，horizon=5
        # 界线 eval_from-gap = 18；obs_end = cutoff+5
        # cutoff=12 -> obs_end=17 < 18 可训练；cutoff=13 -> 18 >= 18 隔离
        fx.add_anchor("T12", "A", 12)
        fx.add_anchor("T13", "B", 13)
        fx.add_anchor("V", "C", 22)
        fx.add_fold("f0", 100, train=(0, 14), eval_=(20, 25), gap=2)
        _, split_db, _ = fx.build()
        r12 = lookup_example(split_db, "f0", "T12")
        r13 = lookup_example(split_db, "f0", "T13")
        self.assertEqual(r12["state"], "train")
        self.assertEqual(r13["state"], "embargo")
        self.assertEqual(r13["reason"]["obs_end"], 18)
        self.assertEqual(r13["reason"]["embargo_line"], 18)

        # 让 T13 经共享事实连到真实验证样本，仍必须报告 embargo
        fx.facts = []
        fx.add_fact("G", 1, "B", 12)
        fx.add_fact("G", 2, "C", 21)
        _, split_db2, _ = fx.build(tag="_rel")
        r13b = lookup_example(split_db2, "f0", "T13")
        self.assertEqual(r13b["state"], "embargo")
        self.assertNotIn("path", r13b["reason"])

    # ---- 4. pending 验证候选是唯一中间点：换对手而非失去对手 ----
    def test_pending_intermediate_switches_opponent(self):
        fx = Fixture(self.root)
        # T@A 训练；P@B 验证范围内但 pending；V@C 验证范围内且闭合。
        # 链路 T --F1-- P --F2-- V，P 是通向 V 的唯一中间点。
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("P", "B", 22, closed=False)   # pending
        fx.add_anchor("V", "C", 23, closed=True)
        # F1: rev1 在 A（T 窗口 event<5），rev2 在 B（P 窗口 event<22）
        fx.add_fact("F1", 1, "A", 4)
        fx.add_fact("F1", 2, "B", 20)
        # F2: rev1 在 B（P 窗口，available=21<=cutoff22）；
        # rev2 改到 C 且 available=23，P(cutoff=22) 不可见、V(cutoff=23) 可见
        fx.add_fact("F2", 1, "B", 21, available_time=21)
        fx.add_fact("F2", 2, "C", 22, available_time=23)
        fx.add_fold("f0", 100)
        _, split_db, _ = fx.build()
        # P 是 pending：不是对手，但 T-F1-P-F2-V 仍连通，对手换成 V
        self.assertEqual(lookup_example(split_db, "f0", "P")["state"],
                         "pending")
        report = lookup_example(split_db, "f0", "T")
        self.assertEqual(report["state"], "related_eval")
        self.assertEqual(report["reason"]["opponent_id"], "V")
        node_ids = [n["id"] for n in
                    report["reason"]["path"]["nodes"]]
        self.assertIn("P", node_ids)
        # 若没有 V，只剩 pending 候选，则 T 应放行（无对手）
        fx.anchors = [a for a in fx.anchors if a["example_id"] != "V"]
        fx.coverage = [c for c in fx.coverage if c["entity"] != "C"]
        _, split_db2, _ = fx.build(tag="_noopp")
        self.assertEqual(lookup_example(split_db2, "f0", "T")["state"],
                         "train")

    # ---- 5. 同名异类编号不混淆；多对手选字节序最小 ----
    def test_namespace_collision_and_min_opponent(self):
        fx = Fixture(self.root)
        # 样本与事实同名 "Z"，实体也叫 "Z"，三类命名空间必须隔离
        fx.add_anchor("Z", "Z", 5)
        fx.add_anchor("V1", "E1", 22)
        fx.add_anchor("V2", "E2", 23)
        fx.add_fact("Z", 1, "Z", 4)
        # Z 样本通过两条独立关系连到两个 eval，对手取字节序最小 V1
        fx.add_fact("Q1", 1, "Z", 4)
        fx.add_fact("Q1", 2, "E1", 21)
        fx.add_fact("Q2", 1, "Z", 4)
        fx.add_fact("Q2", 2, "E2", 21)
        fx.add_fold("f0", 100)
        _, split_db, _ = fx.build()
        report = lookup_example(split_db, "f0", "Z")
        self.assertEqual(report["state"], "related_eval")
        self.assertEqual(report["reason"]["opponent_id"], "V1")
        # 回查路径节点带 kind；节点登记里同名 "Z" 的三类节点可区分
        all_kinds = set()
        for (kind, name), nid in conn_node_kinds(split_db):
            if name == "Z":
                all_kinds.add((kind, name))
        self.assertEqual(
            all_kinds,
            {("example", "Z"), ("entity", "Z"), ("fact", "Z")})
        # 路径上每个节点也带显式 kind，同名不会被当成同一节点
        path_kinds = {(n["kind"], n["id"]) for n in
                      report["reason"]["path"]["nodes"]}
        self.assertIn(("example", "Z"), path_kinds)
        self.assertTrue(all(k in ("example", "entity", "fact")
                            for k, _ in path_kinds))

    # ---- 6. 平行边生效端点独立判定 ----
    def test_parallel_edges_endpoints(self):
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("V", "C", 22)
        # 两条 A 实体到 C 实体的平行边（经过同实体边作用于样本）
        fx.add_link("L1", "A", "C", 0, 100)    # [0,100)
        fx.add_link("L2", "A", "C", 100, 200)  # [100,200)
        for link_at in (99, 100, 199, 200):
            fx.add_fold(f"f{link_at}", link_at)
        _, split_db, _ = fx.build()
        for link_at in (99, 100, 199):
            self.assertEqual(
                lookup_example(split_db, f"f{link_at}", "T")["state"],
                "related_eval")
        self.assertEqual(lookup_example(split_db, "f200", "T")["state"],
                         "train")
        # link_at=99 时选中的具体 link_id 必须是 L1
        r99 = lookup_example(split_db, "f99", "T")
        alias_hops = [h for h in r99["reason"]["path"]["hops"]
                      if h["type"] == "alias_link"]
        self.assertEqual({h["link_id"] for h in alias_hops}, {"L1"})

    # ---- 7. 折序打乱 + 重复 link_at 的置换不变性 ----
    def test_fold_order_and_duplicate_link_at_invariance(self):
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("M", "B", 40)
        fx.add_anchor("V", "C", 22)
        fx.add_fact("F", 1, "A", 4)
        fx.add_fact("F", 2, "B", 30)
        fx.add_link("L_BC", "B", "C", 100, 200)
        # 两个 fold_id 不同但 link_at 相同（重复 link_at 合法）
        fx.add_fold("z_last", 150)
        fx.add_fold("a_first", 150)
        fx.add_fold("m_mid", 99)
        _, split_db, info_a = fx.build()
        _, split_db_shuf, info_b = fx.build(tag="_shuf", shuffle=True)
        for db in (split_db, split_db_shuf):
            self.assertEqual(
                lookup_example(db, "a_first", "T")["state"],
                "related_eval")
            self.assertEqual(
                lookup_example(db, "z_last", "T")["state"],
                "related_eval")
            self.assertEqual(
                lookup_example(db, "m_mid", "T")["state"], "train")
        # 两次构建的全部 assignment 行逐字节一致
        import sqlite3
        def snap(path):
            c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            rows = c.execute(
                "SELECT fold_id, example_id, state, embargo_line, "
                "opponent_id FROM assignments ORDER BY 1,2").fetchall()
            c.close()
            return rows
        self.assertEqual(snap(split_db), snap(split_db_shuf))
        # 重开产物后回查路径仍一致
        r1 = lookup_example(split_db, "a_first", "T")
        r2 = lookup_example(split_db_shuf, "a_first", "T")
        self.assertEqual(
            [h["type"] for h in r1["reason"]["path"]["hops"]],
            [h["type"] for h in r2["reason"]["path"]["hops"]])

    # ---- 8. 没有可用验证样本的折：全部训练候选放行 ----
    def test_fold_without_eval_samples(self):
        fx = Fixture(self.root)
        fx.add_anchor("T1", "A", 5)
        fx.add_anchor("T2", "B", 6)
        # 验证范围 [20,25) 内没有任何样本
        fx.add_fold("f0", 100, eval_=(20, 25))
        _, split_db, _ = fx.build()
        stats = fold_stats(split_db, "f0")
        self.assertEqual(stats["eval"], 0)
        self.assertEqual(stats["train"], 2)
        self.assertEqual(export_examples(split_db, "f0", "train"),
                         ["T1", "T2"])
        self.assertEqual(export_examples(split_db, "f0", "eval"), [])

    # ---- 9. 非法输入拒绝 ----
    def test_invalid_inputs_rejected(self):
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("V", "C", 22)
        fx.add_link("L1", "A", "C", 0, 100)
        fx.add_fold("f0", 50)

        # 重复 link_id
        bad = Fixture(self.root)
        bad.__dict__.update(fx.__dict__)
        bad.links = fx.links + [
            {"link_id": "L1", "left_entity": "A", "right_entity": "X",
             "valid_from": 0, "valid_to": 10}]
        with self.assertRaises(SplitError):
            bad.build(tag="_dup")

        # 空区间 valid_from >= valid_to
        bad2 = Fixture(self.root)
        bad2.__dict__.update(fx.__dict__)
        bad2.links = [{"link_id": "Lx", "left_entity": "A",
                       "right_entity": "C", "valid_from": 100,
                       "valid_to": 100}]
        with self.assertRaises(SplitError):
            bad2.build(tag="_empty")

        # 计划范围非法：训练范围为空
        bad3 = Fixture(self.root)
        bad3.__dict__.update(fx.__dict__)
        bad3.folds = [{"fold_id": "f", "link_at": 1, "train_from": 10,
                       "train_to": 10, "eval_from": 20, "eval_to": 30,
                       "gap": 0}]
        with self.assertRaises(SplitError):
            bad3.build(tag="_plan")

        # train_to > eval_from
        bad4 = Fixture(self.root)
        bad4.__dict__.update(fx.__dict__)
        bad4.folds = [{"fold_id": "f", "link_at": 1, "train_from": 0,
                       "train_to": 25, "eval_from": 20, "eval_to": 30,
                       "gap": 0}]
        with self.assertRaises(SplitError):
            bad4.build(tag="_overlap")

        # 负 gap
        bad5 = Fixture(self.root)
        bad5.__dict__.update(fx.__dict__)
        bad5.folds = [{"fold_id": "f", "link_at": 1, "train_from": 0,
                       "train_to": 10, "eval_from": 20, "eval_to": 30,
                       "gap": -1}]
        with self.assertRaises(SplitError):
            bad5.build(tag="_gap")

        # 重复 fold_id
        bad6 = Fixture(self.root)
        bad6.__dict__.update(fx.__dict__)
        bad6.folds = fx.folds + fx.folds
        with self.assertRaises(SplitError):
            bad6.build(tag="_folddup")

        # int64 越界
        bad7 = Fixture(self.root)
        bad7.__dict__.update(fx.__dict__)
        bad7.folds = [{"fold_id": "f", "link_at": 2 ** 63,
                       "train_from": 0, "train_to": 10,
                       "eval_from": 20, "eval_to": 30, "gap": 0}]
        with self.assertRaises(SplitError):
            bad7.build(tag="_i64")

        # 端点算术溢出：eval_from - gap 越界
        bad8 = Fixture(self.root)
        bad8.__dict__.update(fx.__dict__)
        bad8.folds = [{"fold_id": "f", "link_at": 1,
                       "train_from": -(2 ** 63), "train_to": -(2 ** 63) + 5,
                       "eval_from": -(2 ** 63) + 10, "eval_to": -(2 ** 63) + 20,
                       "gap": 100}]
        with self.assertRaises(SplitError):
            bad8.build(tag="_overflow")

    # ---- 10. 失败后旧库仍可用；未完成/损坏来源库拒绝；未知项不冒充 outside
    def test_failure_keeps_old_db_and_unknown_rejected(self):
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("V", "C", 22)
        fx.add_link("L1", "A", "C", 0, 100)
        fx.add_fold("f0", 50)
        _, split_db, _ = fx.build()

        # 用一份非法计划重建同名库：失败后旧库仍能查询
        p = fx._write_inputs("_bad")
        bad_plan = os.path.join(self.root, "bad_plan.csv")
        write_csv(bad_plan, PLAN_HEADERS, [
            {"fold_id": "g", "link_at": 1, "train_from": 0,
             "train_to": 0, "eval_from": 20, "eval_to": 30, "gap": 0}])
        with self.assertRaises(SplitError):
            build_splits(split_db, assembly_db=os.path.join(
                self.root, "asm.db"),
                fold_plan_csv=bad_plan,
                alias_csv=p("links"))
        # 旧库完好
        self.assertEqual(fold_stats(split_db, "f0")["eval"], 1)
        self.assertFalse(os.path.exists(split_db + ".building"))

        # 未完成装配库拒绝
        import sqlite3
        incomplete = os.path.join(self.root, "incomplete.db")
        c = sqlite3.connect(incomplete)
        c.executescript(
            "CREATE TABLE anchors (example_id TEXT PRIMARY KEY, entity TEXT, "
            "cutoff INTEGER); CREATE TABLE meta(key TEXT, value TEXT);")
        c.commit()
        c.close()
        with self.assertRaises(SplitError):
            build_splits(os.path.join(self.root, "x.db"),
                         assembly_db=incomplete,
                         fold_plan_csv=p("folds"), alias_csv=p("links"))

        # 未知折/未知样本报错，不冒充 outside
        with self.assertRaises(SplitError):
            lookup_example(split_db, "no_such_fold", "T")
        with self.assertRaises(SplitError):
            lookup_example(split_db, "f0", "no_such_example")

        # outside 样本仍可精确回查与计数
        fx.add_anchor("OUT", "D", 999)
        _, split_db2, _ = fx.build(tag="_out")
        r = lookup_example(split_db2, "f0", "OUT")
        self.assertEqual(r["state"], "outside")
        self.assertEqual(fold_stats(split_db2, "f0")["outside"], 1)

    # ---- 11. 来源核对：换库无法冒充；文件哈希绑定 ----
    def test_verify_source_binding(self):
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_anchor("V", "C", 22)
        fx.add_link("L1", "A", "C", 0, 100)
        fx.add_fold("f0", 50)
        assembly_db, split_db, _ = fx.build()
        p = fx._write_inputs("_v")
        report = verify_source(
            split_db, assembly_db=assembly_db,
            fold_plan_csv=p("folds"), alias_csv=p("links"))
        self.assertTrue(report["ok"])

        # 换一份内容不同的装配库（同路径重建），指纹必须不匹配
        fx.anchors.append({"example_id": "EXTRA", "entity": "Z",
                           "cutoff": 7})
        fx.coverage.append({"entity": "Z", "a": 8, "b": 13,
                            "received_at": 13})
        other_db = os.path.join(self.root, "asm_other.db")
        p2 = fx._write_inputs("_other")
        assemble(other_db, anchors_csv=p2("anchors"),
                 feature_defs_csv=p2("feature_defs"),
                 facts_csv=p2("facts"), outcomes_csv=p2("outcomes"),
                 coverage_csv=p2("coverage"),
                 horizon=fx.horizon, as_of_k=fx.k)
        report2 = verify_source(split_db, assembly_db=other_db)
        self.assertFalse(report2["ok"])

    # ---- 12. 来路孤儿样本拒绝 ----
    def test_orphan_provenance_rejected(self):
        import sqlite3
        fx = Fixture(self.root)
        fx.add_anchor("T", "A", 5)
        fx.add_fold("f0", 50)
        assembly_db, split_db, _ = fx.build()
        # 直接构造一个含孤儿来路的库（模拟损坏的装配产物）
        bad_db = os.path.join(self.root, "orphan.db")
        shutil.copy(assembly_db, bad_db)
        c = sqlite3.connect(bad_db)
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("INSERT INTO feature_provenance VALUES "
                  "('GHOST','f1','FG',1,1,0)")
        c.commit()
        c.close()
        p = fx._write_inputs("_o")
        with self.assertRaises(SplitError):
            build_splits(os.path.join(self.root, "s2.db"),
                         assembly_db=bad_db,
                         fold_plan_csv=p("folds"), alias_csv=p("links"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
