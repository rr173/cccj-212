#!/usr/bin/env python3
"""题面主演练的可复跑样例。

从空目录构建 data/walkthrough/ 下的全部 CSV，运行装配器，
并用独立手算 oracle 对账 E1/E2 的期望值，最后打印逐样本解释。

运行：python3 -I examples/run_walkthrough.py
"""

import csv
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from assembler import assemble, explain_example  # noqa: E402
from assembler.store import connect  # noqa: E402
from acceptance.manual_oracle import oracle_feature, oracle_label  # noqa: E402

WORK = os.path.join(ROOT, "data", "walkthrough")


def write_csv(name, header, rows):
    with open(os.path.join(WORK, name), "w", encoding="utf-8",
              newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def main():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)

    write_csv("anchors.csv",
              ["example_id", "entity", "cutoff"],
              [{"example_id": "E1", "entity": "A", "cutoff": 10},
               {"example_id": "E2", "entity": "A", "cutoff": 13}])
    write_csv("feature_defs.csv",
              ["feature_id", "category", "window", "op", "horizon"],
              [{"feature_id": "fx_count", "category": "X", "window": 5,
                "op": "count", "horizon": ""},
               {"feature_id": "fx_sum", "category": "X", "window": 5,
                "op": "sum", "horizon": ""},
               {"feature_id": "fx_latest", "category": "X", "window": 5,
                "op": "latest", "horizon": ""}])
    write_csv(
        "facts.csv",
        ["fact_id", "revision", "event_time", "available_time",
         "entity", "category", "value", "op"],
        [
            # F rev1 -> rev2：rev2 改 event_time 到 12
            {"fact_id": "F", "revision": 1, "event_time": 8,
             "available_time": 8, "entity": "A", "category": "X",
             "value": 7, "op": "upsert"},
            {"fact_id": "F", "revision": 2, "event_time": 12,
             "available_time": 12, "entity": "A", "category": "X",
             "value": 9, "op": "upsert"},
            # G：available=11，E1 不可见；值为 null
            {"fact_id": "G", "revision": 1, "event_time": 9,
             "available_time": 11, "entity": "A", "category": "X",
             "value": "", "op": "upsert"},
            # H：event=7 在 E1 窗口内，但 available=14，必须排除
            {"fact_id": "H", "revision": 1, "event_time": 7,
             "available_time": 14, "entity": "A", "category": "X",
             "value": 100, "op": "upsert"},
        ])
    write_csv("outcomes.csv",
              ["outcome_id", "entity", "event_time", "available_time"],
              [{"outcome_id": "O1", "entity": "A", "event_time": 15,
                "available_time": 15}])
    # 第一阶段只给 [11,15)，缺 [15,16) => pending；脚本随后补齐 => positive
    write_csv("coverage.csv",
              ["entity", "a", "b", "received_at"],
              [{"entity": "A", "a": 11, "b": 15, "received_at": 15}])

    paths = {n: os.path.join(WORK, f"{n}.csv")
             for n in ("anchors", "feature_defs", "facts",
                       "outcomes", "coverage")}
    db_path = os.path.join(WORK, "assembly.db")

    print("== 第一阶段：覆盖 [11,15) 缺 15 ==")
    assemble(db_path, anchors_csv=paths["anchors"],
             feature_defs_csv=paths["feature_defs"],
             facts_csv=paths["facts"], outcomes_csv=paths["outcomes"],
             coverage_csv=paths["coverage"], horizon=5, as_of_k=20)
    conn = connect(db_path)
    expected = {
        ("E1", "fx_count"): 1, ("E1", "fx_sum"): 7,
        ("E1", "fx_latest"): 7,
        ("E2", "fx_count"): 2, ("E2", "fx_sum"): 9,
        ("E2", "fx_latest"): 9,
    }
    for (example, feature), want in expected.items():
        got = int(conn.execute(
            "SELECT value_text FROM feature_values "
            "WHERE example_id=? AND feature_id=?",
            (example, feature)).fetchone()[0])
        assert got == want, f"{example}/{feature}: 期望 {want}，实际 {got}"
    status = conn.execute(
        "SELECT status FROM labels WHERE example_id='E1'").fetchone()[0]
    assert status == "pending", status
    kept = conn.execute(
        "SELECT ref_id FROM label_evidence "
        "WHERE example_id='E1' AND kind='positive_outcome'").fetchall()
    assert kept == [("O1",)], kept
    print("  E1 特征 count/sum/latest = 1/7/7；E2 = 2/9/9（rev1 未残留）")
    print("  E1 标签 = pending，已观察结局 O1 单独保留：", kept)
    conn.close()

    print("== 第二阶段：补齐覆盖 [15,16) ==")
    write_csv("coverage.csv",
              ["entity", "a", "b", "received_at"],
              [{"entity": "A", "a": 11, "b": 15, "received_at": 15},
               {"entity": "A", "a": 15, "b": 16, "received_at": 16}])
    assemble(db_path, anchors_csv=paths["anchors"],
             feature_defs_csv=paths["feature_defs"],
             facts_csv=paths["facts"], outcomes_csv=paths["outcomes"],
             coverage_csv=paths["coverage"], horizon=5, as_of_k=20)
    conn = connect(db_path)
    status = conn.execute(
        "SELECT status FROM labels WHERE example_id='E1'").fetchone()[0]
    assert status == "positive", status
    print("  E1 标签 = positive（覆盖闭合且结局在 (10,15] 内）")

    # 用独立 oracle 再对一遍（不调用生产版本选择/窗口索引）
    facts = [
        {"fact_id": "F", "revision": 1, "event_time": 8,
         "available_time": 8, "entity": "A", "category": "X",
         "value": 7, "op": "upsert"},
        {"fact_id": "F", "revision": 2, "event_time": 12,
         "available_time": 12, "entity": "A", "category": "X",
         "value": 9, "op": "upsert"},
        {"fact_id": "G", "revision": 1, "event_time": 9,
         "available_time": 11, "entity": "A", "category": "X",
         "value": None, "op": "upsert"},
        {"fact_id": "H", "revision": 1, "event_time": 7,
         "available_time": 14, "entity": "A", "category": "X",
         "value": 100, "op": "upsert"},
    ]
    for anchor in (("E1", "A", 10), ("E2", "A", 13)):
        for definition in (
                {"feature_id": "fx_count", "category": "X",
                 "window": 5, "op": "count"},
                {"feature_id": "fx_sum", "category": "X",
                 "window": 5, "op": "sum"},
                {"feature_id": "fx_latest", "category": "X",
                 "window": 5, "op": "latest"}):
            o_value, o_status, _ = oracle_feature(anchor, definition, facts)
            db_value = conn.execute(
                "SELECT value_text FROM feature_values "
                "WHERE example_id=? AND feature_id=?",
                (anchor[0], definition["feature_id"])).fetchone()[0]
            db_value = None if db_value is None else int(db_value)
            assert db_value == o_value, (anchor, definition, db_value, o_value)
    print("  独立手算 oracle 对账全部一致")

    print("\n== E1 逐样本解释（回查入口）==")
    report = explain_example(conn, "E1")
    for feature in report["features"]:
        print(f"  {feature['feature_id']}: value={feature['value']} "
              f"status={feature['status']} — {feature['note']}")
    print(f"  标签: {report['label'][0]}，证据条数: "
          f"{len(report['label_evidence'])}")
    conn.close()
    print(f"\n装配库位于 {db_path}（大资料目录 data/ 不入库）")


if __name__ == "__main__":
    main()
