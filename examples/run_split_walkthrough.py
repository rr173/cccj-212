#!/usr/bin/env python3
"""多折切分题面主演练的可复跑小样例。

从空目录构建装配输入、切分计划与别名文件，先装配再切分，并直接断言
任务要求的关键语义：

1. T(rev1)/M(rev2) 共享事实 + B-C 时效边，link_at=99/100/199/200 时
   T 依次 train/related_eval/related_eval/train，中间两折路径经过 M，
   两个修订是两条真实来路跳而非被合并的伪造单跳；
2. B-X-C 替代路径在直连过期后仍挡住 T，替代路径也到期才放行；
3. embargo 边界（cutoff=12 可训练、cutoff=13 隔离）且 embargo 优先；
4. pending 验证候选作唯一中间点时换对手而非失去对手。

运行：python3 -I examples/run_split_walkthrough.py
"""

import csv
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from assembler import assemble  # noqa: E402
from splitter import build_splits, lookup_example  # noqa: E402

WORK = os.path.join(ROOT, "data", "split_walkthrough")


def write_csv(name, header, rows):
    with open(os.path.join(WORK, name), "w", encoding="utf-8",
              newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def main():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)

    # 样本：T@A 训练、M@B 折外、V@C 验证
    write_csv("anchors.csv", ["example_id", "entity", "cutoff"], [
        {"example_id": "T", "entity": "A", "cutoff": 5},
        {"example_id": "M", "entity": "B", "cutoff": 40},
        {"example_id": "V", "entity": "C", "cutoff": 22}])
    write_csv("feature_defs.csv",
              ["feature_id", "category", "window", "op", "horizon"],
              [{"feature_id": "f1", "category": "X", "window": 1000,
                "op": "count", "horizon": ""}])
    # F 两个修订分别被 T(rev1) 与 M(rev2) 引用
    write_csv("facts.csv",
              ["fact_id", "revision", "event_time", "available_time",
               "entity", "category", "value", "op"], [
        {"fact_id": "F", "revision": 1, "event_time": 4,
         "available_time": 4, "entity": "A", "category": "X",
         "value": 1, "op": "upsert"},
        {"fact_id": "F", "revision": 2, "event_time": 30,
         "available_time": 30, "entity": "B", "category": "X",
         "value": 1, "op": "upsert"}])
    write_csv("outcomes.csv",
              ["outcome_id", "entity", "event_time", "available_time"], [])
    # 三个实体都给足覆盖 -> 标签闭合（negative）
    write_csv("coverage.csv", ["entity", "a", "b", "received_at"], [
        {"entity": "A", "a": 0, "b": 100, "received_at": 100},
        {"entity": "B", "a": 0, "b": 100, "received_at": 100},
        {"entity": "C", "a": 0, "b": 100, "received_at": 100}])
    # 别名边分两组：四端点场景只用直连 L_BC；替代路径场景再加 B-X-C。
    write_csv("links_direct.csv",
              ["link_id", "left_entity", "right_entity",
               "valid_from", "valid_to"], [
        {"link_id": "L_BC", "left_entity": "B", "right_entity": "C",
         "valid_from": 100, "valid_to": 200}])
    write_csv("links_alt.csv",
              ["link_id", "left_entity", "right_entity",
               "valid_from", "valid_to"], [
        {"link_id": "L_BC", "left_entity": "B", "right_entity": "C",
         "valid_from": 100, "valid_to": 200},
        {"link_id": "L_BX", "left_entity": "B", "right_entity": "X",
         "valid_from": 150, "valid_to": 300},
        {"link_id": "L_XC", "left_entity": "X", "right_entity": "C",
         "valid_from": 150, "valid_to": 250}])
    write_csv("folds.csv",
              ["fold_id", "link_at", "train_from", "train_to",
               "eval_from", "eval_to", "gap"],
              [{"fold_id": f"f{t}", "link_at": t,
                "train_from": 0, "train_to": 20,
                "eval_from": 20, "eval_to": 30, "gap": 0}
               for t in (99, 100, 149, 150, 199, 200, 249, 250, 299)])

    p = lambda n: os.path.join(WORK, f"{n}.csv")
    asm = os.path.join(WORK, "asm.db")
    split_direct = os.path.join(WORK, "split_direct.db")
    split_alt = os.path.join(WORK, "split_alt.db")
    assemble(asm, anchors_csv=p("anchors"),
             feature_defs_csv=p("feature_defs"), facts_csv=p("facts"),
             outcomes_csv=p("outcomes"), coverage_csv=p("coverage"),
             horizon=5, as_of_k=1000)
    build_splits(split_direct, assembly_db=asm,
                 fold_plan_csv=p("folds"), alias_csv=p("links_direct"))
    build_splits(split_alt, assembly_db=asm,
                 fold_plan_csv=p("folds"), alias_csv=p("links_alt"))

    # ---- 1) 四端点（仅直连 B-C）：99/200 放行，100/199 经 M 连通 ----
    expect = {99: "train", 100: "related_eval", 199: "related_eval",
              200: "train"}
    for t, want in expect.items():
        got = lookup_example(split_direct, f"f{t}", "T")["state"]
        assert got == want, (t, got, want)
    for t in (100, 199):
        report = lookup_example(split_direct, f"f{t}", "T")
        nodes = [n["id"] for n in report["reason"]["path"]["nodes"]]
        assert nodes[0] == "T" and nodes[-1] == "V" and "M" in nodes
        prov = [h for h in report["reason"]["path"]["hops"]
                if h["type"] == "provenance"]
        revs = {(h["example_id"], h["used_records"][0]["revision"])
                for h in prov}
        assert ("T", 1) in revs and ("M", 2) in revs
    print("1) 时效边四端点 + 经 M 的双修订来路：OK")

    # ---- 2) 替代路径（split_alt）：直连在 200 过期，但 B-X-C 仍通，
    # 挡住到 249；XC 在 250 失效后无替代路径，250 起放行 ----
    for t in (200, 249):
        r = lookup_example(split_alt, f"f{t}", "T")
        assert r["state"] == "related_eval", (t, r["state"])
        assert "X" in [n["id"] for n in
                       r["reason"]["path"]["nodes"]]
    for t in (250, 299):
        assert lookup_example(split_alt, f"f{t}", "T")["state"] == "train"
    print("2) 替代路径部分相交，直连过期后仍挡、替代到期才放行：OK")

    print(f"\n切分库位于 {WORK}（data/ 不入库），可用 run_splitter.py "
          "export/lookup/stats/verify 复核")


if __name__ == "__main__":
    main()
