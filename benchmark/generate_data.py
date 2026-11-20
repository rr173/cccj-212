#!/usr/bin/env python3
"""大规模乱序资料生成器（流式写 CSV，不在内存中持有全量数据）。

默认规模满足任务下限：
  锚点 100,000 个、事实 1,200,000 条（含多修订/撤回/空值/乱序到达）。

所有文件写入独立空目录，装配脚本以路径参数读取，解释器以 -I 运行。
"""

import argparse
import csv
import os
import random

HEADERS = {
    "anchors": ["example_id", "entity", "cutoff"],
    "feature_defs": ["feature_id", "category", "window", "op", "horizon"],
    "facts": ["fact_id", "revision", "event_time", "available_time",
              "entity", "category", "value", "op"],
    "outcomes": ["outcome_id", "entity", "event_time", "available_time"],
    "coverage": ["entity", "a", "b", "received_at"],
}


def generate(out_dir, n_anchors, n_facts, n_entities, seed):
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(seed)
    cutoff_max = 90_000
    horizon = 5
    as_of_k = 95_000
    categories = ["X", "Y", "Z"]

    # ---- 锚点：同一实体大量锚点（每实体约 n_anchors/n_entities 个）----
    anchor_path = os.path.join(out_dir, "anchors.csv")
    with open(anchor_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS["anchors"])
        writer.writeheader()
        for i in range(n_anchors):
            writer.writerow({
                "example_id": f"E{i:08d}",
                "entity": f"ent{i % n_entities}",
                "cutoff": rng.randint(1_000, cutoff_max),
            })

    # ---- 特征定义：count/sum/latest 各一个 ----
    def_path = os.path.join(out_dir, "feature_defs.csv")
    with open(def_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS["feature_defs"])
        writer.writeheader()
        for op in ("count", "sum", "latest"):
            writer.writerow({"feature_id": f"f_{op}", "category": "X",
                             "window": 100, "op": op, "horizon": ""})

    # ---- 事实：流式生成，乱序落盘；约 40% 的 ID 有多个修订 ----
    fact_path = os.path.join(out_dir, "facts.csv")
    n_distinct_ids = n_facts // 2
    with open(fact_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS["facts"])
        writer.writeheader()
        written = 0
        logical_id = 0
        while written < n_facts:
            entity = f"ent{rng.randrange(n_entities)}"
            base_event = rng.randint(0, cutoff_max + 2_000)
            base_avail = rng.randint(0, cutoff_max + 3_000)
            revisions = (1 if rng.random() < 0.6
                         else rng.randint(2, 4))
            for rev in range(1, revisions + 1):
                if written >= n_facts:
                    break
                # 新修订可改实体与事件时间（题目明确允许）
                if rev > 1 and rng.random() < 0.3:
                    entity = f"ent{rng.randrange(n_entities)}"
                event_time = base_event + rng.randint(-50, 50)
                available_time = base_avail + (rev - 1) * rng.randint(1, 50)
                # 故意制造低修订晚到：部分 rev1 的可用时间晚于 rev2
                if rev == 1 and revisions > 1 and rng.random() < 0.25:
                    available_time += rng.randint(60, 200)
                is_retract = rev == revisions and rng.random() < 0.05
                value = "" if rng.random() < 0.2 else rng.randint(
                    -1_000_000, 1_000_000)
                writer.writerow({
                    "fact_id": f"F{logical_id:09d}",
                    "revision": rev,
                    "event_time": event_time,
                    "available_time": available_time,
                    "entity": entity,
                    "category": rng.choice(categories),
                    "value": value,
                    "op": "retract" if is_retract else "upsert",
                })
                written += 1
            logical_id += 1

    # ---- 结局：约 30% 实体有结局 ----
    outcome_path = os.path.join(out_dir, "outcomes.csv")
    with open(outcome_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS["outcomes"])
        writer.writeheader()
        for e in range(n_entities):
            if rng.random() < 0.3:
                et = rng.randint(1_000, cutoff_max + horizon)
                writer.writerow({
                    "outcome_id": f"O{e:05d}",
                    "entity": f"ent{e}",
                    "event_time": et,
                    # 部分结局晚于 K，装配时必须不可见
                    "available_time": et + rng.choice([0, 1, 10_000]),
                })

    # ---- 覆盖：为 80% 实体生成可能断裂的区间；部分 received_at 晚于 K ----
    coverage_path = os.path.join(out_dir, "coverage.csv")
    with open(coverage_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS["coverage"])
        writer.writeheader()
        span_lo, span_hi = 1_000, cutoff_max + horizon + 2
        for e in range(n_entities):
            if rng.random() >= 0.8:
                continue
            cursor = span_lo
            while cursor < span_hi:
                step = rng.randint(50, 400)
                end = min(cursor + step, span_hi)
                # 10% 概率制造断裂（跳过一段）
                if rng.random() < 0.1:
                    cursor = end
                    continue
                writer.writerow({
                    "entity": f"ent{e}",
                    "a": cursor,
                    "b": end,
                    "received_at": rng.choice([
                        end, end + rng.randint(0, 50),
                        as_of_k + rng.randint(1, 5_000)]),
                })
                cursor = end

    manifest = {
        "n_anchors": n_anchors, "n_facts": n_facts,
        "n_entities": n_entities, "horizon": horizon,
        "as_of_k": as_of_k, "seed": seed,
    }
    with open(os.path.join(out_dir, "manifest.tsv"), "w",
              encoding="utf-8") as fh:
        for key, value in manifest.items():
            fh.write(f"{key}\t{value}\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description="大规模乱序资料生成器")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--anchors", type=int, default=100_000)
    parser.add_argument("--facts", type=int, default=1_200_000)
    parser.add_argument("--entities", type=int, default=2_000)
    parser.add_argument("--seed", type=int, default=20261120)
    args = parser.parse_args()
    manifest = generate(args.out_dir, args.anchors, args.facts,
                        args.entities, args.seed)
    for key, value in manifest.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
