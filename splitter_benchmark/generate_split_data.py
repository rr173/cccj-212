#!/usr/bin/env python3
"""64 折压力场景资料生成器（流式写 CSV，不在内存持有全量数据）。

规模满足任务下限并混入四类关系结构：

* 120,000 个样本、10 个 count 特征，装配后 feature_provenance 约
  102 万行（事实对相关样本总是“可用且在窗口内”，逐特征逐样本落来路）；
* 250,000 条别名边，其中：
  - 跨多折长边（覆盖全部 link_at）；
  - 只命中部分折的短边（折段命中，半开端点）；
  - 100 个**隔离**断连/复连簇（保留实体不参与随机边），每簇一条
    直连边与一条经 hub 的两跳替代路径，区间错开，先连通、中断、复连；
  - 一条覆盖数千样本的共享事实（星形，绝不展开成样本两两配对）。

所有文件写入独立空目录，装配/切分脚本以路径参数读取，用 -I 运行。
"""

import argparse
import csv
import os
import random

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

N_SAMPLES = 120_000
N_ENTITIES = 2_400
N_FEATURES = 10
HORIZON = 5
K = 200_000
N_FOLDS = 64
N_ALIAS = 250_000
LINK_STEP = 100
LINK_BASE = 100
# 100 个隔离复连簇占用 200 个保留实体，不参与随机别名边
N_CLUSTERS = 100
RESERVED = 2 * N_CLUSTERS
# 共享事实覆盖的样本数（锚定在专用实体 shared_ent 上）
N_SHARED = 4_000
SHARED_ENT = "shared_ent"
# 复连簇受控锚点的 cutoff（x 侧训练、y 侧验证，且 x 不触发 embargo）
X_CUTOFF = 10_000
Y_CUTOFF = 80_000


def generate(out_dir, seed=20261120):
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(seed)
    cutoff_lo, cutoff_hi = 1_000, 100_000
    pool_entities = N_ENTITIES - RESERVED  # 随机边只用这些实体

    # ---- 锚点 ----
    # 1) 每个复连簇两个受控锚点：x 侧训练 cutoff、y 侧验证 cutoff；
    # 2) N_SHARED 个锚点锚定专用实体 shared_ent（共享事实星形）；
    # 3) 其余锚点均匀落在池内实体（保留实体不再被普通锚点触及）。
    anchor_path = os.path.join(out_dir, "anchors.csv")
    n_controlled = 2 * N_CLUSTERS
    with open(anchor_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=A_HEADERS)
        wr.writeheader()
        idx = 0
        for c in range(N_CLUSTERS):
            wr.writerow({"example_id": f"CX{c:03d}_x",
                         "entity": f"ent{2 * c}", "cutoff": X_CUTOFF})
            wr.writerow({"example_id": f"CX{c:03d}_y",
                         "entity": f"ent{2 * c + 1}",
                         "cutoff": Y_CUTOFF})
            idx += 2
        for _ in range(N_SHARED):
            wr.writerow({"example_id": f"E{idx:08d}",
                         "entity": SHARED_ENT,
                         "cutoff": rng.randint(cutoff_lo, cutoff_hi)})
            idx += 1
        remaining = N_SAMPLES - idx
        for k in range(remaining):
            ent = f"ent{RESERVED + (k % pool_entities)}"
            wr.writerow({"example_id": f"E{idx:08d}", "entity": ent,
                         "cutoff": rng.randint(cutoff_lo, cutoff_hi)})
            idx += 1

    # ---- 特征定义：10 个 count，窗口足够大以保证事实落在窗口内 ----
    def_path = os.path.join(out_dir, "feature_defs.csv")
    with open(def_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=D_HEADERS)
        wr.writeheader()
        for j in range(N_FEATURES):
            wr.writerow({"feature_id": f"f{j:02d}", "category": f"cat{j}",
                         "window": 200_000, "op": "count", "horizon": ""})

    # ---- 事实：每特征每实体一条，available=1 且 event=0 ----
    # cutoff>=1000、窗口下界远小于 0，因此对该实体所有样本都可用且在
    # 窗口 [cutoff-window, cutoff) 内 -> 来路行 ≈ 样本数*特征数。
    fact_path = os.path.join(out_dir, "facts.csv")
    n_facts = 0
    with open(fact_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=F_HEADERS)
        wr.writeheader()
        for j in range(N_FEATURES):
            for e in range(N_ENTITIES):
                wr.writerow({
                    "fact_id": f"F{j:04d}_{e:05d}", "revision": 1,
                    "event_time": 0, "available_time": 1,
                    "entity": f"ent{e}", "category": f"cat{j}",
                    "value": rng.randint(1, 99), "op": "upsert"})
                n_facts += 1
            # 专用共享实体的每特征事实，保证其锚点逐特征都有来路
            wr.writerow({
                "fact_id": f"F{j:04d}_SHARED", "revision": 1,
                "event_time": 0, "available_time": 1,
                "entity": SHARED_ENT, "category": f"cat{j}",
                "value": rng.randint(1, 99), "op": "upsert"})
            n_facts += 1
        # 共享事实：单修订、专用实体；被全部该实体锚点引用（星形）
        wr.writerow({
            "fact_id": "F_SHARED", "revision": 1, "event_time": 0,
            "available_time": 1, "entity": SHARED_ENT,
            "category": "cat0", "value": 1, "op": "upsert"})
        n_facts += 1

    # ---- 结局：约 25% 池内实体有结局 ----
    outcome_path = os.path.join(out_dir, "outcomes.csv")
    with open(outcome_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=O_HEADERS)
        wr.writeheader()
        for e in range(N_ENTITIES):
            if rng.random() < 0.25:
                et = rng.randint(cutoff_lo, cutoff_hi + HORIZON)
                wr.writerow({"outcome_id": f"O{e:05d}",
                             "entity": f"ent{e}", "event_time": et,
                             "available_time": et})

    # ---- 覆盖：绝大多数实体宽区间闭合，5% 池内实体留缺口制造 pending。
    # 保留的复连簇实体（前 RESERVED 个）必须闭合，保证探针取得到 eval ----
    coverage_path = os.path.join(out_dir, "coverage.csv")
    with open(coverage_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=C_HEADERS)
        wr.writeheader()
        for e in range(N_ENTITIES):
            if e >= RESERVED and rng.random() < 0.05:
                continue
            wr.writerow({"entity": f"ent{e}", "a": 0,
                         "b": cutoff_hi + HORIZON + 10,
                         "received_at": K})
        # 专用共享实体必须闭合
        wr.writerow({"entity": SHARED_ENT, "a": 0,
                     "b": cutoff_hi + HORIZON + 10, "received_at": K})

    # ---- 切分计划：64 折，固定窗口，link_at 均匀分布 ----
    plan_path = os.path.join(out_dir, "folds.csv")
    with open(plan_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=PLAN_HEADERS)
        wr.writeheader()
        for k in range(N_FOLDS):
            wr.writerow({
                "fold_id": f"fold{k:02d}",
                "link_at": LINK_BASE + k * LINK_STEP,
                "train_from": 1_000, "train_to": 60_000,
                "eval_from": 70_000, "eval_to": 100_001, "gap": 1_000})

    # ---- 别名边 ----
    alias_path = os.path.join(out_dir, "links.csv")
    link_seq = 0

    def write_link(wr, left, right, vf, vt):
        nonlocal link_seq
        wr.writerow({"link_id": f"L{link_seq:07d}", "left_entity": left,
                     "right_entity": right,
                     "valid_from": vf, "valid_to": vt})
        link_seq += 1

    # 复连簇三阶段布局（64 折）：
    #   直连 x-y：活跃折 0..19（折 20 过期）；
    #   替代路径 x-hub（段1，活跃到折 29）+ hub-y（段2，折 15..29），
    #   两段在折 15..29 相交，因此替代路径整体在折 15..29 可走。
    # 折 0..14：仅直连；折 15..19：直连与替代并存；
    # 折 20..29：直连过期但替代路径仍挡住 x；
    # 折 30..63：替代两段都到期，无路径，x 放行。
    if N_FOLDS >= 64:
        direct_end_fold, alt2_start_fold, alt_dead_fold = 20, 15, 30
    else:
        third = max(1, N_FOLDS // 4)
        direct_end_fold = max(1, N_FOLDS // 3)
        alt2_start_fold = max(0, direct_end_fold - third)
        alt_dead_fold = min(N_FOLDS, direct_end_fold + third)
    direct_end = LINK_BASE + direct_end_fold * LINK_STEP
    seg1_end = LINK_BASE + alt_dead_fold * LINK_STEP       # x-hub 到折30
    seg2_from = LINK_BASE + alt2_start_fold * LINK_STEP   # hub-y 折15起
    seg2_to = LINK_BASE + alt_dead_fold * LINK_STEP       # hub-y 折30止
    far_end = LINK_BASE + N_FOLDS * LINK_STEP

    with open(alias_path, "w", encoding="utf-8", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=LINK_HEADERS)
        wr.writeheader()

        # (a) 隔离断连/复连簇：保留实体，随机边绝不触及。
        # 簇 c：x=ent(2c) --直连-- y=ent(2c+1)，另一条经 hub 的替代路径。
        # 直连与 x-hub 同段（折 0..gap_start-1）；hub-y 提前生效、
        # 在 alt2_end 到期（与前段部分相交）。三阶段：仅直连 ->
        # 直连+替代并存 -> 直连过期但替代仍通 -> 替代也到期而放行。
        for c in range(N_CLUSTERS):
            x = f"ent{2 * c}"
            y = f"ent{2 * c + 1}"
            hub = f"alias_only_{c}"   # 只在别名文件出现的实体
            write_link(wr, x, y, LINK_BASE, direct_end)       # 直连
            write_link(wr, x, hub, LINK_BASE, seg1_end)       # 替代段1
            write_link(wr, hub, y, seg2_from, seg2_to)        # 替代段2

        # (b) 长边：覆盖全部 64 折（仅在池内实体间，避开保留实体）
        n_long = (N_ALIAS - link_seq) // 2
        for _ in range(n_long):
            a = f"ent{rng.randrange(pool_entities) + RESERVED}"
            b = f"ent{rng.randrange(pool_entities) + RESERVED}"
            write_link(wr, a, b, 0, far_end)

        # (c) 短边：只命中一段连续折（1..7 折），半开端点
        while link_seq < N_ALIAS:
            a = f"ent{rng.randrange(pool_entities) + RESERVED}"
            b = f"ent{rng.randrange(pool_entities) + RESERVED}"
            start = rng.randrange(0, N_FOLDS)
            length = rng.randrange(1, 8)
            end = min(start + length, N_FOLDS)
            write_link(wr, a, b,
                       LINK_BASE + start * LINK_STEP,
                       LINK_BASE + end * LINK_STEP)

    manifest = {
        "n_samples": N_SAMPLES, "n_entities": N_ENTITIES,
        "n_features": N_FEATURES, "n_facts": n_facts,
        "n_folds": N_FOLDS, "n_alias_target": N_ALIAS,
        "n_alias_written": link_seq,
        "n_reconnect_clusters": N_CLUSTERS,
        "direct_active_folds": f"0..{direct_end_fold - 1}",
        "alternative_walkable_folds": f"{alt2_start_fold}..{alt_dead_fold - 1}",
        "released_fold": alt_dead_fold,
        "shared_fact_samples": N_SHARED,
        "horizon": HORIZON, "as_of_k": K, "seed": seed,
    }
    with open(os.path.join(out_dir, "manifest.tsv"), "w",
              encoding="utf-8") as fh:
        for key, value in manifest.items():
            fh.write(f"{key}\t{value}\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description="64 折压力资料生成器")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--seed", type=int, default=20261120)
    args = parser.parse_args()
    manifest = generate(args.out_dir, args.seed)
    for key, value in manifest.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
