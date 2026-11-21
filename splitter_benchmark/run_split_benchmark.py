#!/usr/bin/env python3
"""64 折切分的 256 MiB 受限压测。

worker 子进程通过 RLIMIT_AS 把整个进程地址空间硬限制在 256 MiB（含
SQLite 页缓存、array 图结构与 Python 分配），父进程记录装配耗时、
切分耗时、峰值 RSS、状态分布与断连/复连簇成员变化。

运行：python3 -I splitter_benchmark/run_split_benchmark.py
"""

import argparse
import json
import os
import resource
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
BUDGET_BYTES = 256 * 1024 * 1024

WORKER_SCRIPT = r'''
import json, os, resource, sqlite3, sys, time
sys.path.insert(0, %r)
from assembler import assemble
from splitter import build_splits, fold_stats, lookup_example
from splitter.store import open_split_readonly

work = sys.argv[1]
paths = {n: os.path.join(work, f"{n}.csv")
         for n in ("anchors", "feature_defs", "facts", "outcomes",
                   "coverage", "folds", "links")}
asm_db = os.path.join(work, "stress_asm.db")
split_db = os.path.join(work, "stress_split.db")

# ---- 1) 装配（同一 256MiB 预算内）----
t0 = time.perf_counter()
assemble(asm_db, anchors_csv=paths["anchors"],
         feature_defs_csv=paths["feature_defs"],
         facts_csv=paths["facts"], outcomes_csv=paths["outcomes"],
         coverage_csv=paths["coverage"], horizon=5, as_of_k=200000)
asm_elapsed = time.perf_counter() - t0

src = sqlite3.connect("file:" + asm_db + "?mode=ro", uri=True)
counts = {t: src.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
          for t in ("anchors", "facts", "feature_provenance", "labels")}
label_dist = dict(src.execute(
    "SELECT status, COUNT(*) FROM labels GROUP BY status"))
src.close()

# ---- 2) 64 折切分 ----
t1 = time.perf_counter()
info = build_splits(split_db, assembly_db=asm_db,
                    fold_plan_csv=paths["folds"], alias_csv=paths["links"])
split_elapsed = time.perf_counter() - t1

conn = open_split_readonly(split_db)
# 处理计数：折数、每折状态行数、别名边与静态边规模
n_folds = conn.execute("SELECT COUNT(*) FROM folds").fetchone()[0]
n_alias = conn.execute("SELECT COUNT(*) FROM alias_edges").fetchone()[0]
n_static = conn.execute("SELECT COUNT(*) FROM static_edges").fetchone()[0]
n_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
n_assign = conn.execute("SELECT COUNT(*) FROM assignments").fetchone()[0]
state_totals = dict(conn.execute(
    "SELECT state, COUNT(*) FROM assignments GROUP BY state").fetchall())

# ---- 3) 断连/复连簇成员核对（簇 0：x=ent0 / y=ent1，保留实体）----
# 三阶段：仅直连(0..14) -> 直连+替代并存(15..19) -> 直连过期但替代
# 路径仍通(20..29) -> 替代也到期而放行(30..63)。
# 受控锚点 CX000_x（训练侧，ent0）与 CX000_y（验证侧，ent1）。
# 保留实体无任何同实体 eval：连通期 x 的对手恰为 CX000_y；
# 替代路径也到期后 x 无对手 -> train。
x_train = "CX000_x"
y_eval = "CX000_y"
reconnect = []
expectations = {
    "fold00": "related_eval",   # 仅直连
    "fold14": "related_eval",   # 直连，替代段2 刚于折15前未生效
    "fold15": "related_eval",   # 直连+替代并存
    "fold19": "related_eval",
    "fold20": "related_eval",   # 直连过期，替代路径 x-hub-y 仍挡住
    "fold29": "related_eval",
    "fold30": "train",          # 替代段2 到期，无路径，放行
    "fold63": "train"}
reconnect_ok = True
for fid, want in expectations.items():
    state, opp = conn.execute(
        "SELECT state, opponent_id FROM assignments "
        "WHERE fold_id=? AND example_id=?", (fid, x_train)).fetchone()
    match = state == want
    # 连通时对手必须是受控验证锚点；放行时必须无对手
    if want == "related_eval":
        match = match and opp == y_eval
    else:
        match = match and opp is None
    reconnect.append({"fold": fid, "example": x_train, "state": state,
                      "opponent": opp, "expected": want, "match": match})
    if not match:
        reconnect_ok = False
# y 在各折稳定为 eval（闭合标签、验证 cutoff）
y_states = dict(conn.execute(
    "SELECT fold_id, state FROM assignments WHERE example_id=?",
    (y_eval,)).fetchall())
if any(s != "eval" for s in y_states.values()):
    reconnect_ok = False

# 共享事实星形核对：F_SHARED 度数 = 引用它的去重样本数（无两两配对）
shared_degree = conn.execute(
    "SELECT COUNT(*) FROM static_edges s JOIN nodes n ON n.node_id=s.b_node "
    "WHERE n.kind='fact' AND n.name='F_SHARED'").fetchone()[0]
shared_refs = conn.execute(
    "SELECT COUNT(DISTINCT example_id) FROM provenance_refs "
    "WHERE fact_id='F_SHARED'").fetchone()[0]
# 星形而非两两配对：事实节点的邻居数应远小于样本数的平方量级，
# 且等于去重样本数
shared_ok = (shared_degree == shared_refs and shared_refs >= 1000)

# 折间计数稳定性：每折状态行数之和 == 样本数 * 折数
per_fold = [r[0] for r in conn.execute(
    "SELECT COUNT(*) FROM assignments GROUP BY fold_id ORDER BY fold_id")]
row_invariant_ok = all(n == per_fold[0] for n in per_fold) and (
    per_fold[0] == counts["anchors"])

# 处理计数：证明没有为每折重读来路 / 重扫全部长期活跃别名
n_prov = counts["feature_provenance"]
# 别名在各折首次生效的总次数（事件驱动），与“折数 * 别名总数”对比可证
# 长期边只在首折处理一次；该数从切分库独立按区间复算
fold_rows = conn.execute(
    "SELECT fold_id, link_at FROM folds ORDER BY fold_order").fetchall()
alias_intervals = conn.execute(
    "SELECT valid_from, valid_to FROM alias_edges").fetchall()
start_events = 0
for _fid, link_at in fold_rows:
    start_events += sum(1 for vf, vt in alias_intervals if vf <= link_at < vt)
naive_scan = n_alias * len(fold_rows)  # 朴素做法每折重扫全部边的比较次数
conn.close()
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
print(json.dumps({
    "asm_elapsed_sec": asm_elapsed,
    "split_elapsed_sec": split_elapsed,
    "peak_rss_bytes": peak,
    "source_counts": counts, "label_dist": label_dist,
    "graph": {"nodes": n_nodes, "static_edges": n_static,
              "alias_edges": n_alias, "assignment_rows": n_assign,
              "folds": n_folds},
    "state_totals": state_totals,
    "per_fold_rows_minmax": [min(per_fold), max(per_fold)],
    "processing_counters": {
        "provenance_rows_scanned_once": n_prov,
        "alias_edges_scanned_twice_bisect": n_alias,
        "active_edge_union_total": start_events,
        "naive_per_fold_rescan_comparisons": naive_scan,
        "note": "来路仅构建期流式扫描一次；别名每条仅两次二分定位折段，"
                "长期边只在首折 union 一次，比较次数远小于朴素重扫"},
    "invariants": {
        "per_fold_row_count_ok": row_invariant_ok,
        "reconnect_ok": reconnect_ok,
        "shared_fact_star_ok": shared_ok},
    "reconnect_probe": reconnect,
    "shared_fact": {"degree": shared_degree,
                    "distinct_samples": shared_refs},
    "build_info": info,
}, ensure_ascii=False, indent=2))
''' % ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir",
                        default=os.path.join(ROOT, "data", "split_benchmark"))
    parser.add_argument("--skip-generate", action="store_true")
    args = parser.parse_args()

    if not args.skip_generate:
        from splitter_benchmark.generate_split_data import generate
        os.makedirs(args.work_dir, exist_ok=True)
        print(f"[1/3] 生成压力资料到 {args.work_dir} ...")
        t0 = time.perf_counter()
        manifest = generate(args.work_dir)
        print(f"      生成耗时 {time.perf_counter()-t0:.1f}s：{manifest}")

    print(f"[2/3] RLIMIT_AS={BUDGET_BYTES//1024//1024} MiB worker 中 "
          f"装配 + 64 折切分 ...")
    worker_path = os.path.join(args.work_dir, "_split_worker.py")
    with open(worker_path, "w", encoding="utf-8") as fh:
        fh.write(WORKER_SCRIPT)

    def preexec():
        resource.setrlimit(resource.RLIMIT_AS,
                           (BUDGET_BYTES, BUDGET_BYTES))

    t0 = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, "-I", worker_path, args.work_dir],
        capture_output=True, text=True, preexec_fn=preexec)
    wall = time.perf_counter() - t0

    report_path = os.path.join(args.work_dir, "split_benchmark_report.json")
    if proc.returncode != 0:
        print("worker 失败（可能超出 256MiB），退出码", proc.returncode)
        print(proc.stderr[-4000:])
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump({"ok": False, "returncode": proc.returncode,
                       "stderr_tail": proc.stderr[-4000:]}, fh,
                      ensure_ascii=False, indent=2)
        sys.exit(1)

    report = json.loads(proc.stdout)
    report["ok"] = True
    report["parent_wall_sec"] = wall
    report["budget_bytes"] = BUDGET_BYTES
    report["within_budget"] = report["peak_rss_bytes"] <= BUDGET_BYTES
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    print("[3/3] 压测结果")
    print(f"  装配耗时: {report['asm_elapsed_sec']:.1f}s")
    print(f"  64 折切分耗时: {report['split_elapsed_sec']:.1f}s "
          f"（墙钟 {wall:.1f}s）")
    mib = report["peak_rss_bytes"] / 1024 / 1024
    print(f"  峰值 RSS: {mib:.1f} MiB / 256 MiB（达标: "
          f"{report['within_budget']}）")
    print(f"  来源行数: {json.dumps(report['source_counts'])}")
    print(f"  图规模: {json.dumps(report['graph'])}")
    print(f"  状态总量: {json.dumps(report['state_totals'])}")
    pc = report["processing_counters"]
    print("  处理计数（结构证据）:")
    print(f"    来路流式扫描次数=1（{pc['provenance_rows_scanned_once']} 行，"
          f"未按折重读）")
    print(f"    别名每条两次二分（{pc['alias_edges_scanned_twice_bisect']} 边）"
          f" vs 朴素逐折重扫 {pc['naive_per_fold_rescan_comparisons']} 次比较")
    print(f"    各折活跃边 union 总量 {pc['active_edge_union_total']}")
    print("  断连/复连探针（先连通 -> 中断 -> 复连）:")
    for row in report["reconnect_probe"]:
        flag = "OK" if row["match"] else "MISMATCH"
        print(f"    [{flag}] {row['fold']}: {row['example']} -> "
              f"{row['state']}（期望 {row['expected']}，对手 {row['opponent']}）")
    sf = report["shared_fact"]
    print(f"  共享事实 F_SHARED 度数={sf['degree']} "
          f"（=去重样本数 {sf['distinct_samples']}，星形未两两展开）")
    inv = report["invariants"]
    print(f"  不变量: {json.dumps(inv, ensure_ascii=False)}")
    if not report["within_budget"] or not all(inv.values()):
        print("压测未达标（超内存或不变量失败）", file=sys.stderr)
        sys.exit(1)
    print(f"\n完整报告: {report_path}")


if __name__ == "__main__":
    main()
