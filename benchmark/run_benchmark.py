#!/usr/bin/env python3
"""256 MiB 进程预算下的真实装配压测。

做法：worker 子进程通过 RLIMIT_AS 把整个进程地址空间硬限制在 256 MiB
（含 SQLite 页缓存与 Python 分配），父进程记录耗时、峰值 RSS、
抽样结果与标签状态分布。内存超限会在 worker 内表现为 MemoryError
或 sqlite3 OperationalError，父进程据此明确报告失败。

运行：python3 -I benchmark/run_benchmark.py
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

from benchmark.generate_data import generate  # noqa: E402

BUDGET_BYTES = 256 * 1024 * 1024
WORKER_SCRIPT = r'''
import json, os, resource, sys, time
sys.path.insert(0, %r)
from assembler import assemble, explain_example
from assembler.store import connect
from assembler.labels import pending_reason_stats

work = sys.argv[1]
db_path = os.path.join(work, "benchmark.db")
paths = {n: os.path.join(work, f"{n}.csv")
         for n in ("anchors", "feature_defs", "facts",
                   "outcomes", "coverage")}
t0 = time.perf_counter()
info = assemble(db_path, anchors_csv=paths["anchors"],
                feature_defs_csv=paths["feature_defs"],
                facts_csv=paths["facts"],
                outcomes_csv=paths["outcomes"],
                coverage_csv=paths["coverage"],
                horizon=5, as_of_k=95000)
elapsed = time.perf_counter() - t0

conn = connect(db_path)
counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
          for t in ("anchors", "facts", "outcomes", "coverage",
                    "feature_values", "feature_explanations",
                    "feature_provenance", "labels", "label_evidence")}
label_dist = dict(conn.execute(
    "SELECT status, COUNT(*) FROM labels GROUP BY status").fetchall())
status_dist = dict(conn.execute(
    "SELECT status, COUNT(*) FROM feature_explanations "
    "GROUP BY status").fetchall())
# 抽样回查：按实际锚点规模固定取首/中/尾 3 个 example_id
n_anchors = counts["anchors"]
pick_indexes = sorted({0, n_anchors // 2, n_anchors - 1})
samples = []
for idx in pick_indexes:
    example_id = conn.execute(
        "SELECT example_id FROM anchors ORDER BY example_id LIMIT 1 "
        "OFFSET ?", (idx,)).fetchone()[0]
    report = explain_example(conn, example_id, provenance_limit=3)
    samples.append({
        "example_id": example_id,
        "anchor": report["anchor"],
        "features": [{"feature_id": f["feature_id"], "value": f["value"],
                      "status": f["status"],
                      "provenance_total": f["provenance_total"],
                      "provenance_page": f["provenance_page"]}
                     for f in report["features"]],
        "label": report["label"],
    })
pending = pending_reason_stats(conn)
# 精确求和抽查：挑一个有非空 sum 的锚点，手工对来路做 Python 精确求和
cross_check = None
row = conn.execute(
    "SELECT example_id FROM feature_values WHERE feature_id='f_sum' "
    "AND value_text IS NOT NULL LIMIT 1").fetchone()
if row:
    eid = row[0]
    total = 0
    for (enc,) in conn.execute(
            "SELECT cv.value_enc FROM feature_provenance p "
            "JOIN facts cv ON cv.fact_id=p.fact_id AND cv.revision=p.revision "
            "WHERE p.example_id=? AND p.feature_id='f_sum' "
            "AND cv.value_enc != '\x01NULL\x01'", (eid,)):
        total += int(enc)
    db_total = conn.execute(
        "SELECT value_text FROM feature_values "
        "WHERE example_id=? AND feature_id='f_sum'", (eid,)).fetchone()[0]
    cross_check = {"example_id": eid,
                   "python_exact_sum": str(total),
                   "db_sum": db_total,
                   "match": str(total) == db_total}
else:
    cross_check = {"example_id": None, "note": "本资料无任何非空 sum，"
                                               "无可抽查锚点（已由验收测试覆盖）",
                   "match": True}
conn.close()
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
print(json.dumps({"elapsed_sec": elapsed, "peak_rss_bytes": peak,
                  "counts": counts, "label_dist": label_dist,
                  "feature_status_dist": status_dist,
                  "samples": samples,
                  "pending_summary": {
                      "label_status_counts":
                          pending["label_status_counts"],
                      "evidence_kind_counts":
                          pending["evidence_kind_counts"],
                      "total_gap_points": pending["total_gap_points"]},
                  "sum_cross_check": cross_check,
                  "build": info}, ensure_ascii=False, indent=2))
''' % ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir",
                        default=os.path.join(ROOT, "data", "benchmark"))
    parser.add_argument("--anchors", type=int, default=100_000)
    parser.add_argument("--facts", type=int, default=1_200_000)
    parser.add_argument("--entities", type=int, default=2_000)
    args = parser.parse_args()

    os.makedirs(args.work_dir, exist_ok=True)
    print(f"[1/3] 生成资料到 {args.work_dir}（流式，不占大内存）...")
    gen_t0 = time.perf_counter()
    manifest = generate(args.work_dir, args.anchors, args.facts,
                        args.entities, seed=20261120)
    gen_elapsed = time.perf_counter() - gen_t0
    print(f"      生成耗时 {gen_elapsed:.1f}s，清单 {manifest}")

    print(f"[2/3] 在 RLIMIT_AS={BUDGET_BYTES // 1024 // 1024} MiB 的 "
          f"worker 中装配...")
    worker_path = os.path.join(args.work_dir, "_worker.py")
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

    report_path = os.path.join(args.work_dir, "benchmark_report.json")
    if proc.returncode != 0:
        print("worker 失败（可能超出 256MiB 预算），退出码", proc.returncode)
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
    print(f"  装配耗时: {report['elapsed_sec']:.1f}s "
          f"（含进程开销墙钟 {wall:.1f}s）")
    peak_mib = report["peak_rss_bytes"] / 1024 / 1024
    print(f"  峰值 RSS: {peak_mib:.1f} MiB / 预算 256 MiB "
          f"（达标: {report['within_budget']}）")
    print(f"  表行数: {json.dumps(report['counts'], ensure_ascii=False)}")
    print(f"  标签分布: {report['label_dist']}")
    print(f"  特征解释状态分布: {report['feature_status_dist']}")
    print(f"  精确求和抽查: {report['sum_cross_check']}")
    print("  抽样锚点:")
    for sample in report["samples"]:
        print(f"    {sample['example_id']} -> "
              f"{[(f['feature_id'], f['value'], f['status']) for f in sample['features']]} "
              f"label={sample['label'][0]}")
    if not report["within_budget"] or not report["sum_cross_check"]["match"]:
        sys.exit(1)
    print(f"\n完整报告: {report_path}")


if __name__ == "__main__":
    main()
