#!/usr/bin/env python3
"""命令行入口：assemble / explain / pending-stats。

示例：
  python3 run_assembler.py assemble --db data/run.db \
      --anchors a.csv --feature-defs d.csv --facts f.csv \
      --outcomes o.csv --coverage c.csv --horizon 5 --as-of-k 20
  python3 run_assembler.py explain --db data/run.db --example-id E1
  python3 run_assembler.py pending-stats --db data/run.db
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from assembler import (AssemblyError, assemble, explain_example,
                       pending_reason_stats)
from assembler.store import connect


def _json_default(obj):
    if isinstance(obj, (tuple, set)):
        return list(obj)
    return str(obj)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="防时间穿越的离线训练资料装配器")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("assemble", help="运行全量装配")
    build.add_argument("--db", required=True)
    build.add_argument("--anchors", required=True)
    build.add_argument("--feature-defs", required=True)
    build.add_argument("--facts", required=True)
    build.add_argument("--outcomes", required=True)
    build.add_argument("--coverage", required=True)
    build.add_argument("--horizon", type=int, required=True)
    build.add_argument("--as-of-k", type=int, required=True)
    build.add_argument("--keep-db", action="store_true",
                       help="失败时也保留半成品库（默认删除，失败不交付）")

    look = sub.add_parser("explain", help="按 example_id 回查全部依据")
    look.add_argument("--db", required=True)
    look.add_argument("--example-id", required=True)
    look.add_argument("--provenance-limit", type=int, default=100)

    stats = sub.add_parser("pending-stats", help="统计 pending 原因")
    stats.add_argument("--db", required=True)

    args = parser.parse_args(argv)

    try:
        if args.command == "assemble":
            try:
                info = assemble(
                    args.db, anchors_csv=args.anchors,
                    feature_defs_csv=args.feature_defs,
                    facts_csv=args.facts, outcomes_csv=args.outcomes,
                    coverage_csv=args.coverage, horizon=args.horizon,
                    as_of_k=args.as_of_k)
            except AssemblyError:
                if args.keep_db:
                    raise
                raise
            print(json.dumps(info, ensure_ascii=False, indent=2))
            return 0

        conn = connect(args.db)
        try:
            status = conn.execute(
                "SELECT value FROM meta WHERE key='build_status'"
            ).fetchone()
            if not status or status[0] != "complete":
                raise AssemblyError("该库未完成装配，拒绝查询")

            if args.command == "explain":
                report = explain_example(
                    conn, args.example_id,
                    provenance_limit=args.provenance_limit)
                print(json.dumps(report, ensure_ascii=False, indent=2,
                                 default=_json_default))
            else:
                report = pending_reason_stats(conn)
                print(json.dumps(report, ensure_ascii=False, indent=2,
                                 default=_json_default))
        finally:
            conn.close()
        return 0
    except AssemblyError as exc:
        print(f"装配失败: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
