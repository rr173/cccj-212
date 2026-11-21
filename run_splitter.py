#!/usr/bin/env python3
"""命令行入口：build / export / lookup / stats / verify。

示例：
  python3 -I run_splitter.py build --db data/splits.db \
      --assembly data/run.db --plan data/folds.csv --aliases data/links.csv
  python3 -I run_splitter.py export --db data/splits.db --fold f0 --state train
  python3 -I run_splitter.py lookup --db data/splits.db --fold f0 --example T
  python3 -I run_splitter.py stats --db data/splits.db
  python3 -I run_splitter.py verify --db data/splits.db --assembly data/run.db \
      --plan data/folds.csv --aliases data/links.csv
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from splitter import (SplitError, build_splits, export_examples, fold_stats,
                      lookup_example, verify_source)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="多折分组/时间隔离切分器（只读装配库，输出独立切分库）")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="构建切分库")
    build.add_argument("--db", required=True)
    build.add_argument("--assembly", required=True, help="已完成装配库")
    build.add_argument("--plan", required=True, help="切分计划 CSV")
    build.add_argument("--aliases", required=True, help="别名关系 CSV")
    build.add_argument("--keep-db", action="store_true",
                       help="失败时保留临时库（默认删除，失败不交付）")

    export = sub.add_parser("export", help="按折导出 train/eval 清单")
    export.add_argument("--db", required=True)
    export.add_argument("--fold", required=True)
    export.add_argument("--state", required=True, choices=("train", "eval"))
    export.add_argument("--out", help="输出文件；缺省打印到标准输出")

    lookup = sub.add_parser("lookup", help="按折按样本回查")
    lookup.add_argument("--db", required=True)
    lookup.add_argument("--fold", required=True)
    lookup.add_argument("--example", required=True)
    lookup.add_argument("--no-path", action="store_true",
                        help="related_eval 不构造连接路径")

    stats = sub.add_parser("stats", help="按折统计各状态")
    stats.add_argument("--db", required=True)
    stats.add_argument("--fold")

    verify = sub.add_parser("verify", help="核对装配资料/计划/别名输入")
    verify.add_argument("--db", required=True)
    verify.add_argument("--assembly")
    verify.add_argument("--plan")
    verify.add_argument("--aliases")

    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            info = build_splits(
                args.db, assembly_db=args.assembly,
                fold_plan_csv=args.plan, alias_csv=args.aliases)
            print(json.dumps(info, ensure_ascii=False, indent=2))
            return 0
        if args.command == "export":
            ids = export_examples(args.db, args.fold, args.state)
            text = "\n".join(ids) + ("\n" if ids else "")
            if args.out:
                with open(args.out, "w", encoding="utf-8") as fh:
                    fh.write(text)
            else:
                sys.stdout.write(text)
            return 0
        if args.command == "lookup":
            report = lookup_example(args.db, args.fold, args.example,
                                    with_path=not args.no_path)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0
        if args.command == "stats":
            data = fold_stats(args.db, args.fold)
            print(json.dumps(data, ensure_ascii=False, indent=2))
            return 0
        report = verify_source(
            args.db, assembly_db=args.assembly,
            fold_plan_csv=args.plan, alias_csv=args.aliases)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    except SplitError as exc:
        print(f"切分失败: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
