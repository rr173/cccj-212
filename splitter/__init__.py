"""多折切分器：在只读装配库上做分组隔离与时间隔离切分。

公开入口：
  :func:`build_splits`        构建独立 SQLite 切分库
  :func:`export_examples`     按折导出 train/eval 的 example_id
  :func:`lookup_example`      按折按样本回查状态与（可选）真实连接路径
  :func:`fold_stats`          按折统计各状态计数
  :func:`verify_source`       核对切分库与装配资料/计划/别名输入的绑定
"""

from .errors import SplitError
from .pipeline import (build_splits, export_examples, fold_stats,
                       lookup_example, verify_source)

__all__ = ["SplitError", "build_splits", "export_examples",
           "lookup_example", "fold_stats", "verify_source"]
