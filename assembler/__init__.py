"""离线训练资料装配器（防时间穿越）。

公开入口：
  :func:`assemble`        全量装配
  :func:`explain_example` 按 example_id 回查依据
  :func:`pending_reason_stats` 统计 pending 原因
"""

from .errors import AssemblyError
from .labels import pending_reason_stats
from .pipeline import assemble, explain_example

__all__ = ["AssemblyError", "assemble", "explain_example",
           "pending_reason_stats"]
