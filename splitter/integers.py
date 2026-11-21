"""整数工具：复用装配库的 int64 语义，切分端点评分严格算术。

所有计划时间（cutoff 范围、gap、link_at）都必须落在有符号 64 位整数
区间；隔离界线 ``eval_from - gap`` 与观察末点 ``cutoff + horizon``
的计算一旦越界立即拒绝，绝不静默回绕或转成浮点。
"""

from .errors import SplitError

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def check_int64(value, what):
    """校验 ``value`` 落在有符号 64 位整数区间。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise SplitError(f"{what} 必须是整数，实际为 {value!r}")
    if value < INT64_MIN or value > INT64_MAX:
        raise SplitError(f"{what}={value} 超出 int64 范围")
    return value


def checked_add(a, b, what):
    result = a + b
    if result < INT64_MIN or result > INT64_MAX:
        raise SplitError(f"{what}: {a} + {b} 端点计算溢出 int64")
    return result


def checked_sub(a, b, what):
    result = a - b
    if result < INT64_MIN or result > INT64_MAX:
        raise SplitError(f"{what}: {a} - {b} 端点计算溢出 int64")
    return result
