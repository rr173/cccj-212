"""整数工具：64 位有符号整数边界校验与不溢出的精确求和。

时间为离散整数。SQLite 以变长整数存储（最多 64 位），
Python 整数本身无界，但题目要求显式处理端点溢出：
所有外部输入必须落在 int64 范围内，时间端点相加越界直接拒绝。
"""

from .errors import AssemblyError

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def check_int64(value, what):
    """校验 ``value`` 落在有符号 64 位整数区间。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise AssemblyError(f"{what} 必须是整数，实际为 {value!r}")
    if value < INT64_MIN or value > INT64_MAX:
        raise AssemblyError(f"{what}={value} 超出 int64 范围")
    return value


def checked_add(a, b, what):
    """两个 int64 相加，结果必须仍在 int64 范围内，否则拒绝。"""
    result = a + b
    if result < INT64_MIN or result > INT64_MAX:
        raise AssemblyError(f"{what}: {a} + {b} 端点计算溢出 int64")
    return result


def checked_sub(a, b, what):
    """两个 int64 相减，结果必须仍在 int64 范围内，否则拒绝。"""
    result = a - b
    if result < INT64_MIN or result > INT64_MAX:
        raise AssemblyError(f"{what}: {a} - {b} 端点计算溢出 int64")
    return result


def exact_sum(values):
    """精确累加可迭代的 int 值。

    Python int 无界，绝不会因 int64 单值相加溢出或转 float 损失。
    调用方负责保证每个单值已经过 int64 校验；本函数不做范围假设。
    """
    total = 0
    for value in values:
        total += value
    return total
