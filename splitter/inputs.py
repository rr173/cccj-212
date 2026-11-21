"""切分计划与别名边文件的解析、严格校验与输入哈希。

两个 CSV 都只在构建时读一次；校验通过的结构化结果交由落盘模块写入
独立切分库，之后查询与导出不再依赖原文件路径。

别名边语义（硬性要求）：
* 列固定为 ``link_id,left_entity,right_entity,valid_from,valid_to``；
* 每条边无向；``link_id`` 全局唯一，重复即拒绝；
* 区间必须非空（``valid_from < valid_to``），端点为 int64；
* 实体名可以只在别名文件中出现，不要求出现在装配库锚点里；
* 重叠的平行边独立生效，因此不做任何折叠/去重。
"""

import csv
import hashlib
import os

from .errors import SplitError
from .integers import check_int64

PLAN_HEADER = ["fold_id", "link_at", "train_from", "train_to",
               "eval_from", "eval_to", "gap"]
ALIAS_HEADER = ["link_id", "left_entity", "right_entity",
                "valid_from", "valid_to"]


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_inputs(paths):
    """对一组输入文件计算稳定 SHA-256（按路径排序，绑定文件名与字节）。"""
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(os.path.basename(path).encode("utf-8"))
        digest.update(b":")
        digest.update(sha256_file(path).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _open_csv(path):
    if not os.path.isfile(path):
        raise SplitError(f"输入文件不存在: {path}")
    handle = open(path, "r", encoding="utf-8", newline="")
    reader = csv.DictReader(handle)
    if reader.fieldnames is None:
        handle.close()
        raise SplitError(f"{path} 是空文件或缺少表头")
    return handle, reader


def _need(row, column, path, lineno):
    if column not in row or row[column] is None or row[column] == "":
        raise SplitError(f"{path}:{lineno} 缺少必填列 {column}")
    return row[column]


def _parse_int(raw, what, path, lineno):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise SplitError(f"{path}:{lineno} {what}={raw!r} 不是整数")
    if str(value) != str(raw).strip():
        raise SplitError(
            f"{path}:{lineno} {what}={raw!r} 不是合法整数字面量")
    return check_int64(value, f"{path}:{lineno} {what}")


def load_fold_plan(path):
    """解析切分计划 CSV。

    返回按文件行序排列的 fold 字典列表（构建侧会再按 fold_id 排序处理，
    因此产物不依赖输入行序）。每折校验：

    * ``fold_id`` 唯一非空；``gap`` 为非负整数；
    * 训练/验证两段范围均非空：``train_from < train_to``、
      ``eval_from < eval_to``，且 ``train_to <= eval_from``；
    * 全部时间字段为 int64，比较按 Python 精确整数算术。
    """
    handle, reader = _open_csv(path)
    folds = []
    seen_ids = set()
    try:
        for lineno, row in enumerate(reader, start=2):
            fold_id = _need(row, "fold_id", path, lineno)
            if fold_id in seen_ids:
                raise SplitError(
                    f"{path}:{lineno} 重复 fold_id={fold_id}")
            seen_ids.add(fold_id)
            link_at = _parse_int(row.get("link_at"), "link_at",
                                 path, lineno)
            train_from = _parse_int(row.get("train_from"), "train_from",
                                    path, lineno)
            train_to = _parse_int(row.get("train_to"), "train_to",
                                  path, lineno)
            eval_from = _parse_int(row.get("eval_from"), "eval_from",
                                   path, lineno)
            eval_to = _parse_int(row.get("eval_to"), "eval_to",
                                 path, lineno)
            gap = _parse_int(row.get("gap"), "gap", path, lineno)
            if gap < 0:
                raise SplitError(
                    f"{path}:{lineno} gap={gap} 必须是非负整数")
            if not train_from < train_to:
                raise SplitError(
                    f"{path}:{lineno} 训练范围必须非空: "
                    f"[{train_from}, {train_to})")
            if not eval_from < eval_to:
                raise SplitError(
                    f"{path}:{lineno} 验证范围必须非空: "
                    f"[{eval_from}, {eval_to})")
            if train_to > eval_from:
                raise SplitError(
                    f"{path}:{lineno} 要求 train_to <= eval_from，"
                    f"实际 train_to={train_to} eval_from={eval_from}")
            folds.append({
                "fold_id": fold_id, "link_at": link_at,
                "train_from": train_from, "train_to": train_to,
                "eval_from": eval_from, "eval_to": eval_to, "gap": gap,
            })
    finally:
        handle.close()
    if not folds:
        raise SplitError(f"{path} 切分计划为空，至少需要一折")
    return folds


def load_alias_links(path):
    """解析别名关系 CSV。

    返回 ``(links, entities)``：``links`` 为按文件行序的边字典列表
    （平行边保留，不折叠），``entities`` 为全部出现过的实体名集合。
    """
    handle, reader = _open_csv(path)
    links = []
    seen_ids = set()
    entities = set()
    try:
        for lineno, row in enumerate(reader, start=2):
            link_id = _need(row, "link_id", path, lineno)
            if link_id in seen_ids:
                raise SplitError(
                    f"{path}:{lineno} 重复 link_id={link_id}")
            seen_ids.add(link_id)
            left = _need(row, "left_entity", path, lineno)
            right = _need(row, "right_entity", path, lineno)
            valid_from = _parse_int(row.get("valid_from"), "valid_from",
                                    path, lineno)
            valid_to = _parse_int(row.get("valid_to"), "valid_to",
                                  path, lineno)
            if not valid_from < valid_to:
                raise SplitError(
                    f"{path}:{lineno} 别名区间必须非空: "
                    f"[{valid_from}, {valid_to})")
            entities.add(left)
            entities.add(right)
            links.append({
                "link_id": link_id, "left_entity": left,
                "right_entity": right,
                "valid_from": valid_from, "valid_to": valid_to,
            })
    finally:
        handle.close()
    return links, entities


def scan_alias_entities(path):
    """只做校验并收集实体名（第一遍扫描），不保留任何边行。"""
    handle, reader = _open_csv(path)
    seen_ids = set()
    entities = set()
    try:
        for lineno, row in enumerate(reader, start=2):
            link_id = _need(row, "link_id", path, lineno)
            if link_id in seen_ids:
                raise SplitError(
                    f"{path}:{lineno} 重复 link_id={link_id}")
            seen_ids.add(link_id)
            left = _need(row, "left_entity", path, lineno)
            right = _need(row, "right_entity", path, lineno)
            valid_from = _parse_int(row.get("valid_from"), "valid_from",
                                    path, lineno)
            valid_to = _parse_int(row.get("valid_to"), "valid_to",
                                  path, lineno)
            if not valid_from < valid_to:
                raise SplitError(
                    f"{path}:{lineno} 别名区间必须非空: "
                    f"[{valid_from}, {valid_to})")
            entities.add(left)
            entities.add(right)
    finally:
        handle.close()
    return entities


def iter_alias_links(path):
    """流式逐行产出校验后的别名边字典（第二遍扫描，供构建边表使用）。"""
    handle, reader = _open_csv(path)
    try:
        for lineno, row in enumerate(reader, start=2):
            yield {
                "link_id": _need(row, "link_id", path, lineno),
                "left_entity": _need(row, "left_entity", path, lineno),
                "right_entity": _need(row, "right_entity", path, lineno),
                "valid_from": _parse_int(row.get("valid_from"),
                                         "valid_from", path, lineno),
                "valid_to": _parse_int(row.get("valid_to"),
                                       "valid_to", path, lineno),
            }
    finally:
        handle.close()
