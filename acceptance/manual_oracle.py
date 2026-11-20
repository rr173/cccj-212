"""独立手算 oracle（验收专用）。

本模块刻意不 import 任何生产代码，不使用生产库的版本选择逻辑、
窗口索引或 SQL 聚合，而是直接对小资料逐锚点、逐 K 枚举：

1. 先在 available_time<=cutoff 的行里逐 fact_id 选最高 revision；
2. retract 使该 ID 不可用；再按实体/类别/半开事件窗口筛选；
3. 三种运算各自独立解释空结果；
4. 标签只看 available_time<=K 的结局和 received_at<=K 的覆盖，
   区间并集闭合才能给 0/1。

整数运算这里也做与生产一致的溢出检查，保证极端用例语义对齐。
"""

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


class OracleError(Exception):
    pass


def _checked_add(a, b):
    r = a + b
    if not INT64_MIN <= r <= INT64_MAX:
        raise OracleError(f"端点相加溢出: {a}+{b}")
    return r


def _checked_sub(a, b):
    r = a - b
    if not INT64_MIN <= r <= INT64_MAX:
        raise OracleError(f"端点相减溢出: {a}-{b}")
    return r


def select_versions(facts, cutoff):
    """步骤 1：available_time<=cutoff 中逐 fact_id 取最高 revision。"""
    chosen = {}
    for fact in facts:
        if fact["available_time"] > cutoff:
            continue
        cur = chosen.get(fact["fact_id"])
        if cur is None or fact["revision"] > cur["revision"]:
            chosen[fact["fact_id"]] = dict(fact)
    # 低修订晚到不会反向改变已选高修订：选择只比较 revision 大小
    return chosen


def window_members(chosen, entity, category, cutoff, window):
    """步骤 2：在已选版本上按实体、类别与 [cutoff-window, cutoff) 筛选。"""
    lower = _checked_sub(cutoff, window)
    members = []
    for fact in chosen.values():
        if fact["op"] == "retract":
            continue  # 撤回使该 ID 整体不可用
        if (fact["entity"] == entity and fact["category"] == category
                and lower <= fact["event_time"] < cutoff):
            members.append(fact)
    return members


def oracle_feature(anchor, definition, facts):
    """返回 (value, status, provenance_set[(fact_id, revision), ...])。"""
    _eid, entity, cutoff = anchor
    chosen = select_versions(facts, cutoff)
    members = window_members(
        chosen, entity, definition["category"], cutoff,
        definition["window"])
    provenance = {(f["fact_id"], f["revision"]) for f in members}
    op = definition["op"]

    if op == "count":
        if not members:
            return 0, "COUNT_ZERO", provenance
        return len(members), "OK", provenance

    if op == "sum":
        nonnull = [f["value"] for f in members if f["value"] is not None]
        if not members:
            return None, "NO_ELIGIBLE_FACT", provenance
        if not nonnull:
            return None, "SUM_ALL_NULL", provenance
        # 无界整数精确求和
        total = 0
        for value in nonnull:
            total += value
        return total, "OK", provenance

    # latest
    if not members:
        return None, "LATEST_NO_ROW", provenance
    latest = max(members, key=lambda f: (f["event_time"], f["fact_id"]))
    return latest["value"], "OK", provenance


def merge_intervals(intervals):
    merged = []
    for a, b in sorted(intervals):
        if not a < b:
            raise OracleError(f"非法区间 [{a}, {b})")
        if merged and a <= merged[-1][1]:
            if b > merged[-1][1]:
                merged[-1] = (merged[-1][0], b)
        else:
            merged.append((a, b))
    return merged


def coverage_gaps(merged, lo, hi):
    gaps = []
    cursor = lo
    for a, b in merged:
        if b <= cursor:
            continue
        if a >= hi:
            break
        if a > cursor:
            gaps.append((cursor, min(a, hi)))
        cursor = max(cursor, b)
        if cursor >= hi:
            break
    if cursor < hi:
        gaps.append((cursor, hi))
    return gaps


def oracle_label(anchor, outcomes, coverage, horizon, as_of_k):
    """手算标签。返回 (status, observed_outcome_ids, gaps, merged_coverage)。"""
    if not isinstance(horizon, int) or horizon <= 0:
        raise OracleError(f"非法 H={horizon}")
    _eid, entity, cutoff = anchor
    if as_of_k < cutoff:
        raise OracleError(f"K={as_of_k} 早于 cutoff={cutoff}")
    obs_a = _checked_add(cutoff, 1)
    obs_b = _checked_add(_checked_add(cutoff, horizon), 1)

    usable_cov = [(c["a"], c["b"]) for c in coverage
                  if c["entity"] == entity and c["received_at"] <= as_of_k]
    merged = merge_intervals(usable_cov)
    gaps = coverage_gaps(merged, obs_a, obs_b)

    observed = [o for o in outcomes
                if o["entity"] == entity
                and o["available_time"] <= as_of_k
                and obs_a <= o["event_time"] < obs_b]
    observed_ids = [o["outcome_id"] for o in observed]

    if gaps:
        return "pending", observed_ids, gaps, merged
    if observed:
        return "positive", observed_ids, [], merged
    return "negative", [], [], merged
