"""标签装配：资料截至时刻 K + 接收覆盖闭合判定。

标签合同：对锚点 cutoff，观察 (cutoff, cutoff+H] 内是否存在结局。
离散整数时间下等价于半开区间 [cutoff+1, cutoff+H+1)。

硬性规则：

* 结局行与覆盖行都只使用 available_time / received_at <= K 的部分；
* received_at<=K 的覆盖区间并集必须完整覆盖观察区间，才能给 0/1；
* 即使已观察到正结局，只要覆盖未闭合仍标 pending，并单独保留已观察信息；
* 禁止用“没有结局行”充当完整负例——负例必须覆盖闭合且确实无结局；
* 相邻（端点相接）区间视为拼接成功；断裂留缺口。
"""

from .errors import AssemblyError
from .integers import checked_add


def merge_intervals(intervals):
    """合并半开整数区间；相邻（a 与 b 端点相等）自动拼接。"""
    merged = []
    for a, b in sorted(intervals):
        if not a < b:
            raise AssemblyError(f"覆盖区间非法，要求 a < b: [{a}, {b})")
        if merged and a <= merged[-1][1]:
            if b > merged[-1][1]:
                merged[-1] = (merged[-1][0], b)
        else:
            merged.append((a, b))
    return merged


def coverage_gaps(merged, lo, hi):
    """返回 [lo, hi) 未被 merged 并集覆盖的缺口区间列表（可为空）。"""
    if not lo < hi:
        raise AssemblyError(f"观察区间非法: [{lo}, {hi})")
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


def build_labels(conn, horizon, as_of_k):
    """为全部锚点构建标签，写入 labels / label_evidence。"""
    if not isinstance(horizon, int) or isinstance(horizon, bool) or horizon <= 0:
        # 无有效观察区间的非法 H：明确拒绝，不产出任何标签
        raise AssemblyError(
            f"horizon(H)={horizon!r} 非法：必须为正整数，观察区间不能为空")
    if not isinstance(as_of_k, int) or isinstance(as_of_k, bool):
        raise AssemblyError(f"资料截至时刻 K={as_of_k!r} 必须是整数")

    bad = conn.execute(
        "SELECT example_id, cutoff FROM anchors WHERE cutoff > ? LIMIT 1",
        (as_of_k,)).fetchone()
    if bad is not None:
        raise AssemblyError(
            f"K={as_of_k} 不得早于样本 cutoff：example_id={bad[0]} "
            f"cutoff={bad[1]}")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_avail "
                 "ON outcomes(entity, available_time, event_time, outcome_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_coverage_ent "
                 "ON coverage(entity, received_at, a, b, coverage_id)")

    label_rows = []
    evidence_rows = []
    # 全局证据编号；实体/锚点处理顺序与覆盖 ID 都已排序，
    # 因此编号与输入文件的行序无关、可复算
    evidence_seq = 0

    entities = [r[0] for r in conn.execute(
        "SELECT DISTINCT entity FROM anchors ORDER BY entity")]

    for entity in entities:
        # 只使用 received_at <= K 的覆盖证据
        raw_coverage = conn.execute(
            "SELECT a, b, received_at FROM coverage "
            "WHERE entity = ? AND received_at <= ? ORDER BY a, b",
            (entity, as_of_k)).fetchall()
        merged = merge_intervals([(a, b) for a, b, _r in raw_coverage])

        # 只使用 available_time <= K 的结局（不可变行，无版本选择问题）
        known_outcomes = conn.execute(
            "SELECT outcome_id, event_time, available_time FROM outcomes "
            "WHERE entity = ? AND available_time <= ? "
            "ORDER BY event_time, outcome_id",
            (entity, as_of_k)).fetchall()

        anchors = conn.execute(
            "SELECT example_id, cutoff FROM anchors "
            "WHERE entity = ? ORDER BY cutoff, example_id",
            (entity,)).fetchall()

        for example_id, cutoff in anchors:
            # (cutoff, cutoff+H] == [cutoff+1, cutoff+H+1)
            # 端点转换逐次做溢出检查
            obs_a = checked_add(cutoff, 1,
                                f"观察区间下界 cutoff+1 (example {example_id})")
            obs_b_excl = checked_add(
                cutoff, horizon,
                f"观察区间右端 cutoff+H (example {example_id})")
            obs_b = checked_add(
                obs_b_excl, 1,
                f"观察区间右端 +1 (example {example_id})")

            gaps = coverage_gaps(merged, obs_a, obs_b)

            # 无论闭合与否，先保留落在观察区间内的已接收结局信息
            observed = [(oid, et, at) for oid, et, at in known_outcomes
                        if obs_a <= et < obs_b]

            # 证据按 kind/坐标 确定性排序后编号，保证与输入行序无关
            pending_evidence = []

            def add_evidence(kind, ref_id, ea, eb, detail):
                pending_evidence.append((kind, ref_id, ea, eb, detail))

            if gaps:
                # 覆盖未闭合：即使有正结局也只能 pending
                label_rows.append((example_id, entity, cutoff, horizon,
                                   obs_a, obs_b, "pending"))
                for oid, et, at in observed:
                    add_evidence(
                        "positive_outcome", oid, et,
                        et + 1 if et < 2 ** 63 - 1 else None,
                        f"结局已在 available_time={at} 观察到，但覆盖未闭合，"
                        f"仅保留信息，标签仍为 pending")
                for ga, gb in gaps:
                    add_evidence(
                        "coverage_gap", None, ga, gb,
                        f"接收覆盖在 [{ga}, {gb}) 缺失，"
                        f"观察区间 [{obs_a}, {obs_b}) 未闭合")
            else:
                # 覆盖闭合：有结局为正例；无结局才是合法负例
                if observed:
                    label_rows.append((example_id, entity, cutoff, horizon,
                                       obs_a, obs_b, "positive"))
                    for oid, et, at in observed:
                        add_evidence(
                            "positive_outcome", oid, et,
                            et + 1 if et < 2 ** 63 - 1 else None,
                            f"正例依据：结局 available_time={at} <= K={as_of_k}")
                else:
                    label_rows.append((example_id, entity, cutoff, horizon,
                                       obs_a, obs_b, "negative"))
                # 闭合依据只记录与观察窗口相交的合并段（窗口仅 H 个点，
                # 证据量有界），避免把实体全量覆盖行复制到每个锚点
                for ma, mb in merged:
                    ca, cb = max(ma, obs_a), min(mb, obs_b)
                    if ca < cb:
                        ref = f"{entity}|{ca}|{cb}"
                        add_evidence(
                            "coverage_interval", ref, ca, cb,
                            f"覆盖闭合依据：并集段 [{ca}, {cb}) "
                            f"（received_at<=K 的覆盖合并而成）")

            # 确定性排序：结局 -> 缺口 -> 覆盖依据，同类按坐标/ID
            order = {"positive_outcome": 0, "coverage_gap": 1,
                     "coverage_interval": 2}
            pending_evidence.sort(
                key=lambda e: (order[e[0]],
                               e[2] if e[2] is not None else -1,
                               e[3] if e[3] is not None else -1,
                               e[1] or ""))
            for kind, ref_id, ea, eb, detail in pending_evidence:
                evidence_seq += 1
                evidence_rows.append(
                    (evidence_seq, example_id, kind, ref_id, ea, eb, detail))

        # 证据量有界但仍按行数及时 flush，控制 Python 侧占用
        if len(evidence_rows) >= 20000:
            _flush_labels(conn, label_rows, evidence_rows)

    _flush_labels(conn, label_rows, evidence_rows)


def _flush_labels(conn, label_rows, evidence_rows):
    conn.executemany(
        "INSERT INTO labels VALUES (?, ?, ?, ?, ?, ?, ?)", label_rows)
    conn.executemany(
        "INSERT INTO label_evidence "
        "(evidence_id, example_id, kind, ref_id, a, b, detail) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)", evidence_rows)
    label_rows.clear()
    evidence_rows.clear()


def pending_reason_stats(conn):
    """统计 pending 原因，返回可操作的汇总与缺口明细计数。"""
    summary = conn.execute(
        """
        SELECT status, COUNT(*) AS n
        FROM labels
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()
    gap_stats = conn.execute(
        """
        SELECT kind, COUNT(*) AS n
        FROM label_evidence
        WHERE kind IN ('coverage_gap', 'positive_outcome')
        GROUP BY kind
        ORDER BY kind
        """
    ).fetchall()
    # 缺口总长度（离散整数点个数），帮助判断补哪段覆盖最划算
    gap_length = conn.execute(
        "SELECT COALESCE(SUM(b - a), 0) FROM label_evidence "
        "WHERE kind = 'coverage_gap'"
    ).fetchone()[0]
    top_gaps = conn.execute(
        """
        SELECT example_id, a, b, (b - a) AS gap_len
        FROM label_evidence
        WHERE kind = 'coverage_gap'
        ORDER BY gap_len DESC, example_id
        LIMIT 20
        """
    ).fetchall()
    return {
        "label_status_counts": summary,
        "evidence_kind_counts": gap_stats,
        "total_gap_points": gap_length,
        "top_gaps": top_gaps,
    }
