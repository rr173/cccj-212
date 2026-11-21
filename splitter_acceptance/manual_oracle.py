"""独立逐折枚举 oracle：直接按当折活跃边在小图上 BFS 求连通。

本模块刻意不 import 生产侧的 graph/pipeline，不使用其并查集、CSR 或
折调度结构；只根据装配库读出的原始关系（anchors、feature_provenance）
与别名文件原始行，用最直白的邻接集合 + BFS 重算每折每个样本的状态，
供验收测试与生产结果逐字段对账。

节点用 ``(kind, name)`` 元组表示，kind ∈ {'example','entity','fact'}，
三类 ID 分属不同命名空间，同名也不会混淆。
"""

from collections import defaultdict, deque


def build_raw_graph(anchors, provenance_pairs, alias_links):
    """构造原始邻接表。

    anchors: [(example_id, entity, cutoff)]
    provenance_pairs: {(example_id, fact_id)}（已按 (样本,事实) 去重，
        同一事实的多修订只连一条边）
    alias_links: [dict(link_id, left_entity, right_entity,
                       valid_from, valid_to)]
    """
    adj = defaultdict(set)
    for example_id, entity, _cutoff in anchors:
        ex = ("example", example_id)
        ent = ("entity", entity)
        adj[ex].add(ent)
        adj[ent].add(ex)
    for example_id, fact_id in provenance_pairs:
        ex = ("example", example_id)
        fact = ("fact", fact_id)
        adj[ex].add(fact)
        adj[fact].add(ex)
    # 保证只在别名文件出现的实体也有节点
    for link in alias_links:
        adj[("entity", link["left_entity"])]
        adj[("entity", link["right_entity"])]
    return adj


def active_alias(alias_links, link_at):
    """按半开区间 [valid_from, valid_to) 筛当折活跃边（平行边独立）。"""
    out = []
    for link in alias_links:
        if link["valid_from"] <= link_at < link["valid_to"]:
            out.append(link)
    return out


def _active_keys(alias_links, link_at):
    """活跃无向边的规范化键集合（两端按字节序排序，方向无关）。"""
    keys = set()
    for link in active_alias(alias_links, link_at):
        keys.add(tuple(sorted((link["left_entity"],
                               link["right_entity"]))))
    return keys


def connected_component(adj, alias_links, link_at, start):
    """从 start 出发，按当折活跃别名边 + 全部静态边 BFS 求连通组件。

    别名边不进静态邻接表（它们是时效的），遍历到实体节点时按当折
    活跃边即时扩展对端实体。
    """
    active = _active_keys(alias_links, link_at)
    alias_neighbors = defaultdict(set)
    for link in active_alias(alias_links, link_at):
        alias_neighbors[link["left_entity"]].add(link["right_entity"])
        alias_neighbors[link["right_entity"]].add(link["left_entity"])
    seen = {start}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        neighbors = list(adj[node])
        if node[0] == "entity":
            for other in alias_neighbors.get(node[1], ()):
                neighbors.append(("entity", other))
        for nb in neighbors:
            if nb in seen:
                continue
            seen.add(nb)
            queue.append(nb)
    return seen


def find_path(adj, alias_links, link_at, start, target):
    """确定性 BFS 返回一条最短路径（邻居按字节序展开）。"""
    alias_neighbors = defaultdict(set)
    for link in active_alias(alias_links, link_at):
        alias_neighbors[link["left_entity"]].add(link["right_entity"])
        alias_neighbors[link["right_entity"]].add(link["left_entity"])
    parent = {start: None}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        neighbors = list(adj[node])
        if node[0] == "entity":
            for other in alias_neighbors.get(node[1], ()):
                neighbors.append(("entity", other))
        for nb in sorted(neighbors, key=lambda t: (t[0], t[1].encode())):
            if nb in parent:
                continue
            parent[nb] = node
            if nb == target:
                queue.clear()
                break
            queue.append(nb)
    if target not in parent:
        return None
    path = [target]
    while path[-1] != start:
        path.append(parent[path[-1]])
    path.reverse()
    return path


def oracle_states(anchors, labels, horizon, provenance_pairs, alias_links,
                  fold):
    """逐样本枚举一折的期望状态。

    labels: {example_id: 'positive'/'negative'/'pending'}
    返回 {example_id: state}，状态词汇与生产库一致。
    """
    adj = build_raw_graph(anchors, provenance_pairs, alias_links)
    embargo_line = fold["eval_from"] - fold["gap"]
    eval_examples = set()
    prelim = {}
    for example_id, _entity, cutoff in anchors:
        in_train = fold["train_from"] <= cutoff < fold["train_to"]
        in_eval = fold["eval_from"] <= cutoff < fold["eval_to"]
        if not in_train and not in_eval:
            prelim[example_id] = "outside"
        elif in_eval:
            if labels[example_id] == "pending":
                prelim[example_id] = "pending"
            else:
                prelim[example_id] = "eval"
                eval_examples.add(example_id)
        else:
            obs_end = cutoff + horizon
            prelim[example_id] = (
                "embargo" if obs_end >= embargo_line else None)

    # eval 组件集合（pending 验证候选不是对手，但仍是图上中间点）
    eval_components = {}
    for ex in sorted(eval_examples):
        comp = connected_component(
            adj, alias_links, fold["link_at"], ("example", ex))
        eval_components[ex] = comp

    result = {}
    for example_id, _entity, _cutoff in anchors:
        state = prelim[example_id]
        if state is not None:
            result[example_id] = state
            continue
        comp = connected_component(
            adj, alias_links, fold["link_at"], ("example", example_id))
        opponents = sorted(e for e in eval_examples
                           if ("example", e) in comp)
        result[example_id] = (
            "related_eval" if opponents else "train")
    return result


def oracle_min_opponent(anchors, labels, horizon, provenance_pairs,
                        alias_links, fold, example_id):
    """返回同组件中字节序最小的 eval 对手及其真实路径。"""
    adj = build_raw_graph(anchors, provenance_pairs, alias_links)
    comp = connected_component(
        adj, alias_links, fold["link_at"], ("example", example_id))
    opponents = sorted(
        name for kind, name in comp
        if kind == "example"
        and labels.get(name) != "pending"
        and fold["eval_from"] <= dict(
            (e, c) for e, _en, c in anchors)[name] < fold["eval_to"])
    if not opponents:
        return None, None
    target = opponents[0]
    path = find_path(adj, alias_links, fold["link_at"],
                     ("example", example_id), ("example", target))
    return target, path
