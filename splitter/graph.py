"""关系图的内存表示、并查集与确定性路径搜索。

图中有三类节点（样本 example / 实体 entity / 事实 fact），编号分属
不同命名空间，在切分库内统一映射为整数 node_id，并用 kind 显式区分，
杜绝同名异类 ID 混淆。

边分两层：

1. **静态边**（与折无关）：样本--同实体、样本--事实（装配来路）。
   同一 fact_id 的不同修订只连同一个事实节点，不会被合并成伪造的
   单条来路；逐跳证据由 ``provenance_refs`` 中点名真实特征与修订。
2. **时效别名边**：实体--实体，带 ``link_id`` 与生效区间，
   仅当折的 ``link_at`` 落在 ``[valid_from, valid_to)`` 时生效。

内存策略（256 MiB 预算）：邻接用紧凑 ``array('i')`` CSR，不保留
Python 邻接表；并查集同样用 ``array('i')``。共享事实是星形结构
（样本只连事实节点），从不展开成样本两两配对。
"""

from array import array

from .errors import SplitError


class CSR:
    """只读 CSR 邻接。neighbors 全局按 node_id 升序，保证 BFS 确定性。"""

    __slots__ = ("offsets", "neighbors", "edge_ids")

    def __init__(self, offsets, neighbors, edge_ids=None):
        self.offsets = offsets              # array('i'), 长度 n_nodes+1
        self.neighbors = neighbors          # array('i')
        # 别名 CSR 每个邻接位对应一条 alias_edges.edge_id（平行边独立）
        self.edge_ids = edge_ids            # array('i') 或 None

    def range(self, node):
        lo = self.offsets[node]
        return lo, self.offsets[node + 1]


class DSU:
    """array 支撑的并查集：路径压缩 + 按规模合并。"""

    def __init__(self, n):
        self.parent = array('i', range(n))
        self.size = array('i', [1]) * n

    def find(self, x):
        parent = self.parent
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != x:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    def union(self, a, b):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]

    def reset(self):
        """逐格复位为单点 DSU，避免重新分配大块内存。"""
        parent = self.parent
        for i in range(len(parent)):
            parent[i] = i
        size = self.size
        for i in range(len(size)):
            size[i] = 1


def build_csr(n_nodes, pairs, with_edge_ids=False):
    """由 ``(a, b[, edge_id])`` 可迭代对象构建 CSR。

    静态边要求调用方先去重；别名边保留平行边。每个节点的邻接段按
    （邻居 node_id，edge_id）排序，因此输出与输入行序无关。
    """
    degree = array('i', [0]) * n_nodes
    recorded = []
    for item in pairs:
        a, b = item[0], item[1]
        degree[a] += 1
        degree[b] += 1
        recorded.append(item)
    offsets = array('i', [0])
    total = 0
    for d in degree:
        total += d
        offsets.append(total)
    neighbors = array('i', [0]) * total
    edge_ids = array('i', [0]) * total if with_edge_ids else None
    cursor = array('i', offsets[:-1])
    if with_edge_ids:
        recorded.sort(key=lambda t: (t[0], t[1], t[2]))
        for a, b, eid in recorded:
            p = cursor[a]
            neighbors[p] = b
            edge_ids[p] = eid
            cursor[a] = p + 1
            p = cursor[b]
            neighbors[p] = a
            edge_ids[p] = eid
            cursor[b] = p + 1
    else:
        recorded.sort(key=lambda t: (t[0], t[1]))
        for a, b in recorded:
            p = cursor[a]
            neighbors[p] = b
            cursor[a] = p + 1
            p = cursor[b]
            neighbors[p] = a
            cursor[b] = p + 1
    return CSR(offsets, neighbors, edge_ids)


def build_csr_streaming(n_nodes, edge_iterable, with_edge_ids=False):
    """两遍扫描构建 CSR，全程只持有紧凑 array，不缓存 Python 边列表。

    ``edge_iterable`` 要么是一个可迭代对象，要么是无参可调用（工厂），
    调用两次以完成两遍扫描；每条边为 ``(a, b)`` 或 ``(a, b, edge_id)``。
    第一遍统计度数，第二遍填充两端邻接位，最后按节点对邻接段排序，
    使邻居（及别名 edge_id）顺序与输入行序无关、完全确定。
    """
    def _iter():
        if callable(edge_iterable):
            return edge_iterable()
        return edge_iterable

    degree = array('i', [0]) * n_nodes
    for item in _iter():
        degree[item[0]] += 1
        degree[item[1]] += 1
    offsets = array('i', [0])
    total = 0
    for d in degree:
        total += d
        offsets.append(total)
    del degree
    neighbors = array('i', [0]) * total
    edge_ids = array('i', [0]) * total if with_edge_ids else None
    cursor = array('i', offsets[:-1])
    for item in _iter():
        a, b = item[0], item[1]
        p = cursor[a]
        neighbors[p] = b
        if with_edge_ids:
            edge_ids[p] = item[2]
        cursor[a] = p + 1
        p = cursor[b]
        neighbors[p] = a
        if with_edge_ids:
            edge_ids[p] = item[2]
        cursor[b] = p + 1
    del cursor
    # 逐节点排序邻接段：静态按邻居号；别名按 (邻居号, edge_id)
    for node in range(n_nodes):
        lo, hi = offsets[node], offsets[node + 1]
        if hi - lo <= 1:
            continue
        if with_edge_ids:
            seg = sorted(
                (neighbors[i], edge_ids[i]) for i in range(lo, hi))
            for k, (nb, eid) in enumerate(seg):
                neighbors[lo + k] = nb
                edge_ids[lo + k] = eid
        else:
            seg = sorted(neighbors[i] for i in range(lo, hi))
            for k, nb in enumerate(seg):
                neighbors[lo + k] = nb
    return CSR(offsets, neighbors, edge_ids)


def plan_edge_activity(folds_sorted, n_alias, alias_from, alias_to):
    """把每条别名边翻译成按折索引的生效/失效事件。

    ``folds_sorted`` 是按 ``fold_id`` 字节序确定的**执行序**，其
    ``link_at`` 未必有序（如 f100/f199/f200/f99）。内部先按 link_at
    排序做两次二分定位边命中的连续折段，再映射回执行序下标，因此：

    * 重复 link_at（多个不同折同一时刻）被正确一起命中；
    * 每条边只做两次二分，不逐折重扫全部长期活跃别名边；
    * 输出事件表按执行序排列，与折调度一致。

    ``alias_from``/``alias_to`` 为按下标 edge_id 排列的紧凑 int64 数组。
    返回 ``(starts, ends)``：长度均为折数的事件表；由于折 DSU 每折先
    复位，构建侧只需消费 ``starts``，``ends`` 仅供结构核对/解释。
    """
    import bisect
    n_folds = len(folds_sorted)
    # 执行序位置 -> 按 link_at 排序后的位置
    time_order = sorted(range(n_folds),
                        key=lambda i: folds_sorted[i]["link_at"])
    link_ats = [folds_sorted[i]["link_at"] for i in time_order]
    starts = [[] for _ in range(n_folds)]
    ends = [[] for _ in range(n_folds)]
    for edge_id in range(n_alias):
        valid_from = alias_from[edge_id]
        valid_to = alias_to[edge_id]
        first = bisect.bisect_left(link_ats, valid_from)
        last = bisect.bisect_left(link_ats, valid_to) - 1
        if first > last:
            continue
        for time_idx in range(first, last + 1):
            starts[time_order[time_idx]].append(edge_id)
        # last 折仍生效；其后第一个折起失效（若存在）
        if last + 1 < n_folds:
            ends[time_order[last + 1]].append(edge_id)
    for bucket in starts:
        bucket.sort()
    for bucket in ends:
        bucket.sort()
    return starts, ends


def bfs_path(static_csr, alias_csr, alias_from, alias_to,
             link_at, start, target):
    """在当折活跃图上做确定性 BFS，返回 node_id 最短路径（含首尾）。

    邻居按 node_id 升序展开（静态 CSR 与别名 CSR 各自有序，归并后
    仍有序），平行别名边按 edge_id 升序打破平局。因此找到的路径、
    选中的 ``link_id`` 都不受输入行序/折执行顺序影响。
    """
    if start == target:
        return [start]
    n = len(static_csr.offsets) - 1
    parent = array('i', [-1]) * n
    parent_via = array('b', [0]) * n     # 0=静态, 1=别名
    parent_edge = array('i', [-1]) * n  # 别名边 edge_id
    parent[start] = start
    queue = [start]
    head = 0
    found = False
    while head < len(queue):
        node = queue[head]
        head += 1
        s_lo, s_hi = static_csr.range(node)
        a_lo, a_hi = alias_csr.range(node)
        static_it = (static_csr.neighbors[i] for i in range(s_lo, s_hi))
        alias_it = (
            (alias_csr.neighbors[i], alias_csr.edge_ids[i])
            for i in range(a_lo, a_hi)
            if alias_from[alias_csr.edge_ids[i]] <= link_at
            < alias_to[alias_csr.edge_ids[i]])
        # 归并两路有序邻居；静态邻居为裸 int，别名邻居为 (nb, eid)
        for kind, nb, eid in _merge_neighbors(static_it, alias_it):
            if parent[nb] != -1:
                # 已发现的邻居（含 target）保持首次发现的父节点，
                # 不被后序展开覆盖，保证路径完全确定
                continue
            parent[nb] = node
            parent_via[nb] = kind
            parent_edge[nb] = eid if kind == 1 else -1
            if nb == target:
                found = True
                break
            queue.append(nb)
        if found:
            break
    if parent[target] == -1:
        raise SplitError(
            f"内部错误：节点 {start} 与 {target} 当折不连通，无法构造路径")
    path = [target]
    while path[-1] != start:
        path.append(parent[path[-1]])
    path.reverse()
    # 同时回传每跳使用的边类型与别名 edge_id
    via = []
    for i in range(1, len(path)):
        node = path[i]
        via.append((int(parent_via[node]),
                    int(parent_edge[node]) if parent_via[node] == 1 else None))
    return path, via


def _merge_neighbors(static_it, alias_it):
    """归并有序的静态邻居与别名邻居，输出 (kind, nb, eid)。

    静态邻居为 int，别名邻居为 (nb, eid)；同邻居只保留先出现的一条
    （静态优先），避免重复展开。
    """
    static_list = static_it if isinstance(static_it, list) else list(static_it)
    alias_list = list(alias_it)
    i = j = 0
    while i < len(static_list) or j < len(alias_list):
        if j >= len(alias_list) or (
                i < len(static_list) and static_list[i] <= alias_list[j][0]):
            nb = static_list[i]
            i += 1
            if j < len(alias_list) and alias_list[j][0] == nb:
                j += 1  # 同一邻居静态边优先，跳过别名平行项
            yield 0, nb, None
        else:
            nb, eid = alias_list[j]
            j += 1
            if i < len(static_list) and static_list[i] == nb:
                continue
            yield 1, nb, eid
