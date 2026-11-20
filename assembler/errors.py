"""装配器自定义异常。

任何输入/定义/配置级别的失败都抛出 :class:`AssemblyError`，
调用方（CLI）负责以非零退出码结束，保证“失败不交付完整训练表”。
"""


class AssemblyError(Exception):
    """输入资料、特征定义或装配参数不合法时抛出。"""
