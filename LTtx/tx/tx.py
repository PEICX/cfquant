# -*- coding: utf-8 -*-
"""LTtx 通信客户端 —— cfquant/tx.py 的薄包装。

历史问题：本文件曾是 cfquant/tx.py 的完整副本，两份代码 96% 重复并双向
漂移（cfquant 侧修了 sendall 帧交错的并发 bug、LTtx 侧加了 d_push 专用
连接，互相都拿不到对方的修复）。

现在 cfquant/tx.py 是唯一源（包含两侧全部修复与功能）。本模块优先转发
到 cfquant.tx；仅当运行环境里没有 cfquant 包（例如独立部署的 LTtx 服务器
目录被单独拷走）时，才加载同目录的 _tx_embedded.py 离线副本兜底。

更新本文件的正确方式：修改 D:/cfquant/cfquant/tx.py，然后运行
tools/sync_tx_wrapper.py 重新生成 _tx_embedded.py。不要直接编辑副本。
"""
import os as _os
import sys as _sys

try:
    from cfquant.tx import *  # noqa: F401,F403
    from cfquant.tx import txl, _V2_FUNC_PUSH, _V2_PUSH_HEADER, _build_v2_push_frame, _recv_v2_delivery
except ImportError:  # 独立部署兜底：目录内嵌副本
    _here = _os.path.dirname(_os.path.abspath(__file__))
    if _here not in _sys.path:
        _sys.path.insert(0, _here)
    from _tx_embedded import *  # noqa: F401,F403
    from _tx_embedded import txl, _V2_FUNC_PUSH, _V2_PUSH_HEADER, _build_v2_push_frame, _recv_v2_delivery
