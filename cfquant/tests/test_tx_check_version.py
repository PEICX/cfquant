# -*- coding: utf-8 -*-
"""txl.check_version 的回归测试。

历史问题：check_version 收到服务端 JSON 回复后没有 json.loads 就直接用
字典下标访问，check_version=True 时实例化 txl 必然 TypeError（对照
update_tx_version 是有 json.loads 的，属复制时丢失）。
"""
import json
import socket
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import cfquant.tx as tx_module


def test_check_version_parses_json_reply(monkeypatch):
    a, b = socket.socketpair()
    try:

        class FakeVersionSocket:
            def __init__(self, *args, **kwargs):
                self._peer = None

            def setsockopt(self, *args, **kwargs):
                pass

            def connect(self, address):
                self._peer = b

            def sendall(self, data):
                a.sendall(json.dumps({"LTtx_lastest": "7.2.8"}).encode("utf-8"))

            def recv(self, bufsize):
                return self._peer.recv(bufsize)

        class SocketModuleProxy:
            AF_INET = socket.AF_INET
            SOCK_STREAM = socket.SOCK_STREAM
            SOL_SOCKET = socket.SOL_SOCKET
            SO_RCVBUF = socket.SO_RCVBUF
            socket = FakeVersionSocket

        # 只替换 cfquant.tx 模块内的 socket 引用，不污染全局 socket 模块
        monkeypatch.setattr(tx_module, "socket", SocketModuleProxy)
        client = tx_module.txl.__new__(tx_module.txl)
        client.check_version_on = True
        client.sys_print_on = False
        client.__version__ = "7.2.8"
        messages = []
        client.sys_print = lambda msg, **kwargs: messages.append(msg)
        # 旧实现此处抛 TypeError: string indices must be integers
        client.check_version()
        assert any("最新" in str(m) for m in messages)
    finally:
        a.close()
        b.close()
