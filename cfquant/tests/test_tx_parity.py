# -*- coding: utf-8 -*-
"""tx 客户端单源化回归测试。

历史问题：cfquant/tx.py 与 LTtx/tx/tx.py 是两份 96% 重复的完整实现并双向
漂移——cfquant 侧修了 d_push 与发送线程并发 sendall 的帧交错 bug、加了有界
日志队列；LTtx 侧升级了 d_push 专用连接与 8.0.1 版本。两边互相拿不到对方
的修复。

现在 cfquant/tx.py 是唯一源（包含两侧全部修复）；LTtx/tx/tx.py 是薄包装，
LTtx/tx/_tx_embedded.py 是生成出来的离线兜底副本。本文件锁定三件事：
1. 内嵌副本与单一源不漂移（等价 tools/sync_tx_wrapper.py --check）
2. 薄包装在有 cfquant 包时确实转发到 cfquant.tx
3. d_push 走专用连接（不再与 main_tx_que 共用 socket），close_tx 一并关闭
"""
import subprocess
import sys
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import cfquant.tx as tx_module
from cfquant.tx import _build_v2_push_frame

WRAPPER_PATH = REPO_ROOT / "LTtx" / "tx" / "tx.py"
EMBEDDED_PATH = REPO_ROOT / "LTtx" / "tx" / "_tx_embedded.py"


def test_embedded_copy_does_not_drift_from_source():
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "tools" / "sync_tx_wrapper.py"), "--check"],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0, (
        "LTtx/tx/_tx_embedded.py 已与 cfquant/tx.py 漂移：请修改 cfquant/tx.py 后"
        "运行 python tools/sync_tx_wrapper.py 重新生成，不要直接编辑副本。输出：\n%s"
        % result.stdout
    )


def test_wrapper_file_is_thin_and_forwards_to_cfquant():
    source = WRAPPER_PATH.read_text(encoding="utf-8")
    assert "from cfquant.tx import" in source
    # 薄包装自身不允许再出现 txl 类定义（防止有人把副本改回来）
    assert "class txl" not in source
    assert len(source.splitlines()) < 40


def test_wrapper_resolves_to_canonical_module():
    # 在把 LTtx/tx 前置到 sys.path 的导入形态下（web 服务器的做法），
    # `import tx` 必须拿到 cfquant.tx 的实现，而不是任何独立副本
    code = (
        "import sys; sys.path.insert(0, %r); "
        "import tx; print(tx.txl.__module__)"
        % str(REPO_ROOT / "LTtx" / "tx")
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cfquant.tx"


def _bare_client(**attrs):
    client = tx_module.txl.__new__(tx_module.txl)
    client._txl__tx = object()
    client._txl__d_push_tx = False
    client.tx_que = __import__("queue").Queue()
    client.sys_print_on = False
    client.sys_print = lambda *args, **kwargs: None
    for key, value in attrs.items():
        setattr(client, key, value)
    return client


def test_d_push_uses_dedicated_connection_not_shared_socket():
    """d_push 必须走 __d_push_tx 专用连接：与 main_tx_que 发送线程的
    socket 完全隔离，即使无锁也不会交错。同时验证懒建立与 send 路径。"""
    class FakeDPushSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    fake = FakeDPushSocket()
    client = _bare_client(run_on=True, _tx_send_lock=threading.Lock())

    def fake_connect():
        client._txl__d_push_tx = fake

    client._txl__connect_d_push = fake_connect
    client.d_push("event", "payload", "chan-1")
    # 懒建立后专用连接已就位，且帧确实发到专用连接上
    assert client._txl__d_push_tx is fake
    assert len(fake.sent) == 1
    frame = fake.sent[0]
    # v2 帧结构：[Q 总长][func_id=1][who_len(H)][value_len(I)][who][value]
    import struct
    from cfquant.tx import _V2_PUSH_HEADER
    total_len = struct.unpack("Q", frame[:8])[0]
    assert total_len == len(frame) - 8
    body = frame[8:]
    func_id, who_len, value_len = _V2_PUSH_HEADER.unpack_from(body, 0)
    assert func_id == 1
    assert body[7:7 + who_len] == b"chan-1"
    assert body[7 + who_len:7 + who_len + value_len] == b"event|payload"


def test_d_push_reconnects_once_and_raises_when_still_failing():
    """d_push 发送失败：关闭坏连接、重连一次后重发；重连后仍失败才抛错。"""
    class FlakySocket:
        def __init__(self):
            self.calls = 0

        def sendall(self, data):
            self.calls += 1
            raise OSError("broken")

    flaky = FlakySocket()
    client = _bare_client(run_on=True, _tx_send_lock=threading.Lock())
    connect_calls = []

    def fake_connect():
        connect_calls.append(1)
        client._txl__d_push_tx = flaky

    client._txl__connect_d_push = fake_connect
    closed = []
    client.close_connection = lambda sock, name="tx": closed.append(name)
    with pytest.raises(OSError):
        client.d_push("event", "payload", "chan-1")
    # 首次懒建立 + 失败后重连 = 2 次连接尝试；两次发送都落在坏 socket 上
    assert len(connect_calls) == 2
    assert flaky.calls == 2
    assert closed


def test_close_tx_closes_dedicated_d_push_connection():
    closed = []
    client = _bare_client(run_on=True, _tx_send_lock=threading.Lock())
    client._txl__d_push_tx = object()
    client.close_connection = lambda sock, name="tx": closed.append(name)
    # close_tx 需要的最小属性
    client.tx_running = True
    client.txg_running = False
    client.tx_que = __import__("queue").Queue()
    client.close_tx()
    assert closed  # d_push 专用连接已被 close_tx 关闭
    assert client._txl__d_push_tx is False
