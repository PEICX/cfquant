# -*- coding: utf-8 -*-
"""LTtx v2 二进制推送协议测试。

推送一律走 v2 快路径（二进制帧 + raw 投递，不保留旧 JSON 推送路径）；
get/put 等控制消息仍走 JSON 信封（帧体首字节判别）。

覆盖：
1. 帧结构与编解码（纯单元，不起服务器）
2. 端到端：真实 LTtx_server.py 子进程 + 真实 cfquant.tx.txl 客户端
   （push/订阅收发、d_push、get/put 复用、大载荷、同信道 fan-out）
3. 线程生命周期：run_on=False 后内部线程退出，懒脚本进程可正常结束

服务器子进程用桩模块替代 psutil/pandas 等重依赖，避免测试触发 pip 安装；
所有落盘目录通过环境变量隔离到 pytest 临时目录。
"""
import json
import os
import queue
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import cfquant.tx as tx_module
from cfquant.tx import _V2_FUNC_PUSH, _V2_PUSH_HEADER, _build_v2_push_frame, _recv_v2_delivery


SERVER_SOURCE = REPO_ROOT / "LTtx" / "tx" / "LTtx_server.py"
STUB_MODULES = ("psutil", "pandas", "tabulate", "pandas_market_calendars", "pytz")
LTTX_TOKEN = "LTtx"


def _free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _shutdown_client(client):
    client.run_on = False
    try:
        client.close_tx()
    except Exception:
        pass
    # 唤醒 __main_log 线程：run_on=False 后它仍可能阻塞在 log_que.get()
    try:
        client.log_que.put("")
    except Exception:
        pass


@pytest.fixture()
def lttx_server(tmp_path):
    server_dir = tmp_path / "server"
    server_dir.mkdir()
    shutil.copy(SERVER_SOURCE, server_dir / "LTtx_server.py")
    (server_dir / "Config.txt").write_text(
        "port:2049   \ntoken:LTtx      \nzmq_mode:False      \n"
        "zmq_port1:5555     \nzmq_port2:5556     \nset_cpu:  \n",
        encoding="utf-8",
    )
    runner = server_dir / "run_server.py"
    runner.write_text(
        "import os, runpy, sys, types\n"
        "for name in %r:\n"
        "    mod = types.ModuleType(name)\n"
        "    mod.__version__ = '9.9.9'\n"
        "    sys.modules[name] = mod\n"
        "os.environ['CFQUANT_LTTX_BACKGROUND'] = '1'\n"
        "runpy.run_path(%r, run_name='__main__')\n" % (STUB_MODULES, str(server_dir / "LTtx_server.py")),
        encoding="utf-8",
    )
    port = _free_port()
    runtime_dir = tmp_path / "runtime"
    env = dict(os.environ)
    env.update({
        "CFQUANT_LTTX_PORT": str(port),
        "CFQUANT_LTTX_RUNTIME_DIR": str(runtime_dir),
        "CFQUANT_LTTX_FILE_DATA_DIR": str(runtime_dir / "file_data"),
        "CFQUANT_LTTX_DATAFRAME_DIR": str(runtime_dir / "dataframe_data"),
        "CFQUANT_LTTX_DICT_VAR_FILE": str(runtime_dir / "data0.txt"),
        "CFQUANT_LTTX_LOG_DIR": str(tmp_path / "server_logs"),
        "CFQUANT_TX_LOG_DIR": str(tmp_path / "client_logs"),
        "CFQUANT_LOG_DIR": str(tmp_path / "shared_logs"),
        "CFQUANT_HOME": str(tmp_path / "home"),
    })
    proc = subprocess.Popen(
        [sys.executable, str(runner)],
        env=env,
        cwd=str(server_dir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            probe = socket.create_connection(("127.0.0.1", port), timeout=0.3)
            probe.close()
            break
        except OSError:
            if proc.poll() is not None:
                raise RuntimeError("LTtx server exited during startup")
            time.sleep(0.1)
    else:
        proc.kill()
        raise RuntimeError("LTtx server did not start listening")
    yield {"port": port, "proc": proc}
    proc.kill()
    proc.wait(timeout=5)


@pytest.fixture()
def tx_client(lttx_server, tmp_path, monkeypatch):
    monkeypatch.setenv("CFQUANT_TX_LOG_DIR", str(tmp_path / "client_logs"))
    client = tx_module.txl("127.0.0.1", lttx_server["port"], LTTX_TOKEN, show=False)
    client.start_tx()
    yield client
    _shutdown_client(client)


# ---------------------------------------------------------------------------
# 1. 帧结构与编解码
# ---------------------------------------------------------------------------

def test_build_v2_push_frame_layout():
    value = 'event|{"protocol":"cfquant","data":{"price": 10.5}}'
    frame = _build_v2_push_frame("chan-1", value)
    total_len = struct.unpack("Q", frame[:8])[0]
    assert total_len == len(frame) - 8
    body = frame[8:]
    func_id, who_len, value_len = _V2_PUSH_HEADER.unpack_from(body, 0)
    offset = _V2_PUSH_HEADER.size
    assert func_id == _V2_FUNC_PUSH
    assert body[offset:offset + who_len].decode("utf-8") == "chan-1"
    assert body[offset + who_len:].decode("utf-8") == value
    # 二进制推送帧首字节不能与 JSON 信封的 '{' 混淆
    assert body[0:1] != b"{"


def test_recv_v2_delivery_data_and_object_frames():
    a, b = socket.socketpair()
    try:
        value = "event|" + "x" * 5000
        payload = b"\x01" + value.encode("utf-8")
        a.sendall(struct.pack("Q", len(payload)) + payload)
        heartbeat = b"\x00" + json.dumps({"1": 1}).encode("utf-8")
        a.sendall(struct.pack("Q", len(heartbeat)) + heartbeat)
        assert _recv_v2_delivery(b) == value
        assert _recv_v2_delivery(b) is None
    finally:
        a.close()
        b.close()


def _bare_client(**attrs):
    client = tx_module.txl.__new__(tx_module.txl)
    client._txl__tx = object()  # 私有 __tx：非 False 即视为已连接
    client.tx_que = queue.Queue()
    for key, value in attrs.items():
        setattr(client, key, value)
    return client


def test_push_always_uses_v2_frame():
    client = _bare_client()
    client.push("event", '{"a":1}', "chan-1")
    item = client.tx_que.get_nowait()
    assert isinstance(item, tuple)
    assert item[0] == "v2"
    frame = item[1]
    assert frame[8:9] == b"\x01"
    total_len = struct.unpack("Q", frame[:8])[0]
    body = frame[8:]
    assert total_len == len(body)
    func_id, who_len, value_len = _V2_PUSH_HEADER.unpack_from(body, 0)
    offset = _V2_PUSH_HEADER.size
    assert func_id == _V2_FUNC_PUSH
    assert body[offset:offset + who_len].decode("utf-8") == "chan-1"
    assert body[offset + who_len:].decode("utf-8") == 'event|{"a":1}'


def test_d_push_and_sender_thread_are_mutually_exclusive():
    """d_push 直发与 main_tx_que 发送线程共用 put 连接，sendall 必须互斥。

    并发 sendall 遇到部分发送时，长度前缀帧会交错损坏协议。用可检测
    重叠的假 socket 验证两处发送从不同时在 sendall 里。
    """
    class RaceDetectingSocket:
        def __init__(self):
            self._guard = threading.Lock()
            self.inside = 0
            self.overlap = False
            self.calls = 0

        def sendall(self, data):
            with self._guard:
                self.inside += 1
                self.calls += 1
                if self.inside > 1:
                    self.overlap = True
            time.sleep(0.002)
            with self._guard:
                self.inside -= 1

    sock = RaceDetectingSocket()
    client = _bare_client(run_on=True, _tx_send_lock=threading.Lock())
    client._txl__tx = sock
    sender = threading.Thread(target=client.main_tx_que, daemon=True)
    sender.start()
    try:
        for _ in range(50):
            client.tx_que.put(("v2", b"\x01queued-frame"))
            client.d_push("event", "payload", "chan-1")
    finally:
        client.run_on = False
        client.tx_que.put(("v2", b"\x01stop"))
        sender.join(timeout=5)
    assert not sender.is_alive()
    # d_push 的 50 次同步发送必然全部完成；发送线程在退出前也已消费了
    # 大量队列帧（run_on 翻转后循环顶直接退出，尾部帧允许残留）
    assert sock.calls >= 50
    assert sock.overlap is False


# ---------------------------------------------------------------------------
# 2. 端到端：真实服务器 + 真实客户端
# ---------------------------------------------------------------------------

def test_v2_end_to_end_push_and_subscribe(tx_client):
    channel = "v2_e2e"
    tx_client.start_txg(channel)
    payload = '{"protocol":"cfquant","data":{"price": 10.5}}'
    tx_client.push("event", payload, channel)
    received = tx_client.Q.get(timeout=5)
    assert received == "event|" + payload


def test_d_push_v2_end_to_end(tx_client):
    channel = "v2_dpush"
    tx_client.start_txg(channel)
    tx_client.d_push("event", "direct-send", channel)
    assert tx_client.Q.get(timeout=5) == "event|direct-send"


def test_v2_connection_keeps_json_rpc(tx_client):
    # get/put 等低频控制消息在 put 连接上仍走 JSON 信封（首字节判别）
    tx_client.put("cfquant_test_var", "42")
    # put 在服务器侧异步落入 dict_var，get 需要短暂重试；值经 JSON 往返后是 int
    deadline = time.time() + 5
    value = None
    while time.time() < deadline:
        value = tx_client.get("cfquant_test_var")
        if value is not None:
            break
        time.sleep(0.1)
    assert value == 42


def test_get_list_value_indices_end_to_end(tx_client):
    # 回归：服务端旧实现的下标校验条件写反，合法下标恒返回 None
    tx_client.put("cfquant_test_list", ["a", "b", "c"])
    deadline = time.time() + 5
    value = None
    while time.time() < deadline:
        value = tx_client.get_list_value("cfquant_test_list", 1)
        if value is not None:
            break
        time.sleep(0.1)
    assert value == "b"
    assert tx_client.get_list_value("cfquant_test_list", -1) == "c"
    assert tx_client.get_list_value("cfquant_test_list", 99) is None
    assert tx_client.get_list_value("cfquant_test_missing", 0) is None


def test_get_dict_value_end_to_end(tx_client):
    tx_client.put("cfquant_test_dict", {"alpha": 1, "beta": "two"})
    deadline = time.time() + 5
    value = None
    while time.time() < deadline:
        value = tx_client.get_dict_value("cfquant_test_dict", "beta")
        if value is not None:
            break
        time.sleep(0.1)
    assert value == "two"
    assert tx_client.get_dict_value("cfquant_test_dict", "missing") is None
    assert tx_client.get_dict_value("cfquant_test_missing", "alpha") is None


def test_v2_push_with_large_unicode_payload(tx_client):
    channel = "v2_unicode"
    tx_client.start_txg(channel)
    payload = '{"msg":"' + "行情推送测试" * 200 + '"}'
    tx_client.push("event", payload, channel)
    assert tx_client.Q.get(timeout=5) == "event|" + payload


def test_multiple_subscribers_on_same_channel(lttx_server, tmp_path, monkeypatch):
    """同信道多个订阅者（fan-out）都收到快路径投递。"""
    monkeypatch.setenv("CFQUANT_TX_LOG_DIR", str(tmp_path / "logs"))
    port = lttx_server["port"]
    sender = tx_module.txl("127.0.0.1", port, LTTX_TOKEN, show=False)
    receiver = tx_module.txl("127.0.0.1", port, LTTX_TOKEN, show=False)
    try:
        sender.start_tx()
        receiver.start_tx()
        channel = "fanout_chan"
        sender.start_txg(channel)
        receiver.start_txg(channel)
        sender.push("event", "fanout_payload", channel)
        assert sender.Q.get(timeout=5) == "event|fanout_payload"
        assert receiver.Q.get(timeout=5) == "event|fanout_payload"
    finally:
        _shutdown_client(sender)
        _shutdown_client(receiver)


# ---------------------------------------------------------------------------
# 3. 线程生命周期：run_on=False 后内部线程必须退出，不挂住进程
# ---------------------------------------------------------------------------

def test_txl_log_thread_exits_after_run_on_false(monkeypatch, tmp_path):
    """__main_log 曾用无超时 log_que.get()，run_on=False 后永不退出。"""
    monkeypatch.setenv("CFQUANT_TX_LOG_DIR", str(tmp_path / "logs"))
    before = set(threading.enumerate())
    client = tx_module.txl("127.0.0.1", 1, LTTX_TOKEN, show=False)  # 不触发任何连接
    try:
        spawned = [t for t in threading.enumerate() if t not in before]
        assert spawned, "txl 构造后应启动 __main_log 线程"
        client.run_on = False
        deadline = time.time() + 3
        while time.time() < deadline and any(t.is_alive() for t in spawned):
            time.sleep(0.05)
        still_alive = [t.name for t in spawned if t.is_alive()]
        assert not still_alive, "run_on=False 后线程未退出: %s" % still_alive
    finally:
        client.run_on = False
        try:
            client.log_que.put("")
        except Exception:
            pass


def test_txl_internal_threads_are_daemon(lttx_server, tmp_path, monkeypatch):
    """txl 内部线程应全部为 daemon，短生命周期进程退出不被阻塞。"""
    monkeypatch.setenv("CFQUANT_TX_LOG_DIR", str(tmp_path / "logs"))
    before = set(threading.enumerate())
    client = tx_module.txl("127.0.0.1", lttx_server["port"], LTTX_TOKEN, show=False)
    try:
        client.start_tx()
        client.start_txg("daemon_check")
        client.push("event", "x", "daemon_check")
        assert client.Q.get(timeout=5) == "event|x"
        spawned = [t for t in threading.enumerate() if t not in before]
        assert spawned
        non_daemon = [t.name for t in spawned if not t.daemon]
        assert not non_daemon, "存在非 daemon 内部线程: %s" % non_daemon
    finally:
        _shutdown_client(client)


def test_lazy_script_process_exits(lttx_server, tmp_path):
    """模拟最懒的用法：不 close_tx、不清理，跑完就退出，进程必须正常结束。"""
    script = tmp_path / "lazy_script.py"
    script.write_text(
        "import os, sys\n"
        "sys.path.insert(0, %r)\n"
        "os.environ['CFQUANT_TX_LOG_DIR'] = %r\n"
        "import cfquant.tx as tx\n"
        "client = tx.txl('127.0.0.1', %d, 'LTtx', show=False)\n"
        "client.start_tx()\n"
        "client.start_txg('lazy_chan')\n"
        "client.push('event', 'bye', 'lazy_chan')\n"
        "assert client.Q.get(timeout=5) == 'event|bye'\n"
        "print('done')\n" % (str(REPO_ROOT), str(tmp_path / "lazy_logs"), lttx_server["port"]),
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "done" in result.stdout
