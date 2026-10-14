# -*- coding: utf-8 -*-
"""LTtx v2 二进制推送协议测试。

覆盖三块：
1. 帧结构与编解码（纯单元，不起服务器）
2. 端到端：真实 LTtx_server.py 子进程 + 真实 cfquant.tx.txl 客户端
   （v2 协商、push/订阅收发、d_push、同连接 get/put 的 JSON 复用）
3. 互操作：旧客户端（hello 不带 v）× 新服务器自动回落 JSON 信封；
   同信道混合 v1/v2 订阅者都能收到；客户端对"旧服务器"的协商回落。

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
    # txl.__main_log 在 run_on=False 后仍可能阻塞在 log_que.get() 上，
    # 塞一个哨兵让它醒来复查循环条件后退出
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
    client._tx_v2 = False
    for key, value in attrs.items():
        setattr(client, key, value)
    return client


def test_push_uses_v2_frame_when_negotiated():
    client = _bare_client(_tx_v2=True)
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
    assert body[offset + who_len:].decode("utf-8") == 'event|{"a":1}'


def test_push_falls_back_to_json_envelope_without_v2():
    client = _bare_client(_tx_v2=False, json=json, orjson_on=False)
    client.push("event", '{"a":1}', "chan-1")
    item = client.tx_que.get_nowait()
    assert not isinstance(item, tuple)
    msg = json.loads(item.decode("utf-8"))
    assert msg["func"] == "push"
    assert msg["who"] == "chan-1"
    assert msg["value"] == 'event|{"a":1}'


def test_push_v2_with_none_who_keeps_legacy_semantics():
    # who=None 在旧协议里被服务器静默丢弃，v2 分支应回落 JSON 信封保持同语义
    client = _bare_client(_tx_v2=True, json=json, orjson_on=False)
    client.push("event", '{"a":1}', None)
    item = client.tx_que.get_nowait()
    assert not isinstance(item, tuple)
    msg = json.loads(item.decode("utf-8"))
    assert msg["who"] is None


def test_start_tx_negotiation_reply_capture():
    a, b = socket.socketpair()
    try:
        client = tx_module.txl.__new__(tx_module.txl)
        a.sendall(json.dumps({"code": 0, "id_code": 1}).encode("utf-8"))
        assert client.recv_msg_start_tx(b) is True
        assert client._last_start_reply["code"] == 0
        assert not bool(client._last_start_reply.get("v2"))
    finally:
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# 2. 端到端：真实服务器 + 真实客户端
# ---------------------------------------------------------------------------

def test_v2_end_to_end_push_and_subscribe(tx_client):
    assert tx_client._tx_v2 is True
    channel = "v2_e2e"
    tx_client.start_txg(channel)
    assert tx_client.txg_dict[channel]["v2"] is True
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
    # get/put/心跳等低频控制消息在 v2 连接上仍走 JSON 信封（首字节判别）
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


def test_v2_push_with_large_unicode_payload(tx_client):
    channel = "v2_unicode"
    tx_client.start_txg(channel)
    payload = '{"msg":"' + "行情推送测试" * 200 + '"}'
    tx_client.push("event", payload, channel)
    assert tx_client.Q.get(timeout=5) == "event|" + payload


# ---------------------------------------------------------------------------
# 3. 互操作：新旧混用
# ---------------------------------------------------------------------------

def _recv_legacy_frame(sock, timeout=5.0):
    sock.settimeout(timeout)
    deadline = time.time() + timeout
    while time.time() < deadline:
        header = sock.recv(8, socket.MSG_WAITALL)
        size = struct.unpack("Q", header)[0]
        body = sock.recv(size, socket.MSG_WAITALL)
        data = json.loads(body.decode("utf-8"))
        if isinstance(data, str) and "|" in data:
            return data
    raise AssertionError("no data frame received before timeout")


def _connect_legacy_subscriber(port, channel):
    sub = socket.create_connection(("127.0.0.1", port), timeout=5)
    sub.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    hello = {"con_type": "push_mode", "who": channel, "pwd": "", "tocken": LTTX_TOKEN}
    sub.sendall(json.dumps(hello).encode("utf-8"))
    reply = json.loads(sub.recv(1024).decode("utf-8"))
    assert reply["code"] == 0
    assert "v2" not in reply
    return sub


def test_legacy_push_client_against_new_server(lttx_server):
    port = lttx_server["port"]
    put = socket.create_connection(("127.0.0.1", port), timeout=5)
    put.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    hello = {"con_type": "put_mode", "tocken": LTTX_TOKEN, "local_ip": "127.0.0.1",
             "current_file": "legacy", "current_dir": "/tmp"}
    put.sendall(json.dumps(hello).encode("utf-8"))
    reply = json.loads(put.recv(1024).decode("utf-8"))
    assert reply["code"] == 0
    assert "v2" not in reply

    sub = _connect_legacy_subscriber(port, "legacy_chan")
    try:
        envelope = json.dumps({"func": "push", "value": "legacy_chan|legacy_hello", "who": "legacy_chan"})
        put.sendall(struct.pack("Q", len(envelope)))
        put.sendall(envelope.encode("utf-8"))
        assert _recv_legacy_frame(sub) == "legacy_chan|legacy_hello"
    finally:
        put.close()
        sub.close()


def test_mixed_subscribers_on_same_channel(tx_client, lttx_server):
    channel = "mixed_chan"
    tx_client.start_txg(channel)  # v2 订阅者
    sub = _connect_legacy_subscriber(lttx_server["port"], channel)  # v1 订阅者
    try:
        tx_client.push("event", "mixed_payload", channel)
        assert tx_client.Q.get(timeout=5) == "event|mixed_payload"
        assert _recv_legacy_frame(sub) == "event|mixed_payload"
    finally:
        sub.close()


def test_client_falls_back_when_server_lacks_v2(monkeypatch, tmp_path):
    """新客户端 × 旧服务器：hello 带 v:2 但回复无 v2 时回落 JSON 信封。"""
    monkeypatch.setenv("CFQUANT_TX_LOG_DIR", str(tmp_path / "client_logs"))
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    observed = {}

    def fake_old_server():
        conn, _ = srv.accept()
        frames = []
        try:
            hello = json.loads(conn.recv(1024).decode("utf-8"))
            observed["hello_v"] = hello.get("v")
            conn.sendall(json.dumps({"code": 0, "id_code": 7}).encode("utf-8"))
            # 心跳线程可能先于测试自身的 push 发帧，循环读到目标帧为止
            while len(frames) < 20:
                header = conn.recv(8, socket.MSG_WAITALL)
                if not header:
                    break
                size = struct.unpack("Q", header)[0]
                body = conn.recv(size, socket.MSG_WAITALL)
                frames.append(json.loads(body.decode("utf-8")))
                if frames[-1].get("value") == "event|fallback":
                    break
        except Exception:
            pass
        finally:
            observed["frames"] = frames
            try:
                conn.close()
            except Exception:
                pass

    thread = threading.Thread(target=fake_old_server, daemon=True)
    thread.start()
    client = tx_module.txl("127.0.0.1", port, LTTX_TOKEN, show=False)
    try:
        client.start_tx()
        assert client._tx_v2 is False  # 旧服务器不回 v2，自动回落
        client.push("event", "fallback", "chan")
        thread.join(timeout=5)
        assert observed["hello_v"] == 2
        frames = observed["frames"]
        pushes = [f for f in frames if f.get("func") == "push"]
        assert pushes, frames
        assert any(f.get("value") == "event|fallback" for f in pushes)
        # 所有帧都是 JSON 信封 dict，证明确实回落到了 v1 协议
        assert all(isinstance(f, dict) for f in frames)
    finally:
        _shutdown_client(client)
        srv.close()
