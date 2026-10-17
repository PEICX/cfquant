# -*- coding: utf-8 -*-
"""单跳命名管道（npsh_lite / NPSH_LAB）自动化回归。

加载真实 QMT 侧策略文件 qmt_scripts/NPSH_LAB.py，注入假内建函数
（get_etf_iopv / passorder）与假 ContextInfo（schedule_run 10ms 泵线程），
从外部 client 走完整链路验证：

- 查询直执行：get_etf_iopv 在管道 serve 线程上执行，不经泵
- passorder 线程亲和：经 schedule_run 泵线程执行，绝不在 serve 线程
- 超时不掉线：慢请求超时后连接仍可用，迟到回包被安全丢弃
- 异常路径 / 白名单拒绝 / 并发在途请求 / 断线重挂 / 单实例单客户端
- 事件推送：serve 线程与外部线程并发写事件帧（overlapped 并发验证）
- 与 hub 链路并存：管道名隔离，互不影响

不要求启动 QMT、PipeHub 或网页，不执行真实交易。
"""

import os
import sys
import threading
import time
import uuid

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_HERE))
_SCRIPTS = os.path.join(_REPO, "qmt_scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

import npsh_lite  # noqa: E402
import NPSH_LAB  # noqa: E402
from npsh_lab_client import FakeContextInfo, _fake_get_etf_iopv  # noqa: E402


def _wait_for(condition, timeout=5.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return condition()


class _Recorder(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.query_threads = []
        self.order_calls = []

    def note_query(self):
        with self.lock:
            self.query_threads.append(threading.current_thread())

    def note_order(self, args, kwargs):
        with self.lock:
            self.order_calls.append((threading.current_thread(), args, kwargs))


@pytest.fixture
def qmt_mock(monkeypatch):
    """One fresh NPSH_LAB lifecycle on a unique pipe name."""
    pipe_name = "npsh_test_%s" % uuid.uuid4().hex[:8]
    monkeypatch.setenv(npsh_lite.PIPE_NAME_ENV, pipe_name)
    recorder = _Recorder()

    def fake_get_etf_iopv(stockcode):
        recorder.note_query()
        if stockcode == "SLOW.CODE":
            time.sleep(1.0)
        if stockcode == "BAD.CODE":
            raise ValueError("boom")
        return _fake_get_etf_iopv(stockcode)

    def fake_passorder(*args, **kwargs):
        recorder.note_order(args, kwargs)
        return 0

    # 真实 QMT 里内建函数由终端注入模块命名空间；mock 需要直接创建属性
    monkeypatch.setattr(NPSH_LAB, "get_etf_iopv", fake_get_etf_iopv, raising=False)
    monkeypatch.setattr(NPSH_LAB, "passorder", fake_passorder, raising=False)

    ctx = FakeContextInfo()
    NPSH_LAB.init(ctx)
    try:
        yield recorder
    finally:
        NPSH_LAB.stop(ctx)
        ctx.stop_timers()


def _client(**kwargs):
    return npsh_lite.NpshClient(timeout=kwargs.pop("timeout", 5.0), **kwargs)


def test_query_roundtrip_runs_on_serve_thread_not_pump(qmt_mock):
    client = _client()
    try:
        value = client.get_etf_iopv("510050.SH")
        assert value == _fake_get_etf_iopv("510050.SH")
    finally:
        client.close()

    assert qmt_mock.query_threads, "query was never executed"
    serve_thread = NPSH_LAB._nps_server._thread if NPSH_LAB._nps_server else None
    # 查询无 pump：读取线程直接执行，不在调用线程，也不在泵线程
    assert serve_thread is not None
    for thread in qmt_mock.query_threads:
        assert thread is serve_thread
        assert thread is not threading.current_thread()


def test_passorder_runs_on_pump_thread_with_positional_mapping(qmt_mock):
    client = _client()
    try:
        result = client.passorder(code="510050.SH", op_type=23, volume=100,
                                  price=0.5, account="10110148")
        assert result == 0
    finally:
        client.close()

    assert qmt_mock.order_calls, "passorder was never executed"
    serve_thread = NPSH_LAB._nps_server._thread
    for thread, args, kwargs in qmt_mock.order_calls:
        # passorder 线程亲和：在泵（schedule_run 定时器）线程执行，
        # 绝不在管道 serve 线程或测试主线程
        assert thread is not serve_thread
        assert thread is not threading.current_thread()
        # kwargs 按内置签名映射为位置参数（ContextInfo 追加在第 11 位）
        assert args[0] == 23          # op_type
        assert args[2] == "10110148"  # account
        assert args[3] == "510050.SH"  # code
        assert args[4] == 44          # price_type 默认（对手价）
        assert args[5] == 0.5         # price
        assert args[6] == 100         # volume
        assert len(args) in (10, 11)


def test_passorder_positional_args_pass_through(qmt_mock):
    client = _client()
    try:
        assert client.passorder(24, 1101, "A1", "159915.SZ", 44, 0.999, 200,
                                "strat", 2, "oid-1") == 0
    finally:
        client.close()

    thread, args, kwargs = qmt_mock.order_calls[-1]
    assert args[:10] == (24, 1101, "A1", "159915.SZ", 44, 0.999, 200,
                         "strat", 2, "oid-1")


def test_timeout_keeps_connection_and_discards_late_reply(qmt_mock):
    client = _client()
    try:
        # _timeout 是代理保留字：作为请求超时，不透传给远端函数
        with pytest.raises(npsh_lite.NpshTimeout):
            client.get_etf_iopv("SLOW.CODE", _timeout=0.3)
        # 超时后连接不应被丢弃（迟到回包按请求 id 安全丢弃）
        assert client.connected
        value = client.get_etf_iopv("510050.SH", _timeout=5.0)
        assert value == _fake_get_etf_iopv("510050.SH")
    finally:
        client.close()


def test_remote_exception_surfaces_as_error(qmt_mock):
    client = _client()
    try:
        with pytest.raises(npsh_lite.NpshRemoteError) as excinfo:
            client.get_etf_iopv("BAD.CODE")
        message = str(excinfo.value)
        assert "ValueError" in message and "boom" in message
        # 异常不应杀掉连接
        assert client.ping()["pong"] is True
    finally:
        client.close()


def test_whitelist_rejects_unknown_action(qmt_mock):
    client = _client()
    try:
        with pytest.raises(npsh_lite.NpshRemoteError) as excinfo:
            client.cancel_order_stock("whatever")
        assert "not allowed" in str(excinfo.value)
    finally:
        client.close()


def test_concurrent_inflight_requests_single_connection(qmt_mock):
    client = _client(timeout=10.0)
    results = {}
    errors = []

    def worker(idx):
        try:
            for round_no in range(10):
                code = "510050.SH" if round_no % 2 == 0 else "159915.SZ"
                results[(idx, round_no)] = client.get_etf_iopv(code)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    start = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(15.0)
    elapsed = time.perf_counter() - start
    client.close()

    assert not errors, errors
    assert len(results) == 80
    for (idx, round_no), value in results.items():
        expected = _fake_get_etf_iopv("510050.SH" if round_no % 2 == 0 else "159915.SZ")
        assert value == expected


def test_client_restart_rearms_same_instance(qmt_mock):
    first = _client()
    try:
        assert first.ping()["pong"] is True
    finally:
        first.close()

    server = NPSH_LAB._nps_server
    assert server is not None
    assert _wait_for(lambda: not server.client_connected, timeout=3.0)

    second = _client()
    try:
        assert second.ping()["pong"] is True
        assert second.get_etf_iopv("510050.SH") == _fake_get_etf_iopv("510050.SH")
    finally:
        second.close()


def test_second_client_rejected_while_first_connected(qmt_mock):
    first = _client()
    try:
        assert first.ping()["pong"] is True
        rival = npsh_lite.NpshClient(timeout=1.0, connect_timeout_ms=600)
        with pytest.raises(npsh_lite.NpshConnectError):
            rival.ping()
    finally:
        first.close()


def test_reinit_after_stop_schedules_pump_again(qmt_mock):
    # fixture 已 stop；重新 init 必须重建 server 并重新注册泵定时器
    ctx = FakeContextInfo()
    NPSH_LAB.init(ctx)
    try:
        client = _client()
        try:
            assert client.passorder(code="510050.SH", op_type=23, volume=100) == 0
        finally:
            client.close()
        assert qmt_mock.order_calls, "passorder pump did not run after re-init"
    finally:
        NPSH_LAB.stop(ctx)
        ctx.stop_timers()


def test_transport_event_push_and_cross_thread_write(monkeypatch):
    pipe_name = "npsh_test_evt_%s" % uuid.uuid4().hex[:8]
    monkeypatch.setenv(npsh_lite.PIPE_NAME_ENV, pipe_name)
    received = []

    def handler(action, args, kwargs):
        if action == "fire":
            # serve 线程在处理请求期间直接推事件
            server.send_event("tick", {"v": args[0] if args else None})
            return "fired"
        if action == "slow":
            time.sleep(0.3)
            return "slow-done"
        raise ValueError("unknown action: %s" % action)

    server = npsh_lite.NpshServer(pipe_name=pipe_name, handler=handler)
    server.start()
    try:
        client = npsh_lite.NpshClient(pipe_name=pipe_name, timeout=5.0)
        client.on_event = lambda name, data: received.append((name, data))
        try:
            assert client.fire(7) == "fired"
            assert _wait_for(lambda: ("tick", {"v": 7}) in received)

            # 外部线程在 serve 线程挂读/写期间并发推事件（overlapped 并发）
            def push_background():
                for i in range(5):
                    assert server.send_event("bg", i)

            bg = threading.Thread(target=push_background)
            bg.start()
            assert client.slow() == "slow-done"
            bg.join(2.0)
            assert not bg.is_alive()
            assert _wait_for(
                lambda: sum(1 for name, _ in received if name == "bg") == 5)
        finally:
            client.close()
    finally:
        server.close()


def test_single_client_policy_survives_protocol_garbage(monkeypatch):
    # 非 npsh 帧：server 丢弃该客户端并重新挂起等下一个客户端
    pipe_name = "npsh_test_garbage_%s" % uuid.uuid4().hex[:8]
    monkeypatch.setenv(npsh_lite.PIPE_NAME_ENV, pipe_name)
    server = npsh_lite.NpshServer(pipe_name=pipe_name, handler=lambda a, p, k: "ok")
    server.start()
    try:
        conn = npsh_lite.connect_pipe(pipe_name, timeout_ms=2000)
        try:
            conn.write_frame(b"not-a-npsh-payload")
        finally:
            time.sleep(0.2)
            conn.close()
        assert _wait_for(lambda: not server.client_connected, timeout=3.0)

        good = npsh_lite.NpshClient(pipe_name=pipe_name, timeout=5.0)
        try:
            assert good.anything() == "ok"
        finally:
            good.close()
    finally:
        server.close()


def test_pipe_name_isolated_from_hub():
    from cfquant import pipe_transport

    assert npsh_lite.DEFAULT_PIPE_NAME != pipe_transport.DEFAULT_PIPE_NAME
    assert "singlehop" in npsh_lite.DEFAULT_PIPE_NAME
    assert "hub" in pipe_transport.DEFAULT_PIPE_NAME


def test_lab_file_embeds_current_transport():
    # NPSH_LAB.py 内嵌传输块必须与 npsh_lite.py 同步（部署工具同款校验）
    with open(os.path.join(_SCRIPTS, "NPSH_LAB.py"), "rb") as f:
        lab = f.read().decode("ascii")
    with open(os.path.join(_SCRIPTS, "npsh_lite.py"), "rb") as f:
        transport = f.read().decode("ascii")
    begin = "# BEGIN NPSH_LITE_TRANSPORT"
    end = "# END NPSH_LITE_TRANSPORT"
    assert begin in lab and end in lab
    embedded = lab[lab.index(begin):lab.index(end)]
    # 内嵌块（去掉首行标记）应包含传输文件全部内容
    body = embedded.split("\n", 1)[1]
    for line in transport.rstrip("\n").split("\n"):
        assert line in body, "npsh_lite.py line missing in NPSH_LAB.py: %r" % line[:60]
