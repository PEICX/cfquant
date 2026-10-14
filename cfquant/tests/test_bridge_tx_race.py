# -*- coding: utf-8 -*-
"""桥接层 self.tx 的关闭竞态回归测试。

历史问题：_send_response/_send_error/publish_callback_event 等直接引用
self.tx.push(...)，close() 在另一线程把 self.tx 置为 None 后，检查与
使用之间没有任何保护，回调/响应线程会 AttributeError 崩掉
（qmt_bridge 一直是局部快照写法，normal/tx 侧没有同步过去）。
修复：统一走 TxTradeBridge._push，推送前先做局部快照。
"""
import threading

from cfquant.normal_bridge import NormalQmtBridge
from cfquant.tx_trade_bridge import TxTradeBridge


class FakeTx:
    def __init__(self):
        self.pushes = []

    def push(self, key, payload, channel):
        self.pushes.append((key, payload, channel))
        return None


def _bridge(cls):
    bridge = cls(object(), ip="127.0.0.1", port=2049, show=False)
    bridge._log = lambda message: None
    return bridge


def test_send_response_and_error_tolerate_none_tx():
    bridge = _bridge(NormalQmtBridge)
    bridge.tx = None
    # 旧实现此处 AttributeError: 'NoneType' object has no attribute 'push'
    bridge._send_response({"client_id": "c1", "id": "r1"}, {"ok": 1})
    bridge._send_error({"client_id": "c1", "id": "r2"}, "boom")


def test_send_response_survives_concurrent_close_race():
    bridge = _bridge(NormalQmtBridge)
    bridge.tx = FakeTx()
    stop = threading.Event()

    def closer():
        # 模拟 close() 在另一个线程把 tx 置空
        while not stop.is_set():
            bridge.tx = None

    worker = threading.Thread(target=closer)
    worker.start()
    try:
        for _ in range(2000):
            bridge._send_response({"client_id": "c1", "id": "r"}, {"ok": 1})
    finally:
        stop.set()
        worker.join(timeout=5)


def test_push_snapshot_uses_captured_tx_and_swallows_push_error():
    bridge = _bridge(TxTradeBridge)
    fake = FakeTx()
    bridge.tx = fake
    bridge._push("event", "payload", "chan-1")
    assert fake.pushes == [("event", "payload", "chan-1")]
    # push 返回错误 dict 时只记日志不抛异常
    bridge.tx = type("BadTx", (), {"push": staticmethod(lambda *a: {"code": -1, "msg": "down"})})()
    bridge._push("event", "payload", "chan-1")
