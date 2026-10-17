#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""单跳命名管道调用端：直接连 QMT 内嵌的 NPSH_LAB 命名管道 server。

用法（外部任意 Python 3，与 QMT 内嵌 3.6 同协议）::

    python qmt_scripts/npsh_lab_client.py                    # 连真实 QMT：ping + get_etf_iopv 延迟统计
    python qmt_scripts/npsh_lab_client.py 510050.SH 159915.SZ
    python qmt_scripts/npsh_lab_client.py --mock            # 本进程模拟 QMT 侧，自测全链路
    python qmt_scripts/npsh_lab_client.py --stats 50        # 50 次查询延迟统计

作为库使用（"外面调用和本地调用一样"）::

    from npsh_lite import NpshClient
    qmt = NpshClient()                                   # 默认 \\\\.\\pipe\\cfquant_singlehop
    iopv = qmt.get_etf_iopv("510050.SH")                  # 直接转发，签名与 QMT 内一致
    qmt.on_event = lambda name, data: ...                 # 事件回调（server 推送）

管道名默认 \\\\.\\pipe\\cfquant_singlehop，可用环境变量 CFQUANT_SINGLEHOP_PIPE
或 NpshClient(pipe_name=...) 覆盖。真实下单走 --passorder（需显式 --yes）。
"""

import os
import sys
import threading
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import npsh_lite


# --------------------------------------------------------------------------
# --mock: simulate the QMT side in-process (real NPSH_LAB lifecycle)
# --------------------------------------------------------------------------


def _fake_get_etf_iopv(stockcode):
    # 确定性替身：不依赖 QMT 终端，用于验证整条链路
    text = stockcode if isinstance(stockcode, str) else str(stockcode)
    seed = sum(bytearray(text.encode("utf-8")))
    return round(0.5 + (seed % 1000) / 1000.0, 6)


class _MockQmtTimer(object):
    """模拟 ContextInfo.schedule_run：独立线程按 interval 节拍回调，
    站位 QMT 策略主线程（passorder 泵的执行线程）。"""

    def __init__(self, func, interval):
        self.func = func
        self.interval = max(0.001, interval.total_seconds() if interval else 0.01)
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._loop)
        self.thread.daemon = True
        self.thread.start()

    def _loop(self):
        while self.running:
            time.sleep(self.interval)
            if not self.running:
                break
            try:
                self.func()
            except Exception:
                pass

    def stop(self):
        self.running = False


class FakeContextInfo(object):
    def __init__(self):
        self.timers = []

    def schedule_run(self, func, first_time, repeat_times=-1, interval=None, name=""):
        timer = _MockQmtTimer(func, interval)
        timer.start()
        self.timers.append(timer)
        return len(self.timers)

    def stop_timers(self):
        for timer in self.timers:
            timer.stop()
        self.timers = []


def run_mock(codes):
    import NPSH_LAB

    NPSH_LAB.get_etf_iopv = _fake_get_etf_iopv

    def _fake_passorder(*args):
        # 记录执行线程，便于人工核对 passorder 确实走的是泵线程
        print("[mock] passorder executed on thread: %s" % threading.current_thread().name)
        return 0

    NPSH_LAB.passorder = _fake_passorder

    ctx = FakeContextInfo()
    NPSH_LAB.init(ctx)
    try:
        return run_calls(codes, expect=_fake_get_etf_iopv)
    finally:
        NPSH_LAB.stop(ctx)
        ctx.stop_timers()


# --------------------------------------------------------------------------
# measurement / reporting
# --------------------------------------------------------------------------


def _percentile(samples, ratio):
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    idx = min(len(ordered) - 1, int(round(ratio * (len(ordered) - 1))))
    return ordered[idx]


def run_calls(codes, expect=None, repeat=20):
    client = npsh_lite.NpshClient()
    failures = 0
    try:
        t0 = time.perf_counter()
        pong = client.ping()
        t1 = time.perf_counter()
        print("ping -> %r  (%.3f ms)" % (pong, (t1 - t0) * 1000.0))
        for code in codes:
            latencies = []
            value = None
            for _ in range(repeat):
                t0 = time.perf_counter()
                value = client.get_etf_iopv(code)
                t1 = time.perf_counter()
                latencies.append((t1 - t0) * 1000.0)
            print("get_etf_iopv(%r) = %r  x%d  min/p50/p90/max = %.2f / %.2f / %.2f / %.2f ms"
                  % (code, value, repeat, min(latencies), _percentile(latencies, 0.5),
                     _percentile(latencies, 0.9), max(latencies)))
            if expect is not None and value != expect(code):
                print("  !! mismatch, expected %r" % expect(code))
                failures += 1
        return failures
    finally:
        client.close()


def run_passorder(args):
    # 真实下单路径：参数全部显式给出，--yes 之前只打印不执行。
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("code")
    parser.add_argument("side", choices=["buy", "sell"])
    parser.add_argument("volume", type=int)
    parser.add_argument("price", type=float)
    parser.add_argument("--account", required=True)
    parser.add_argument("--price-type", type=int, default=44, help="44=对手价")
    parser.add_argument("--yes", action="store_true")
    params = parser.parse_args(args)

    op_type = 23 if params.side == "buy" else 24
    kwargs = {
        "op_type": op_type,
        "order_type": 1101,
        "account": params.account,
        "code": params.code,
        "price_type": params.price_type,
        "price": params.price,
        "volume": params.volume,
        "strategy_name": "npsh_lab",
        "quick_trade": 2,
        "user_order_id": "",
    }
    print("passorder %s %s x%d @ %.3f account=%s price_type=%d"
          % (params.side, params.code, params.volume, params.price,
             params.account, params.price_type))
    if not params.yes:
        print("dry run only; append --yes to send the real order")
        return 0
    client = npsh_lite.NpshClient()
    try:
        t0 = time.perf_counter()
        result = client.passorder(**kwargs)
        t1 = time.perf_counter()
        print("passorder -> %r  (%.3f ms)" % (result, (t1 - t0) * 1000.0))
        return 0 if result == 0 else 1
    finally:
        client.close()


def main(argv):
    args = argv[1:]
    if args[:1] == ["--passorder"]:
        return run_passorder(args[1:])
    mock = "--mock" in args
    repeat = 20
    if "--stats" in args:
        idx = args.index("--stats")
        repeat = max(1, int(args[idx + 1]))
        args = args[:idx] + args[idx + 2:]
    codes = [a for a in args if not a.startswith("--")] or ["510050.SH"]
    if mock:
        print("[mock] simulating QMT side in-process")
        failures = run_mock(codes)
    else:
        failures = run_calls(codes, repeat=repeat)
    print("RESULT: %s" % ("OK" if failures == 0 else "FAILED (%d)" % failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
