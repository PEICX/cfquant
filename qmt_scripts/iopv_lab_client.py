#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""IOPV 实验调用端：通过本地 AF_UNIX socket 文件调用 QMT 内的 get_etf_iopv。

用法（外部任意 Python 3）::

    python qmt_scripts/iopv_lab_client.py                  # 调真实 QMT（需已部署 IOPV_LAB.py 并运行）
    python qmt_scripts/iopv_lab_client.py 510050.SH 159915.SZ
    python qmt_scripts/iopv_lab_client.py --mock           # 本进程模拟 QMT，自测全链路

作为库使用（"外面调用和本地调用一样"）::

    from afunix_lite import LabClient
    qmt = LabClient()
    iopv = qmt.get_etf_iopv("510050.SH")   # 直接转发，签名与 QMT 内一致

socket 路径默认 D:\\cfquant\\runtime\\cfquant_afunix_lab.sock，
可用环境变量 CFQUANT_AFUNIX_SOCK 或 LabClient(sock_path=...) 覆盖。
"""

import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import afunix_lite


def _fake_get_etf_iopv(stockcode):
    # --mock 模式的确定性替身：不依赖 QMT 终端，用于验证整条链路
    text = stockcode if isinstance(stockcode, str) else str(stockcode)
    seed = sum(bytearray(text.encode("utf-8")))
    return round(0.5 + (seed % 1000) / 1000.0, 6)


def run_calls(codes, expect=None):
    client = afunix_lite.LabClient()
    try:
        t0 = time.perf_counter()
        pong = client.ping()
        t1 = time.perf_counter()
        print("ping -> %r  (%.3f ms)" % (pong, (t1 - t0) * 1000.0))
        failures = 0
        for code in codes:
            t0 = time.perf_counter()
            value = client.get_etf_iopv(code)
            t1 = time.perf_counter()
            print("get_etf_iopv(%r) = %r  (%.3f ms)" % (code, value, (t1 - t0) * 1000.0))
            if expect is not None and value != expect(code):
                print("  !! mismatch, expected %r" % expect(code))
                failures += 1
        return failures
    finally:
        client.close()


def run_mock(codes):
    # 在本进程里加载真正的 QMT 侧文件 IOPV_LAB.py，注入一个假的
    # get_etf_iopv（模拟 QMT 注入内建函数），走真实的 init/stop 生命周期
    import IOPV_LAB

    IOPV_LAB.get_etf_iopv = _fake_get_etf_iopv

    class _FakeContextInfo(object):
        pass

    ctx = _FakeContextInfo()
    IOPV_LAB.init(ctx)
    try:
        return run_calls(codes, expect=_fake_get_etf_iopv)
    finally:
        IOPV_LAB.stop(ctx)


def main(argv):
    args = argv[1:]
    mock = "--mock" in args
    codes = [a for a in args if not a.startswith("--")] or ["510050.SH"]
    if mock:
        print("[mock] simulating QMT side in-process")
        failures = run_mock(codes)
    else:
        failures = run_calls(codes)
    print("RESULT: %s" % ("OK" if failures == 0 else "FAILED (%d)" % failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
