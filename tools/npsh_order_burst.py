#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""单跳命名管道 passorder 连发统计：N 笔真实下单，总时间 / 平均 / 分位延迟。

用法（真实 QMT 需已部署 NPSH_LAB 并运行；账户为仿真盘）::

    python tools/npsh_order_burst.py                  # 默认 100 笔
    python tools/npsh_order_burst.py --count 50
    python tools/npsh_order_burst.py --code 159915.SZ --sell --volume 200

每笔带独立 user_order_id，返回 0 视为已受理；输出总耗时、平均单笔耗时、
min/p50/p90/max 与受理计数。
"""

import argparse
import os
import sys
import time

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO, "qmt_scripts"))

import npsh_lite  # noqa: E402


def _percentile(samples, ratio):
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(round(ratio * (len(ordered) - 1))))]


def main():
    parser = argparse.ArgumentParser(description="npsh passorder 连发统计")
    parser.add_argument("--count", type=int, default=100, help="下单笔数（默认 100）")
    parser.add_argument("--code", default="510050.SH")
    parser.add_argument("--account", default="10110148")
    parser.add_argument("--volume", type=int, default=100)
    parser.add_argument("--price", type=float, default=0.412)
    parser.add_argument("--price-type", type=int, default=44, help="44=对手价")
    parser.add_argument("--sell", action="store_true", help="卖出（默认买入）")
    parser.add_argument("--timeout", type=float, default=15.0, help="单笔请求超时秒数")
    args = parser.parse_args()

    op_type = 24 if args.sell else 23
    client = npsh_lite.NpshClient(timeout=args.timeout)
    latencies = []
    accepted = 0
    failures = []
    try:
        pong = client.ping()
        print("ping -> %r" % (pong,))

        total_start = time.perf_counter()
        for i in range(args.count):
            order_id = "npsh_burst_%d_%03d" % (int(time.time() * 1000), i)
            begin = time.perf_counter()
            try:
                result = client.passorder(
                    op_type=op_type,
                    order_type=1101,
                    account=args.account,
                    code=args.code,
                    price_type=args.price_type,
                    price=args.price,
                    volume=args.volume,
                    strategy_name="npsh_burst",
                    quick_trade=2,
                    user_order_id=order_id,
                )
            except Exception as exc:  # noqa: BLE001
                failures.append((i, order_id, repr(exc)))
                continue
            latencies.append((time.perf_counter() - begin) * 1000.0)
            if result == 0:
                accepted += 1
            else:
                failures.append((i, order_id, "passorder -> %r" % (result,)))
            if (i + 1) % 20 == 0:
                print("  ... %d/%d 笔完成，受理 %d" % (i + 1, args.count, accepted))
        total_ms = (time.perf_counter() - total_start) * 1000.0
    finally:
        client.close()

    print("=" * 52)
    print("笔数: %d  受理(返回0): %d  失败: %d" % (args.count, accepted, len(failures)))
    if latencies:
        print("总时间: %.1f ms" % total_ms)
        print("平均单笔: %.2f ms（含往返；总时间/笔数 = %.2f ms）"
              % (sum(latencies) / len(latencies), total_ms / args.count))
        print("单笔延迟 min/p50/p90/max = %.2f / %.2f / %.2f / %.2f ms"
              % (min(latencies), _percentile(latencies, 0.5),
                 _percentile(latencies, 0.9), max(latencies)))
    for item in failures[:10]:
        print("失败样例: %r" % (item,))
    return 0 if not failures and accepted == args.count else 1


if __name__ == "__main__":
    sys.exit(main())
