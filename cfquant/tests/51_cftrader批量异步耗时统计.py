# -*- coding: utf-8 -*-
"""统计 cftrader 批量异步下单（order_stock_batch_async）的耗时。

与 50_真实下单50次统计.py 的区别：
  - 该脚本走的是 cftrader 批量接口，50 笔委托封装成 1 次 RPC 发出去，
    由 QMT 侧批量提交，而不是本地循环调用 50 次下单接口。
  - 因此 order_stock_batch_async 的“耗时”只代表一次批量请求的往返，
    不代表任何单笔委托的受理或成交耗时。

统计口径：
  - batch_call_ms : 一次 order_stock_batch_async 调用（提交->返回确认）的耗时
  - per_order_ms  : batch_call_ms / 笔数，作为分摊到单笔的等效值
  - 多轮 batch 后汇总 min/p50/p90/max/mean，避免单次采样的偶然性

用法：直接修改下方“用户配置区”后运行本文件；不读取命令行参数。

注意：ROUNDS > 0 且 DRY_RUN 为 False 时会提交真实委托（每轮 ORDER_COUNT 笔）。
"""

from types import SimpleNamespace
import contextlib
import io
import json
import statistics
import sys
import time

from cfquant import configure
from cfquant.cftrader import CfQuantTrader
from cfquant.xtconstant import FIX_PRICE, STOCK_BUY, STOCK_SELL
from cfquant.xttrader import XtQuantTrader, XtQuantTraderCallback, close_trade_client
from cfquant.xttype import StockAccount


# ======================== 用户配置区 ========================
TRANSPORT = "auto"        # auto 自动发现；也可填写 ctypes、web_lttx 或 lttx
BRIDGE_ID = "default"
REQUEST_TIMEOUT = 30.0    # 批量请求超时（批量要比单笔给得更宽松）
ACCOUNT_ID = "10110148"
ACCOUNT_TYPE = "STOCK"
STOCK_CODE = "000001.SZ"
SIDE = "buy"              # buy=买入，sell=卖出
PRICE = 11.5
VOLUME = 100
PRICE_TYPE = FIX_PRICE
STRATEGY_NAME = "cfquant_batch_async_bench"

ORDER_COUNT = 50          # 每次批量提交的笔数
ROUNDS = 3                # 批量重复轮数（用于取 p50；1=只跑一次）
ROUND_INTERVAL = 0.5      # 两轮之间的间隔（秒）
ORDER_REMARK_PREFIX = ""  # 留空自动生成

STOP_ON_ERROR = False
JSON_OUTPUT = False       # True 时打印逐轮 JSON
DRY_RUN = False           # True 只预览配置，不连接也不下单
# 注意：DRY_RUN 为 False 时会提交 ROUNDS * ORDER_COUNT 笔真实委托。
# ===========================================================

LOG_PREFIX = "【批量异步耗时统计】"


def configure_stdout():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def elapsed_ms(started, ended=None):
    ended = time.perf_counter() if ended is None else ended
    return round((ended - started) * 1000, 3)


def print_info(message, **fields):
    if not fields:
        print("%s%s" % (LOG_PREFIX, message), flush=True)
        return
    parts = []
    for key, value in fields.items():
        if value is None or value == "":
            value = "-"
        elif str(key).endswith("_ms"):
            value = "%sms" % value
        parts.append("%s=%s" % (key, value))
    print("%s%s | %s" % (LOG_PREFIX, message, " ".join(parts)), flush=True)


def print_json(payload):
    if JSON_OUTPUT:
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


@contextlib.contextmanager
def capture_external_output():
    captured = []
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            yield captured
    finally:
        for stream_name, buffer in (("stdout", stdout_buffer), ("stderr", stderr_buffer)):
            for line in buffer.getvalue().splitlines():
                line = line.strip()
                if line:
                    captured.append((stream_name, line))


def normalize_stock_code(stock_code):
    stock_code = str(stock_code or "").strip().upper()
    if "." in stock_code or not stock_code:
        return stock_code
    if stock_code.startswith(("5", "6", "9")):
        return "%s.SH" % stock_code
    return "%s.SZ" % stock_code


def order_type_for_side(side):
    return STOCK_BUY if str(side or "").strip().lower() == "buy" else STOCK_SELL


def percentile(values, fraction):
    """线性插值分位数，与 statistics.median / numpy 默认口径一致。"""
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def stats(values):
    values = [v for v in values if v is not None]
    if not values:
        return {}
    values = sorted(values)
    result = {
        "count": len(values),
        "min_ms": round(values[0], 3),
        "max_ms": round(values[-1], 3),
        "mean_ms": round(statistics.mean(values), 3),
        "p50_ms": round(percentile(values, 0.50), 3),
        "p90_ms": round(percentile(values, 0.90), 3),
        "p95_ms": round(percentile(values, 0.95), 3),
    }
    if len(values) > 1:
        result["stdev_ms"] = round(statistics.pstdev(values), 3)
    return result


def build_orders(account_id, stock_code, order_type, volume, price, remark_prefix, round_index, count):
    return [
        {
            "stock_code": stock_code,
            "order_type": order_type,
            "order_volume": volume,
            "price_type": PRICE_TYPE,
            "price": price,
            "strategy_name": STRATEGY_NAME,
            "order_remark": "%s_r%02d_%03d" % (remark_prefix, round_index, index + 1),
        }
        for index in range(count)
    ]


def main():
    configure_stdout()
    config = SimpleNamespace(
        transport=TRANSPORT, bridge_id=BRIDGE_ID, timeout=REQUEST_TIMEOUT,
        account_id=ACCOUNT_ID, account_type=ACCOUNT_TYPE, stock_code=STOCK_CODE,
        side=SIDE, price=PRICE, volume=VOLUME, order_count=ORDER_COUNT,
        rounds=ROUNDS, round_interval=ROUND_INTERVAL, stop_on_error=STOP_ON_ERROR,
        dry_run=DRY_RUN,
    )

    transport = str(config.transport or "").strip().lower() or "auto"
    account_id = str(config.account_id or "").strip()
    account_type = str(config.account_type or "STOCK").strip().upper()
    side = str(config.side or "buy").strip().lower()
    stock_code = normalize_stock_code(config.stock_code)
    price = float(config.price or 0)
    volume = int(config.volume or 0)
    order_count = int(config.order_count or 0)
    rounds = int(config.rounds or 0)
    remark_prefix = str(ORDER_REMARK_PREFIX or "").strip() or (
        "batchasync_%s_%s" % (stock_code.replace(".", ""), int(time.time()))
    )

    print_info("测试启动", api="CfQuantTrader.order_stock_batch_async",
               transport=transport, dry_run=bool(config.dry_run),
               order_count=order_count, rounds=rounds)
    print_info("委托参数", account_id=account_id, side=side, stock_code=stock_code,
               price=price, volume=volume, remark_prefix=remark_prefix)
    print_info("批量说明：每轮 %d 笔合并为 1 次 RPC，共 %d 轮，合计 %d 笔"
               % (order_count, rounds, order_count * rounds))

    if side not in ("buy", "sell"):
        print_info("配置校验失败：SIDE 必须为 buy 或 sell")
        return 2
    if not account_id or not stock_code:
        print_info("参数校验失败：account_id / stock_code 不能为空")
        return 2
    if volume <= 0 or price <= 0:
        print_info("参数校验失败：volume / price 必须大于 0")
        return 2
    if order_count <= 0 or rounds <= 0:
        print_info("参数校验失败：ORDER_COUNT / ROUNDS 必须大于 0")
        return 2
    if config.dry_run:
        print_info("DRY_RUN = True：只打印配置，不连接也不下单")
        print_json({"case": "dry_run", "ok": True, "rounds": rounds, "order_count": order_count})
        return 0

    configure(transport=transport, bridge_id=config.bridge_id, timeout=config.timeout)
    account = StockAccount(account_id, account_type, config.bridge_id)
    trader = XtQuantTrader(callback=XtQuantTraderCallback(), account=account)
    api = CfQuantTrader(trader)

    round_records = []
    try:
        print_info("开始连接 cfquant 交易通道")
        connect_started = time.perf_counter()
        with capture_external_output() as logs:
            connect_result = trader.connect()
        print_info("连接结果", ok=connect_result == 0, result=connect_result,
                   latency_ms=elapsed_ms(connect_started))
        if connect_result != 0:
            print_info("连接失败，未提交委托",
                       error=getattr(trader, "last_connect_error", ""))
            return 1

        for round_index in range(1, rounds + 1):
            orders = build_orders(account_id, stock_code, order_type_for_side(side),
                                  volume, price, remark_prefix, round_index, order_count)
            started = time.perf_counter()
            error_text = ""
            try:
                with capture_external_output() as logs:
                    result = api.order_stock_batch_async(
                        account, orders,
                        strategy_name=STRATEGY_NAME,
                        order_remark=remark_prefix,
                        stop_on_error=config.stop_on_error,
                    )
                call_ms = elapsed_ms(started)
            except Exception as error:
                call_ms = elapsed_ms(started)
                result = None
                error_text = "%s: %s" % (type(error).__name__, error)

            record = {
                "round": round_index,
                "batch_call_ms": call_ms,
                "per_order_ms": round(call_ms / order_count, 3) if order_count else None,
            }
            if result is not None:
                record.update({
                    "total": result.get("total"),
                    "submitted": result.get("submitted"),
                    "failed": result.get("failed"),
                    "unknown": result.get("unknown"),
                    "skipped": result.get("skipped"),
                    "ok": result.get("ok"),
                    "batch_id": result.get("batch_id"),
                    "qmt_submit_ms": result.get("qmt_submit_ms"),
                })
            if error_text:
                record["error"] = error_text
            round_records.append(record)
            print_info("[轮 %02d/%02d]" % (round_index, rounds),
                       batch_call_ms=record["batch_call_ms"],
                       per_order_ms=record["per_order_ms"],
                       total=record.get("total"), submitted=record.get("submitted"),
                       failed=record.get("failed"), unknown=record.get("unknown"),
                       qmt_submit_ms=record.get("qmt_submit_ms"),
                       error=record.get("error"))
            print_json({"type": "round", **record})
            time.sleep(max(float(config.round_interval or 0), 0.0))

        total_orders = order_count * rounds
        call_ms_values = [r["batch_call_ms"] for r in round_records]
        wall_ms = round(sum(call_ms_values), 3)
        submitted = sum((r.get("submitted") or 0) for r in round_records)
        failed = sum((r.get("failed") or 0) for r in round_records)
        unknown = sum((r.get("unknown") or 0) for r in round_records)

        summary = {
            "type": "summary",
            "api": "CfQuantTrader.order_stock_batch_async",
            "order_count_per_round": order_count,
            "rounds": rounds,
            "total_orders": total_orders,
            "submitted": submitted,
            "failed": failed,
            "unknown": unknown,
            "batch_call_ms": stats(call_ms_values),
            "per_order_ms": stats([r["per_order_ms"] for r in round_records]),
            "submit_calls_total_ms": wall_ms,
            "per_order_effective_ms": round(wall_ms / total_orders, 3) if total_orders else None,
            "rounds_detail": round_records,
        }
        print_info("========== 汇总统计（批量异步）==========")
        print_info("提交情况", total=total_orders, submitted=submitted,
                   failed=failed, unknown=unknown)
        print_info("单次批量调用耗时 ms", **summary["batch_call_ms"])
        print_info("分摊单笔耗时 ms", **summary["per_order_ms"])
        print_info("等效总耗时", submit_calls_total_ms=wall_ms,
                   per_order_effective_ms=summary["per_order_effective_ms"])
        print_json(summary)
        return 0 if failed == 0 and unknown == 0 else 1
    finally:
        try:
            with capture_external_output() as logs:
                trader.disconnect()
        except Exception:
            pass
        try:
            with capture_external_output() as logs:
                close_trade_client()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
