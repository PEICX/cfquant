# -*- coding: utf-8 -*-
"""真实下单 x N 次并统计耗时（基于 5_真实下单测试.py 改造）。

用法：直接修改下方“用户配置区”后运行本文件，不读取命令行参数。

ORDER_MODE 选择下单方式：
  - sync  : trader.order_stock()        同步下单，直接返回委托编号
  - async : trader.order_stock_async()  异步下单，返回请求序号(seq)，
            真实委托编号经 on_order_stock_async_response 回调按 seq 返回

统计口径（每次循环单独计时）：
  - order_call_ms            : 下单接口调用本身耗时（提交到接口返回）
  - async_response_ms        : (仅 async) 从提交到收到该 seq 异步反馈的耗时
  - query_visible_ms         : 从下单发起到委托表轮询到该委托的耗时
  - callback_from_submit_ms  : 从下单发起到收到 on_stock_order 回调的耗时
汇总输出 count / ok / min / max / mean / p50 / p90 / stdev。

注意：ORDER_COUNT > 0 且 DRY_RUN 和 CONNECT_ONLY 都为 False 时会提交真实委托。
"""

from types import SimpleNamespace
import contextlib
import io
import json
import statistics
import threading
import time

from cfquant.xtconstant import FIX_PRICE, STOCK_BUY, STOCK_SELL
from cfquant.xttrader import XtQuantTrader, XtQuantTraderCallback, close_trade_client
from cfquant.xttype import StockAccount
from cfquant.tests._helpers import configure_cfquant, configure_stdout


# ======================== 用户配置区 ========================
TRANSPORT = "auto"        # auto 自动发现；也可填写 ctypes、web_lttx 或 lttx
BRIDGE_ID = "default"
REQUEST_TIMEOUT = 15.0
ACCOUNT_ID = "10110148"
ACCOUNT_TYPE = "STOCK"
STOCK_CODE = "000001.SZ"
SIDE = "buy"              # buy=买入，sell=卖出
PRICE = 11.5
VOLUME = 100
PRICE_TYPE = FIX_PRICE
STRATEGY_NAME = "cfquant_real_order_latency"
ORDER_REMARK_PREFIX = ""  # 留空自动生成前缀

ORDER_COUNT = 50          # 下单次数
INTERVAL = 0.2            # 两次下单之间的间隔（秒）
ORDER_MODE = "async"      # sync=同步 order_stock；async=异步 order_stock_async
FIND_ORDER_TIMEOUT = 5.0
CALLBACK_TIMEOUT = 5.0
QUERY_INTERVAL = 0.05

JSON_OUTPUT = False       # True 额外打印逐次 JSON（机器可读）
SHOW_TRANSPORT_LOG = False
DRY_RUN = False           # True 只预览配置，不连接
CONNECT_ONLY = False      # True 只连接并订阅账号，不下单
ALLOW_STOCK_FALLBACK = False
# 注意：DRY_RUN 和 CONNECT_ONLY 都为 False 时会按以上配置提交真实委托。
# ===========================================================

LOG_PREFIX = "【下单50次统计】"
JSON_OUTPUT_ENABLED = False


def elapsed_ms(started, ended=None):
    ended = time.perf_counter() if ended is None else ended
    return round((ended - started) * 1000, 3)


def seconds_to_ms(value):
    return round(float(value or 0) * 1000, 3)


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
    if JSON_OUTPUT_ENABLED:
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


def print_external_logs(stage, captured, show=False):
    if not captured or not show:
        return
    for stream_name, line in captured:
        print_info("外部日志", stage=stage, stream=stream_name, line=line)


def pick_value(obj, *names):
    if obj is None:
        return None
    for name in names:
        try:
            if isinstance(obj, dict) and name in obj:
                value = obj.get(name)
            else:
                value = getattr(obj, name)
        except Exception:
            continue
        if value not in (None, ""):
            return value
    return None


def normalize_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text in ("", "-1", "None", "none") else text


def normalize_stock_code(stock_code):
    stock_code = str(stock_code or "").strip().upper()
    if "." in stock_code or not stock_code:
        return stock_code
    if stock_code.startswith(("5", "6", "9")):
        return "%s.SH" % stock_code
    return "%s.SZ" % stock_code


def stock_code_from_obj(obj):
    stock_code = normalize_text(pick_value(obj, "stock_code", "code"))
    if stock_code:
        return normalize_stock_code(stock_code)
    instrument_id = normalize_text(pick_value(obj, "m_strInstrumentID", "instrument_id"))
    exchange_id = normalize_text(pick_value(obj, "m_strExchangeID", "exchange_id", "market"))
    if instrument_id and exchange_id:
        return normalize_stock_code("%s.%s" % (instrument_id, exchange_id))
    return normalize_stock_code(instrument_id)


def order_type_for_side(side):
    return STOCK_BUY if str(side or "").strip().lower() == "buy" else STOCK_SELL


def order_id_candidates(obj):
    return [
        normalize_text(pick_value(obj, "order_id", "m_nOrderID", "m_strOrderID")),
        normalize_text(pick_value(obj, "order_sysid", "m_strOrderSysID", "sysid")),
    ]


def match_order(obj, account_id, stock_code, order_id, order_remark, allow_stock_fallback=False):
    event_account_id = normalize_text(
        pick_value(obj, "account_id", "m_strAccountID", "m_strAccountId", "m_strAccount", "m_accountID")
    )
    if event_account_id and account_id and event_account_id != account_id:
        return ""

    target_order_id = normalize_text(order_id)
    if target_order_id:
        for candidate in order_id_candidates(obj):
            if candidate and candidate == target_order_id:
                return "order_id"

    target_remark = normalize_text(order_remark)
    candidate_remark = normalize_text(pick_value(obj, "order_remark", "remark", "m_strRemark", "m_strOrderRemark"))
    if target_remark and candidate_remark == target_remark:
        return "order_remark"

    candidate_stock_code = stock_code_from_obj(obj)
    if allow_stock_fallback and candidate_stock_code and candidate_stock_code == stock_code:
        return "stock_code_fallback"
    return ""


class OrderLatencyCallback(XtQuantTraderCallback):
    def __init__(self, account_id, stock_code, order_remark, allow_stock_fallback=False):
        self.account_id = account_id
        self.stock_code = stock_code
        self.order_remark = order_remark
        self.allow_stock_fallback = bool(allow_stock_fallback)
        self.order_id = ""
        self.submit_started_at = None
        self.order_returned_at = None
        self.events = []
        self.async_response = None
        self.lock = threading.RLock()
        self.changed = threading.Event()

    def mark_submit_started(self, started_at):
        with self.lock:
            self.submit_started_at = started_at
            self.events = []
            self.async_response = None
            self.changed.clear()

    def mark_order_returned(self, order_id, returned_at):
        with self.lock:
            self.order_id = normalize_text(order_id)
            self.order_returned_at = returned_at

    def on_order_stock_async_response(self, response):
        with self.lock:
            if self.submit_started_at is None:
                return
            at = time.perf_counter()
            self.async_response = {
                "order_id": normalize_text(pick_value(response, "order_id", "m_nRef", "m_nOrderID")),
                "seq": pick_value(response, "seq", "m_nSeq"),
                "error_msg": normalize_text(pick_value(response, "error_msg", "m_strErrorMsg")),
                "latency_from_submit_ms": elapsed_ms(self.submit_started_at, at),
            }
            if self.async_response["order_id"] and not self.order_id:
                self.order_id = self.async_response["order_id"]
            self.changed.set()

    def wait_async_response(self, deadline_perf):
        while True:
            with self.lock:
                if self.async_response is not None:
                    return dict(self.async_response)
            remaining = deadline_perf - time.perf_counter()
            if remaining <= 0:
                return None
            self.changed.wait(min(remaining, 0.05))
            self.changed.clear()

    def on_stock_order(self, order):
        self._capture("on_stock_order", order)

    def on_order_error(self, order_error):
        self._capture("on_order_error", order_error)

    def on_stock_trade(self, trade):
        self._capture("on_stock_trade", trade)

    def _capture(self, event_name, data):
        now = time.perf_counter()
        with self.lock:
            if self.submit_started_at is None:
                return
            matched_by = match_order(
                data,
                self.account_id,
                self.stock_code,
                self.order_id,
                self.order_remark,
                allow_stock_fallback=self.allow_stock_fallback,
            )
            if not matched_by:
                return
            self.events.append({
                "event": event_name,
                "matched_by": matched_by,
                "latency_from_submit_ms": elapsed_ms(self.submit_started_at, now),
                "latency_from_order_return_ms": (
                    elapsed_ms(self.order_returned_at, now)
                    if self.order_returned_at is not None
                    else None
                ),
            })
            self.changed.set()

    def first_event(self, event_name=None):
        with self.lock:
            for event in self.events:
                if event_name is None or event.get("event") == event_name:
                    return dict(event)
        return None

    def wait_first(self, event_name, deadline_perf):
        while True:
            event = self.first_event(event_name)
            if event is not None:
                return event
            remaining = deadline_perf - time.perf_counter()
            if remaining <= 0:
                return None
            self.changed.wait(min(remaining, 0.05))
            self.changed.clear()


class CallbackRouter(XtQuantTraderCallback):
    """固定注册到 trader 上的转发器，把回调转发给当前正在记录的 OrderLatencyCallback。"""

    def __init__(self):
        self.lock = threading.RLock()
        self.target = None

    def set_target(self, target):
        with self.lock:
            self.target = target

    def _forward(self, name, data):
        with self.lock:
            target = self.target
        if target is not None:
            handler = getattr(target, name, None)
            if callable(handler):
                handler(data)

    def on_stock_order(self, order):
        self._forward("on_stock_order", order)

    def on_order_error(self, order_error):
        self._forward("on_order_error", order_error)

    def on_stock_trade(self, trade):
        self._forward("on_stock_trade", trade)

    def on_order_stock_async_response(self, response):
        self._forward("on_order_stock_async_response", response)


def query_until_order_visible(
    trader, account, account_id, stock_code, order_id, order_remark,
    order_started_at, timeout, interval, allow_stock_fallback=False,
):
    deadline = time.perf_counter() + max(float(timeout or 0), 0.0)
    attempts = 0
    last_error = ""
    while True:
        attempts += 1
        query_started = time.perf_counter()
        try:
            orders = trader.query_stock_orders(account, cancelable_only=False) or []
            query_ended = time.perf_counter()
            for order in orders:
                if match_order(order, account_id, stock_code, order_id, order_remark,
                               allow_stock_fallback=allow_stock_fallback):
                    return {
                        "ok": True,
                        "latency_from_submit_ms": elapsed_ms(order_started_at, query_ended),
                        "attempts": attempts,
                    }
        except Exception as error:
            last_error = str(error)
        if time.perf_counter() >= deadline:
            return {
                "ok": False,
                "latency_from_submit_ms": elapsed_ms(order_started_at),
                "attempts": attempts,
                "error": last_error or "order not found by order_id or order_remark",
            }
        time.sleep(max(float(interval or 0), 0.0))


def stats(values):
    values = [v for v in values if v is not None]
    if not values:
        return {}
    values = sorted(values)
    result = {
        "count": len(values),
        "min_ms": round(values[0], 3),
        "max_ms": round(values[-1], 3),
        "mean_ms": round(statistics.fmean(values), 3),
        "p50_ms": round(values[len(values) // 2], 3),
        "p90_ms": round(values[min(len(values) - 1, int(len(values) * 0.9))], 3),
    }
    if len(values) > 1:
        result["stdev_ms"] = round(statistics.pstdev(values), 3)
    return result


def main():
    configure_stdout()
    global JSON_OUTPUT_ENABLED
    JSON_OUTPUT_ENABLED = bool(JSON_OUTPUT)

    config = SimpleNamespace(
        transport=TRANSPORT, bridge_id=BRIDGE_ID, timeout=REQUEST_TIMEOUT,
        account_id=ACCOUNT_ID, account_type=ACCOUNT_TYPE, stock_code=STOCK_CODE,
        side=SIDE, price=PRICE, volume=VOLUME, price_type=PRICE_TYPE,
        strategy_name=STRATEGY_NAME, order_remark_prefix=ORDER_REMARK_PREFIX,
        order_count=ORDER_COUNT, interval=INTERVAL, order_mode=ORDER_MODE,
        find_order_timeout=FIND_ORDER_TIMEOUT, callback_timeout=CALLBACK_TIMEOUT,
        query_interval=QUERY_INTERVAL, json=JSON_OUTPUT, show_transport_log=SHOW_TRANSPORT_LOG,
        dry_run=DRY_RUN, connect_only=CONNECT_ONLY, allow_stock_fallback=ALLOW_STOCK_FALLBACK,
    )

    config.transport = str(config.transport or "").strip().lower() or "auto"
    account_id = str(config.account_id or "").strip()
    account_type = str(config.account_type or "STOCK").strip().upper()
    side = str(config.side or "buy").strip().lower()
    stock_code = normalize_stock_code(config.stock_code)
    price = float(config.price or 0)
    volume = int(config.volume or 0)
    strategy_name = str(config.strategy_name or "cfquant_real_order_latency").strip()
    remark_prefix = str(config.order_remark_prefix or "").strip() or (
        "burst_%s_%s" % (stock_code.replace(".", ""), int(time.time()))
    )
    order_count = int(config.order_count or 0)
    order_mode = str(config.order_mode or "sync").strip().lower()

    api_name = ("XtQuantTrader.order_stock_async x%s" % order_count
                if order_mode == "async" else "XtQuantTrader.order_stock x%s" % order_count)
    print_info("测试启动", api=api_name, mode=order_mode,
               transport=config.transport, dry_run=bool(config.dry_run),
               connect_only=bool(config.connect_only))
    print_info("委托参数", account_id=account_id, side=side, stock_code=stock_code,
               price=price, volume=volume, order_count=order_count,
               interval_ms=seconds_to_ms(config.interval))
    print_json({"type": "start", "remark_prefix": remark_prefix, "order_count": order_count,
                "order_mode": order_mode})

    # 参数校验
    if side not in ("buy", "sell"):
        print_info("配置校验失败：SIDE 必须为 buy 或 sell")
        return 2
    if order_mode not in ("sync", "async"):
        print_info("配置校验失败：ORDER_MODE 只能为 sync 或 async")
        return 2
    if not account_id or not stock_code:
        print_info("参数校验失败：account_id / stock_code 不能为空")
        return 2
    if volume <= 0 or price <= 0:
        print_info("参数校验失败：volume / price 必须大于 0")
        return 2
    if order_count <= 0:
        print_info("参数校验失败：ORDER_COUNT 必须大于 0")
        return 2
    if config.dry_run:
        print_info("DRY_RUN = True：只打印配置，不连接也不下单")
        return 0

    configure_cfquant(config)
    account = StockAccount(account_id, account_type, config.bridge_id)
    router = CallbackRouter()
    trader = XtQuantTrader(callback=router, account=account)

    per_order = []  # 每次循环的计时记录
    try:
        print_info("开始连接 cfquant 交易通道")
        connect_started = time.perf_counter()
        with capture_external_output() as logs:
            connect_result = trader.connect()
        print_external_logs("connect", logs, show=config.show_transport_log)
        print_info("连接结果", ok=connect_result == 0, result=connect_result,
                   latency_ms=elapsed_ms(connect_started))
        if connect_result != 0:
            print_info("连接失败，未提交委托", error=getattr(trader, "last_connect_error", ""))
            return 1
        if config.connect_only:
            print_info("连接检查完成，未下单")
            return 0

        for index in range(1, order_count + 1):
            order_remark = "%s_%03d" % (remark_prefix, index)
            callback = OrderLatencyCallback(account_id, stock_code, order_remark,
                                            allow_stock_fallback=config.allow_stock_fallback)
            router.set_target(callback)

            order_started = time.perf_counter()
            callback.mark_submit_started(order_started)
            record = {"index": index, "order_remark": order_remark, "ok": False, "order_mode": order_mode}
            try:
                with capture_external_output() as logs:
                    if order_mode == "async":
                        seq = trader.order_stock_async(
                            account, stock_code, order_type_for_side(side), volume,
                            config.price_type, price, strategy_name, order_remark,
                        )
                    else:
                        seq = None
                        order_id = trader.order_stock(
                            account, stock_code, order_type_for_side(side), volume,
                            config.price_type, price, strategy_name, order_remark,
                        )
                print_external_logs("order_stock", logs, show=config.show_transport_log)
                order_ended = time.perf_counter()
                if order_mode == "async":
                    record["seq"] = seq
                    callback.mark_order_returned("", order_ended)
                else:
                    callback.mark_order_returned(order_id, order_ended)
            except Exception as error:
                record.update({
                    "order_call_ms": elapsed_ms(order_started),
                    "error": "%s: %s" % (type(error).__name__, error),
                })
                per_order.append(record)
                print_info("[%03d/%03d] 下单异常" % (index, order_count), **record)
                time.sleep(max(float(config.interval or 0), 0.0))
                continue

            record["order_call_ms"] = elapsed_ms(order_started, order_ended)

            deadline = order_started + max(float(config.callback_timeout or 0), 0.0)
            if order_mode == "async":
                # 异步下单：真实委托编号经 on_order_stock_async_response 按 seq 返回
                response = callback.wait_async_response(deadline)
                record["async_response_ms"] = response.get("latency_from_submit_ms") if response else None
                record["async_ok"] = response is not None
                if response is not None:
                    record["order_id"] = response.get("order_id") or ""
                    record["async_error"] = response.get("error_msg") or ""
                    order_id = response.get("order_id") or ""
                else:
                    record["order_id"] = ""
                    order_id = ""
            else:
                order_id = normalize_text(order_id)
                record["order_id"] = order_id

            visible = query_until_order_visible(
                trader, account, account_id, stock_code, order_id, order_remark,
                order_started, config.find_order_timeout, config.query_interval,
                allow_stock_fallback=config.allow_stock_fallback,
            )
            record["query_visible_ms"] = visible.get("latency_from_submit_ms")
            record["query_ok"] = bool(visible.get("ok"))

            deadline = order_started + max(float(config.callback_timeout or 0), 0.0)
            event = callback.wait_first("on_stock_order", deadline)
            record["callback_from_submit_ms"] = event.get("latency_from_submit_ms") if event else None
            record["callback_ok"] = event is not None
            match_keys = [record["query_ok"], record["callback_ok"]]
            if order_mode == "async":
                match_keys.append(record.get("async_ok"))
            record["ok"] = any(match_keys)

            per_order.append(record)
            print_info(
                "[%03d/%03d]" % (index, order_count),
                ok=record["ok"], order_id=record.get("order_id"),
                order_call_ms=record["order_call_ms"],
                async_response_ms=record.get("async_response_ms"),
                query_visible_ms=record.get("query_visible_ms"),
                callback_from_submit_ms=record.get("callback_from_submit_ms"),
            )
            print_json({"type": "order", **record})
            time.sleep(max(float(config.interval or 0), 0.0))

        # ===== 汇总统计 =====
        ok_count = sum(1 for r in per_order if r.get("ok"))
        summary = {
            "type": "summary",
            "order_mode": order_mode,
            "order_count": order_count,
            "ok_count": ok_count,
            "failed_count": order_count - ok_count,
            "order_call_ms": stats([r.get("order_call_ms") for r in per_order]),
            "async_response_ms": stats([r.get("async_response_ms") for r in per_order if r.get("async_ok")]),
            "query_visible_ms": stats([r.get("query_visible_ms") for r in per_order if r.get("query_ok")]),
            "callback_from_submit_ms": stats([r.get("callback_from_submit_ms") for r in per_order if r.get("callback_ok")]),
        }
        print_info("========== 汇总统计 (%s) ==========" % order_mode)
        print_info("成功/总数", ok=ok_count, total=order_count,
                   failed=order_count - ok_count)
        print_info("下单调用耗时 ms", **summary["order_call_ms"])
        if order_mode == "async":
            print_info("异步反馈耗时 ms", **summary["async_response_ms"])
        print_info("委托表可见耗时 ms", **summary["query_visible_ms"])
        print_info("回调(自提交)耗时 ms", **summary["callback_from_submit_ms"])
        print_json(summary)
        return 0 if ok_count == order_count else 1
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
