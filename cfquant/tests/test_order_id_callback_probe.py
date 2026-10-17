# -*- coding: utf-8 -*-
"""同步下单订单号解析：回调 id 一致性探测。

背景：passorder 不返回订单号，回调里的 m_nRef 与订单查询返回的 order_id 在部分
QMT 终端上值域不一致，因此默认必须回查完整 ORDER 列表（get_trade_detail_data 无
过滤/分页参数，QMT 侧恒为全量返回）。本测试验证探测能在可信终端上跳过全量查询、
在不可信终端上保持原有回查行为。
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cfquant.order_identity import order_remark_key
from cfquant.tx_trade_bridge import TxTradeBridge, _OrderIdCallbackProbe


class DummyContext(object):
    pass


def _base_order_params(**overrides):
    params = {
        "account": {"account_id": "A123", "account_type": "STOCK"},
        "stock_code": "000001.SZ",
        "order_type": 23,
        "order_volume": 100,
        "price_type": 11,
        "price": 10.0,
        # 零等待：让查询路径在一次未命中后立即返回，便于统计查询次数。
        "find_order_wait": 0,
    }
    params.update(overrides)
    return params


def _matching_detail(remark, order_id):
    return {
        "m_nRef": order_id,
        "m_strRemark": remark,
        "m_strInstrumentID": "000001",
        "m_strExchangeID": "SZ",
    }


def _make_bridge(query_calls, remarks_and_ids, callback_id=None):
    """query_calls 记录每次 get_trade_detail_data 的调用；remarks_and_ids 是查询返回的委托。"""
    data = [
        _matching_detail(remark, order_id) for remark, order_id in remarks_and_ids
    ]

    def get_trade_detail_data(*args):
        query_calls.append(args)
        return data

    bridge = TxTradeBridge(
        DummyContext(),
        show=False,
        globals_dict={"get_trade_detail_data": get_trade_detail_data},
    )
    return bridge


def test_probe_marks_terminal_trusted_after_consistent_samples():
    probe = _OrderIdCallbackProbe(sample_limit=2)
    assert not probe.trusted()
    probe.observe(700001, 700001)
    assert not probe.trusted(), "单次样本不足以判定可信"
    probe.observe(700002, 700002)
    assert probe.trusted()


def test_probe_disables_fast_path_on_first_mismatch():
    probe = _OrderIdCallbackProbe(sample_limit=2)
    probe.observe(700001, 700001)
    probe.observe(700002, 999002)  # 值域不一致
    assert not probe.trusted()
    # 关闭后不再重开
    probe.observe(700003, 700003)
    probe.observe(700004, 700004)
    assert not probe.trusted()


def test_probe_treats_string_and_int_ids_as_same_reference():
    probe = _OrderIdCallbackProbe(sample_limit=1)
    probe.observe("700001", 700001)
    assert probe.trusted()


def test_trusted_terminal_returns_callback_id_without_order_query():
    query_calls = []
    bridge = _make_bridge(query_calls, [])
    # 模拟探测已完成：终端可信。
    bridge.order_id_callback_probe.state = _OrderIdCallbackProbe.TRUSTED

    record = {
        "account_id": "A123",
        "stock_code": "000001.SZ",
        "order_remark": "remark-a",
        "strategy_name": "",
        "event": __import__("threading").Event(),
        "callback_order_id": 700001,
        "callback_seen_at": 1.0,
    }
    resolved = bridge._find_order_id(
        "A123", "STOCK", "remark-a", "", None,
        _base_order_params(order_remark="remark-a"),
        record,
    )
    assert resolved == 700001
    assert query_calls == [], "可信终端必须跳过完整 ORDER 全量查询"


def test_untrusted_terminal_still_queries_order_list():
    query_calls = []
    remark = "remark-a"
    bridge = _make_bridge(query_calls, [(remark, 700001)])
    assert not bridge.order_id_callback_probe.trusted()

    record = {
        "account_id": "A123",
        "stock_code": "000001.SZ",
        "order_remark": remark,
        "strategy_name": "",
        "event": __import__("threading").Event(),
        "callback_order_id": 700001,
        "callback_seen_at": 1.0,
    }
    resolved = bridge._find_order_id(
        "A123", "STOCK", remark, "", None,
        _base_order_params(order_remark=remark),
        record,
    )
    assert resolved == 700001
    assert len(query_calls) == 1, "不可信终端仍回查一次以核对回调 id"


def test_inconsistent_callback_id_falls_back_to_queried_id():
    query_calls = []
    remark = "remark-a"
    # 查询返回权威 id 999001，但回调声称 700001（值域不一致的终端）。
    bridge = _make_bridge(query_calls, [(remark, 999001)])

    record = {
        "account_id": "A123",
        "stock_code": "000001.SZ",
        "order_remark": remark,
        "strategy_name": "",
        "event": __import__("threading").Event(),
        "callback_order_id": 700001,
        "callback_seen_at": 1.0,
    }
    resolved = bridge._find_order_id(
        "A123", "STOCK", remark, "", None,
        _base_order_params(order_remark=remark),
        record,
    )
    assert resolved == 999001, "必须以查询结果为权威 id"
    assert not bridge.order_id_callback_probe.trusted()
    # 后续下单不再走快路
    query_calls.clear()
    bridge._find_order_id(
        "A123", "STOCK", remark, "", None,
        _base_order_params(order_remark=remark),
        dict(record, callback_order_id=700002),
    )
    assert query_calls, "一旦不一致，后续必须继续回查"


def test_env_off_disables_probe_fast_path(monkeypatch=None):
    import os
    saved = os.environ.get("CFQUANT_ORDER_ID_CALLBACK_PROBE")
    os.environ["CFQUANT_ORDER_ID_CALLBACK_PROBE"] = "off"
    try:
        probe = _OrderIdCallbackProbe(sample_limit=1)
        probe.observe(700001, 700001)
        assert not probe.trusted(), "显式关闭时永不启用快路"
    finally:
        if saved is None:
            os.environ.pop("CFQUANT_ORDER_ID_CALLBACK_PROBE", None)
        else:
            os.environ["CFQUANT_ORDER_ID_CALLBACK_PROBE"] = saved


def test_env_force_enables_fast_path():
    import os
    saved = os.environ.get("CFQUANT_ORDER_ID_CALLBACK_PROBE")
    os.environ["CFQUANT_ORDER_ID_CALLBACK_PROBE"] = "on"
    try:
        probe = _OrderIdCallbackProbe()
        assert probe.trusted(), "强制开启时直接信任回调 id"
    finally:
        if saved is None:
            os.environ.pop("CFQUANT_ORDER_ID_CALLBACK_PROBE", None)
        else:
            os.environ["CFQUANT_ORDER_ID_CALLBACK_PROBE"] = saved


if __name__ == "__main__":
    failures = 0
    for name, value in sorted(globals().items()):
        if name.startswith("test_") and callable(value):
            try:
                value()
                print("PASS %s" % name)
            except AssertionError as error:
                failures += 1
                print("FAIL %s: %s" % (name, error))
            except Exception as error:
                failures += 1
                print("ERROR %s: %r" % (name, error))
    sys.exit(1 if failures else 0)
