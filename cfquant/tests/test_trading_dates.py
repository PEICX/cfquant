"""Legacy bridge calendar translation, retained for older RPC clients."""
import ast
import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from cfquant.tx_trade_bridge import TxTradeBridge


ROOT = Path(__file__).resolve().parents[2]


def stamp(day):
    return int(datetime.datetime.strptime(day, "%Y%m%d").replace(
        tzinfo=datetime.timezone(datetime.timedelta(hours=8))
    ).timestamp() * 1000)


@pytest.fixture(params=[None] + list((ROOT / "qmt_scripts").rglob("CFQUANT_LITE*.py")))
def adapter(request):
    if request.param is None:
        return TxTradeBridge._get_trading_dates
    tree = ast.parse(request.param.read_text(encoding="gbk"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TxTradeBridge")
    method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_get_trading_dates")
    scope = {}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(request.param), "exec"), scope)
    return scope[method.name]


def wire(adapter, calendar):
    bridge = SimpleNamespace(_get_callable=lambda name: calendar if name == "get_trading_calendar" else None)
    def query(market, start_time="", end_time="", count=-1):
        return adapter(bridge, dict(market=market, start_time=start_time, end_time=end_time, count=count))
    return query


@pytest.mark.parametrize("market", ["SH", "SZ", "BJ", "HK", "IF"])
def test_keywords_range_count_and_timestamps(adapter, market):
    calls = []
    def calendar(*args):
        calls.append(args)
        return ["20260107", "20260105", "20260106", "20260106", "20251231", "20260202"]
    query = wire(adapter, calendar)
    assert query(market=market, start_time="20260101", end_time="20260131", count=2) == [stamp("20260106"), stamp("20260107")]
    assert calls == [(market, "20260101", "20260131")]
    assert query(market, "20260101", "20260131") == [stamp(d) for d in ("20260105", "20260106", "20260107")]


def test_defaults_and_future_cap(adapter):
    calls = []
    query = wire(adapter, lambda *args: calls.append(args) or [])
    today = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y%m%d")
    assert query("SH") == []
    assert calls == [("SH", "", today)]
    assert query("SH", end_time="20991231") == []
    assert calls[-1] == ("SH", "", today)


def test_empty_and_validation(adapter):
    def unexpected(*args):
        pytest.fail("invalid/empty query must not invoke QMT")
    query = wire(adapter, unexpected)
    assert query("SH", count=0) == []
    assert query("SH", "20260201", "20260101") == []
    for count in (-2, 1.5, True):
        with pytest.raises(ValueError, match="count"):
            query("SH", count=count)
    with pytest.raises(ValueError, match="market"):
        query("600000.SH")
    with pytest.raises(ValueError):
        query("SH", "20260230")


def test_missing_calendar_and_malformed_result(adapter):
    query = wire(adapter, None)
    with pytest.raises(NotImplementedError, match="get_trading_calendar"):
        query("SH")
    query = wire(adapter, lambda *args: None)
    with pytest.raises(ValueError, match="returned None"):
        query("SH")


def test_intraday_bounds(adapter):
    calls = []
    query = wire(adapter, lambda *args: calls.append(args) or ["20260105", "20260106"])
    assert query("SH", "20260105120000", "20260106120000") == [stamp("20260106")]
    assert calls == [("SH", "20260105", "20260106")]
