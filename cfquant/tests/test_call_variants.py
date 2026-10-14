# -*- coding: utf-8 -*-
"""_call_variants 签名预判的回归测试。

历史问题：旧实现用 except TypeError 探测参数变体，函数体内抛出的
TypeError 会被误判为签名不匹配而再次调用——对 passorder 这类有副作用的
下单函数意味着重复下单。修复后签名可内省时只有 bind 成功的变体才会被
真正调用，函数体内的 TypeError 直接上抛。
"""
import inspect

import pytest

from cfquant.qmt_bridge import CfquantQmtBridge
from cfquant.tx_trade_bridge import TxTradeBridge


BRIDGES = [TxTradeBridge, CfquantQmtBridge]


def _bridge(cls):
    if cls is TxTradeBridge:
        return cls(object(), globals_dict={}, show=False)
    return cls(object(), globals_dict={})


@pytest.mark.parametrize("cls", BRIDGES)
def test_side_effectful_typeerror_is_not_retried(cls):
    calls = []

    def passorder(a, b):
        calls.append((a, b))
        raise TypeError("order submitted, then QMT internals broke")

    bridge = _bridge(cls)
    with pytest.raises(TypeError):
        bridge._call_variants(passorder, [((1, 2), {}), ((1,), {})])
    assert calls == [(1, 2)]


@pytest.mark.parametrize("cls", BRIDGES)
def test_arity_mismatch_variant_is_skipped_without_calling(cls):
    calls = []

    def query(a, b):
        calls.append((a, b))
        return "ok"

    bridge = _bridge(cls)
    assert bridge._call_variants(query, [((1,), {}), ((1, 2), {})]) == "ok"
    assert calls == [(1, 2)]


@pytest.mark.parametrize("cls", BRIDGES)
def test_all_variants_unmatched_raises_last_error(cls):
    def query(a, b):
        return "ok"

    bridge = _bridge(cls)
    with pytest.raises(TypeError):
        bridge._call_variants(query, [((1,), {}), ((1, 2, 3), {})])


@pytest.mark.parametrize("cls", BRIDGES)
def test_signature_unavailable_keeps_probing_fallback(cls, monkeypatch):
    """C 函数等签名不可内省时保留原有的异常探测行为。"""
    calls = []

    def opaque(a):
        calls.append(a)
        if a == 1:
            raise TypeError("simulated arity mismatch")
        return "fallback-ok"

    def no_signature(func):
        raise ValueError("no signature for builtin")

    monkeypatch.setattr(inspect, "signature", no_signature)
    bridge = _bridge(cls)
    assert bridge._call_variants(opaque, [((1,), {}), ((2,), {})]) == "fallback-ok"
    assert calls == [1, 2]
