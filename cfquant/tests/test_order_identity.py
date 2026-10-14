"""Offline order-identity regressions; native QMT calls are local fakes."""

import copy

import pytest

from cfquant import order_meta
from cfquant.order_identity import order_remark_key, original_order_remark, prepare_order_remark
from cfquant.tests.test_cftrader import connected, order
from cfquant.xttype import XtOrder


def install_native_orders(env, return_id=True):
    rows = []

    def passorder(*args):
        env.native_calls.append(args)
        order_id = 1082219690 if not rows else 1082219717 + len(rows) - 1
        rows.append(dict(
            m_strAccountID=args[2], m_strInstrumentID=args[3].split('.')[0], m_strExchangeID='SH',
            m_nRef=order_id, m_nOrderID=order_id, m_strOrderSysID='xt%s' % order_id,
            m_strRemark=args[9], m_strOrderRemark=args[9], m_strStrategyName='',
            m_nVolumeTotalOriginal=args[6], m_nVolumeTraded=0, m_nOrderStatus=57,
            m_nOrderPriceType=88, m_dLimitPrice=1.29, m_nDirection=48, m_nOffsetFlag=49,
        ))
        return order_id if return_id else 0

    env.bridge.globals_dict.update(passorder=passorder, get_trade_detail_data=lambda *args: copy.deepcopy(rows))
    return rows


@pytest.mark.parametrize('return_id', [True, False])
def test_repeated_remark_queries_the_correct_volume_and_sysid(connected, monkeypatch, return_id):
    env = connected
    monkeypatch.setenv('CFQUANT_ORDER_ID_WAIT_SECONDS', '0')
    rows = install_native_orders(env, return_id)
    ids = [env.trader.order_stock(env.account, **order(order_volume=volume, order_remark='buy'))
           for volume in (300, 600)]
    assert ids == [1082219690, 1082219717]
    assert len({args[9] for args in env.native_calls}) == 2
    for order_id, volume, raw in zip(ids, (300, 600), rows):
        queried = env.trader.query_stock_order(env.account, order_id)
        assert (queried.order_id, queried.m_nRef, queried.order_volume) == (order_id, order_id, volume)
        assert queried.order_sysid == raw['m_strOrderSysID']
        assert queried.order_remark == 'buy'
        assert queried.cfquant_order_remark == raw['m_strRemark']
        assert not getattr(queried, 'cfquant_order_id_reconciled', False)


def test_reversed_async_callbacks_preserve_each_seq_with_identical_orders(connected):
    env = connected
    rows = install_native_orders(env)
    seqs = [env.trader.order_stock_async(env.account, **order(order_remark='buy')) for _ in range(2)]
    for raw in reversed(rows):
        # Exercise the SDK fallback before the bridge's explicit seq response.
        data = env.bridge._format_trade_detail(raw, 'order')
        env.client.handlers['trader:on_stock_order'](data)
        assert env.bridge._handle_async_order_callback(data)
    assert [(response.seq, response.order_id, response.order_remark) for response in env.responses] == [
        (seqs[1], 1082219717, 'buy'), (seqs[0], 1082219690, 'buy'),
    ]
    assert not env.bridge.pending_async_orders
    assert not env.trader._pending_async_orders


def test_callback_without_identity_does_not_consume_a_new_async_order(connected):
    env = connected
    install_native_orders(env)
    env.trader.order_stock_async(env.account, **order(order_remark='buy'))
    unrelated = dict(account_id='TEST_ONLY', stock_code='600000.SH', order_id=123, order_remark='buy')
    assert not env.bridge._handle_async_order_callback(unrelated)
    assert env.trader._async_order_response_from_order(XtOrder.from_any(unrelated)) is None
    assert len(env.trader._pending_async_orders) == 1


def test_query_restores_remark_after_in_memory_metadata_is_lost(connected):
    env = connected
    install_native_orders(env)
    text = '\u5356\u51fa ETF / buy__cfq_not-a-uuid'
    order_id = env.trader.order_stock(env.account, **order(order_remark=text))
    env.bridge.order_request_metadata.clear()
    queried = env.trader.query_stock_order(env.account, order_id)
    assert queried.order_remark == text
    assert original_order_remark(queried.cfquant_order_remark) == text


def test_legacy_bridge_repeated_remarks_preserve_native_order_ids():
    from cfquant.qmt_bridge import CfquantQmtBridge
    native = []

    def passorder(*args):
        native.append(dict(m_strAccountID=args[2], m_strInstrumentID='600000', m_strExchangeID='SH',
                           m_nRef=len(native)+1, m_strRemark=args[9], m_nVolumeTotalOriginal=args[6]))
        return len(native)

    bridge = CfquantQmtBridge(None, show=False, globals_dict={
        'passorder': passorder, 'get_trade_detail_data': lambda *args: native,
    })
    try:
        for volume in (300, 600):
            bridge._order_stock(dict(account={'account_id': 'TEST_ONLY'},
                                      **order(order_volume=volume, order_remark='buy')))
        rows = bridge._query_trade_detail({'account': {'account_id': 'TEST_ONLY'}}, 'ORDER')
        assert [(row['order_id'], row['order_volume'], row['order_remark']) for row in rows] == [
            (1, 300, 'buy'), (2, 600, 'buy'),
        ]
        assert len({row['cfquant_order_remark'] for row in rows}) == 2
    finally:
        bridge.close()


@pytest.mark.parametrize('asynchronous', [False, True])
def test_batch_accepts_duplicate_remarks_and_correlates_each_row(connected, monkeypatch, asynchronous):
    env = connected
    monkeypatch.setenv('CFQUANT_ORDER_ID_WAIT_SECONDS', '0')
    rows = install_native_orders(env, return_id=False)
    submit = env.api.order_stock_batch_async if asynchronous else env.api.order_stock_batch
    result = submit(env.account, [order(order_volume=n, order_remark='buy') for n in (300, 600)])
    assert result['submitted'] == 2
    assert [row['order_remark'] for row in result['results']] == ['buy', 'buy']
    assert len({raw['m_strRemark'] for raw in rows}) == 2
    if asynchronous:
        for raw in reversed(rows):
            assert env.bridge._handle_async_order_callback(env.bridge._format_trade_detail(raw, 'order'))
        assert [(r.seq, r.order_id) for r in env.responses] == [
            (result['results'][1]['seq'], 1082219717), (result['results'][0]['seq'], 1082219690),
        ]
    else:
        assert [row['order_id'] for row in result['results']] == [1082219690, 1082219717]


def test_legacy_plain_remark_cannot_rewrite_another_order_id(connected):
    bridge = connected.bridge
    bridge._remember_order_request('TEST_ONLY', '600000.SH', 'buy', 'strategy', order_id=1082219717)
    raw = dict(account_id='TEST_ONLY', stock_code='600000.SH', order_remark='buy',
               order_id=1082219690, m_nRef=1082219690, order_volume=300)
    bridge._enrich_order_request_fields(raw)
    assert raw['order_id'] == 1082219690


def test_persisted_identity_distinguishes_same_remark_across_bridges():
    cache = order_meta.OrderMetaCache()
    records = []
    for number, volume in ((1082219690, 300), (1082219717, 600)):
        params = dict(order_remark='buy')
        internal = prepare_order_remark(params)
        record = order_meta.normalize_record(dict(
            account_id='TEST_ONLY', account_type='STOCK', stock_code='600000.SH',
            order_remark='buy', user_order_id=internal, cfquant_order_remark=internal,
            order_id=number, order_volume=volume, strategy_name='same', status='bound',
        ))
        records.append(record)
    store = dict(entry for record in records for entry in order_meta.store_entries_for_record(record))
    cache.load_store(store)
    assert len(cache.by_user) == 2
    for expected in reversed(records):
        callback = dict(account_id='TEST_ONLY', stock_code='600000.SH',
                        order_remark=expected['user_order_id'], order_volume=expected['order_volume'])
        matched, info = cache.resolve_callback(callback, account_type='STOCK')
        assert matched['order_id'] == expected['order_id']
        order_meta.apply_record_to_callback(callback, matched, info)
        assert callback['order_id'] == expected['order_id']
        assert callback['order_remark'] == 'buy'
        assert order_remark_key(callback) == expected['user_order_id']


def test_parallel_submissions_do_not_share_an_identity():
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(lambda _: prepare_order_remark({'order_remark': 'buy'}), range(1000)))
    assert len(set(ids)) == 1000


@pytest.mark.parametrize('count', [1, 2])
def test_ambiguous_callback_without_native_remark_does_not_bind_pending_metadata(count):
    cache = order_meta.OrderMetaCache()
    for _ in range(count):
        internal = prepare_order_remark({'order_remark': 'buy'})
        cache.upsert(dict(account_id='TEST_ONLY', account_type='STOCK', stock_code='600000.SH',
                          user_order_id=internal, order_remark='buy', order_type=23,
                          order_volume=100, price=10.5, strategy_name='same'))
    matched, _ = cache.resolve_callback(dict(account_id='TEST_ONLY', stock_code='600000.SH',
                                            order_id=123, order_volume=100, price=10.5, order_type=23))
    assert matched is None


@pytest.mark.parametrize('remark', ['', 'buy'])
def test_deferred_sync_submissions_keep_identity_until_response(remark):
    from cfquant.normal_bridge import NormalQmtBridge
    bridge = NormalQmtBridge(None, show=False, schedule_timer=False, order_meta_enabled=False)
    responses, native = [], []
    bridge._send_response = lambda msg, result: responses.append((msg['id'], result))
    bridge.globals_dict['passorder'] = lambda *args: native.append(args) or 0
    try:
        for index in range(2):
            bridge._defer_sync_order_response(dict(id='request-%s' % index, params=dict(
                account={'account_id': 'TEST_ONLY', 'account_type': 'STOCK'},
                **order(order_remark=remark),
            )))
        assert len({args[9] for args in native}) == 2
        for index in (1, 0):
            assert bridge._resolve_pending_sync_order_callback(dict(
                account_id='TEST_ONLY', stock_code='600000.SH', order_remark=native[index][9],
                m_nRef=7000+index, order_sysid='SYS-%s' % index,
            ))
            bridge._poll_sync_order_responses()
        assert [(request_id, result['order_id']) for request_id, result in responses] == [
            ('request-1', 7001), ('request-0', 7000),
        ]
        assert not bridge.pending_sync_orders
    finally:
        bridge.close()
