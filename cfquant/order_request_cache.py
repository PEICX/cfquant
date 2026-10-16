# -*- coding: utf-8 -*-
"""Shared in-memory order-request metadata cache for the in-package bridges.

``TxTradeBridge._remember_order_request`` / ``_enrich_order_request_fields``
keep the submitted strategy name keyed by (account, stock, remark) so native
QMT callbacks and query rows can be re-associated with the strategy that sent
them.  The original implementation rescanned all metadata rows (up to 1000)
for every formatted row, which is O(rows x 1000) while holding the lock and
took tens of seconds on a 2000-row order query; the current revision keeps a
secondary ``order_id -> metadata key`` index so the common path is an O(1)
lookup.

The methods below are copied verbatim from TxTradeBridge.  The LITE standalone
entries embed their own copies (see tools/sync_qmt_batch.py) and therefore keep
the in-class implementations unchanged; this module exists so bridges that live
in the same cfquant core package (currently CfquantQmtBridge) can share the
single in-package source of truth instead of drifting behind it.
"""
from .order_identity import (
    is_unique_order_remark,
    order_remark_key,
    order_remark_matches_reference,
    restore_order_remark,
)
from . import order_meta


class OrderRequestMetadataMixin(object):
    """Metadata store + O(1) index used by callbacks and trade-detail queries.

    The host class must provide ``order_request_metadata`` (dict),
    ``order_request_metadata_by_id`` (dict) and ``order_request_metadata_lock``
    (an RLock), plus the ``_first_value`` / ``_normalize_order_id`` /
    ``_order_id_from_detail`` helpers it already shares with TxTradeBridge.
    """

    def _order_metadata_known(self, order):
        # 判断该行是否存在于 order_request_metadata 的委托号索引中。
        # 与 _enrich_order_request_fields 使用同一判据（account_id + 任一委托号），
        # 因此"跳过"不会漏掉富化本可命中的行。历史旧单的元数据早已被上限
        # 淘汰，索引必然未命中——查询侧据此跳过逐行富化与登记的空耗。
        if not isinstance(order, dict):
            return False
        account_id = str(self._first_value(order, ("account_id", "m_strAccountID")) or "").strip()
        if not account_id:
            return False
        with self.order_request_metadata_lock:
            index = getattr(self, "order_request_metadata_by_id", None)
            if not index:
                return False
            for name in (
                "order_id",
                "m_nRef",
                "m_nOrderID",
                "m_strOrderRef",
                "m_strOrderID",
            ):
                order_id = self._normalize_order_id(self._first_value(order, (name,)))
                if order_id is not None and (account_id, str(order_id)) in index:
                    return True
        return False

    def _remember_order_request(self, account_id, stock_code, order_remark, strategy_name, order_id=None):
        key = (
            str(account_id or "").strip(),
            str(stock_code or "").strip().upper().split(".", 1)[0],
            str(order_remark or ""),
        )
        if not key[0] or not key[2]:
            return
        metadata = {
            "strategy_name": str(strategy_name or ""),
            "order_remark": str(order_remark or ""),
            "order_id": self._normalize_order_id(order_id),
        }
        with self.order_request_metadata_lock:
            self.order_request_metadata[key] = metadata
            order_id_value = metadata.get("order_id")
            if order_id_value is not None:
                self.order_request_metadata_by_id[(key[0], str(order_id_value))] = key
            while len(self.order_request_metadata) > 1000:
                evicted_key = next(iter(self.order_request_metadata))
                evicted = self.order_request_metadata.pop(evicted_key)
                evicted_id = evicted.get("order_id") if isinstance(evicted, dict) else None
                if evicted_id is not None:
                    self.order_request_metadata_by_id.pop((evicted_key[0], str(evicted_id)), None)

    def _enrich_order_request_fields(self, order):
        if not isinstance(order, dict):
            return order
        key = (
            str(self._first_value(order, ("account_id", "m_strAccountID")) or "").strip(),
            str(self._first_value(order, ("stock_code", "m_strInstrumentID")) or "").strip().upper().split(".", 1)[0],
            order_remark_key(order),
        )
        # 先收集本行的候选委托号：既能用于 O(1) 索引命中，也供后面的备注兜底匹配。
        callback_order_ids = set()
        for name in (
            "order_id",
            "m_nRef",
            "m_nOrderID",
            "m_strOrderRef",
            "m_strOrderID",
        ):
            order_id = self._normalize_order_id(self._first_value(order, (name,)))
            if order_id is not None:
                callback_order_ids.add(str(order_id))
        with self.order_request_metadata_lock:
            metadata = self.order_request_metadata.get(key)
            matched_key = key if metadata is not None else None
            if metadata is None and key[0] and callback_order_ids:
                # O(1) 索引命中：绝大多数行能通过委托号直接定位，避免全量扫描。
                # 与原全量扫描同语义：命中后仍要求备注可匹配（order_remark_matches_reference），
                # 否则不同备注、相同委托号的行会被错误关联。
                for candidate_id in callback_order_ids:
                    indexed_key = self.order_request_metadata_by_id.get((key[0], candidate_id))
                    if indexed_key is None:
                        continue
                    if not order_remark_matches_reference(indexed_key[2], key[2]):
                        continue
                    if key[1] and indexed_key[1] and key[1] != indexed_key[1]:
                        continue
                    candidate = self.order_request_metadata.get(indexed_key)
                    if isinstance(candidate, dict):
                        metadata = candidate
                        matched_key = indexed_key
                        break
            # 索引未命中即视为无元数据：原全量扫描的匹配条件同样要求候选 order_id
            # 落在 callback_order_ids 内，因此索引查不到时扫描也必然查不到（索引是
            # 扫描的超集）。历史旧单的元数据早已被上限淘汰，此前每行仍要扫 1000 条
            # 空跑——2000 行委托因此耗时数十秒。这里直接跳过，语义不变。
        if isinstance(metadata, dict):
            strategy_name = str(metadata.get("strategy_name") or "")
            order_remark = str(
                metadata.get("order_remark")
                or (matched_key[2] if matched_key is not None else "")
            )
            order_id = self._normalize_order_id(metadata.get("order_id"))
        else:
            # Keep compatibility with bridges created before metadata became structured.
            strategy_name = metadata
            order_remark = str(matched_key[2] if matched_key is not None else "")
            order_id = None
        if strategy_name is not None and not order.get("strategy_name"):
            order["strategy_name"] = strategy_name
            if not order.get("m_strStrategyName"):
                order["m_strStrategyName"] = strategy_name
        if order_remark and not order.get("order_remark"):
            order["order_remark"] = order_remark
        if order_remark:
            for name in ("m_strRemark", "m_strOrderRemark"):
                if not order.get(name):
                    order[name] = order_remark
        if order_id is not None and (is_unique_order_remark(key[2]) or
                                    order_id == self._order_id_from_detail(order)):
            order_meta.reconcile_order_id(order, order_id)
            for name in ("m_nRef", "m_nOrderID"):
                if self._normalize_order_id(order.get(name)) is None:
                    order[name] = order_id
            for name in ("m_strOrderRef", "m_strOrderID"):
                if order.get(name) is None or str(order.get(name)).strip() in ("", "0", "-1"):
                    order[name] = str(order_id)
        self.order_callback_cache.enrich(order, self._account_type_name(order.get("account_type")))
        return restore_order_remark(order)

    def _remember_order_callback_fields(self, order):
        self.order_callback_cache.remember(order, self._account_type_name(order.get("account_type")))
