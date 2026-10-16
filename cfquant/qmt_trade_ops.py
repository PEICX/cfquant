# -*- coding: utf-8 -*-
"""Shared QMT passorder optype mapping for the in-package bridges.

The LITE standalone entries embed their own copies of these methods (see
tools/sync_qmt_batch.py), so TxTradeBridge keeps its in-class implementations
verbatim. This module exists for bridges that live in the same cfquant core
package (currently CfquantQmtBridge) so the optype tables have a single
in-package source of truth.
"""
from .stock_connect import validate_connect_order


class QmtTradeOpsMixin(object):
    """Optype mapping + order reference helpers shared by in-package bridges.

    Method bodies are copied verbatim from TxTradeBridge; keep them in sync
    (tools/sync_qmt_batch.py manages the LITE copies from the same source).
    """

    def _passorder_optype(self, params, account_type):
        validate_connect_order(params, account_type)
        qmt_optype = self._first_param(params, ("qmt_optype", "passorder_optype"))
        if qmt_optype is not None:
            return self._coerce_optype(qmt_optype)
        account_type_text = str(account_type or "").strip().upper()
        credit_action_names = ("credit_action", "credit_business")
        if account_type_text == "CREDIT":
            credit_action_names += ("order_action", "business_type", "action")
        credit_action = self._first_param(params, credit_action_names)
        if credit_action is not None:
            return self._credit_action_optype(credit_action)
        derivative_action = self._first_param(params, (
            "future_action",
            "future_business",
            "stock_option_action",
            "future_option_action",
            "option_action",
            "option_business",
            "derivative_action",
            "business_type",
            "order_action",
            "action",
        ))
        if derivative_action is not None:
            return self._derivative_action_optype(account_type_text, derivative_action)
        order_type = params.get("optype", params.get("order_type"))
        if isinstance(order_type, str):
            text = order_type.strip().lower()
            if text in ("buy", "stock_buy"):
                return 33 if account_type_text == "CREDIT" else 23
            if text in ("sell", "stock_sell"):
                return 34 if account_type_text == "CREDIT" else 24
            if text in self._credit_action_optype_map():
                return self._credit_action_optype(text)
            if account_type_text in ("FUTURE", "FUTURE_OPTION", "STOCK_OPTION"):
                return self._derivative_action_optype(account_type_text, text)
        order_type = self._coerce_optype(order_type)
        if order_type is None:
            raise ValueError("order_type is required")
        return self._qmt_account_optype(order_type, account_type_text)


    def _coerce_optype(self, value):
        if isinstance(value, str):
            text = value.strip()
            if text.lstrip("+-").isdigit():
                return int(text)
        return value


    def _credit_action_optype(self, value):
        text = str(value or "").strip().lower()
        key = text if text.startswith("credit_") else self._credit_action_aliases().get(text) or "credit_%s" % text
        actions = self._credit_action_optype_map()
        if key not in actions:
            raise ValueError("unknown credit order action: %s" % value)
        return actions[key]


    def _qmt_credit_optype(self, order_type):
        mapping = {
            23: 33,
            24: 34,
            40: 70,
            41: 71,
            42: 72,
            43: 73,
            44: 74,
            45: 75,
        }
        return mapping.get(order_type, order_type)


    def _qmt_stock_option_optype(self, order_type):
        mapping = {
            48: 50,
            49: 51,
            50: 52,
            51: 53,
            52: 54,
            53: 55,
            54: 56,
            55: 57,
            56: 58,
            57: 59,
        }
        return mapping.get(order_type, order_type)


    def _qmt_account_optype(self, order_type, account_type):
        account_type = str(account_type or "").strip().upper()
        if account_type == "CREDIT":
            return self._qmt_credit_optype(order_type)
        if account_type == "STOCK_OPTION":
            return self._qmt_stock_option_optype(order_type)
        return order_type


    def _derivative_action_optype(self, account_type, value):
        account_type = str(account_type or "").strip().upper()
        coerced = self._coerce_optype(value)
        if isinstance(coerced, int):
            return self._qmt_account_optype(coerced, account_type)
        text = str(value or "").strip().lower()
        if account_type in ("FUTURE", "FUTURE_OPTION"):
            aliases = self._future_action_aliases()
            actions = self._future_action_optype_map()
            if account_type == "FUTURE_OPTION":
                actions = dict(actions, **self._future_option_action_optype_map())
            key = text if text in actions else aliases.get(text)
            if key is None and not text.startswith("future_") and ("future_%s" % text) in actions:
                key = "future_%s" % text
            if key in actions:
                return actions[key]
        if account_type == "STOCK_OPTION":
            aliases = self._stock_option_action_aliases()
            actions = self._stock_option_action_optype_map()
            key = text if text in actions else aliases.get(text)
            if key is None and not text.startswith("stock_option_") and ("stock_option_%s" % text) in actions:
                key = "stock_option_%s" % text
            if key in actions:
                return actions[key]
        raise ValueError("unknown %s order action: %s" % (account_type or "derivative", value))


    def _credit_action_aliases(self):
        return {
            "buy": "credit_buy",
            "collateral_buy": "credit_buy",
            "assure_buy": "credit_buy",
            "sell": "credit_sell",
            "collateral_sell": "credit_sell",
            "assure_sell": "credit_sell",
            "fin_buy": "credit_fin_buy",
            "finance_buy": "credit_fin_buy",
            "margin_buy": "credit_fin_buy",
            "slo_sell": "credit_slo_sell",
            "short_sell": "credit_slo_sell",
            "buy_secu_repay": "credit_buy_secu_repay",
            "buy_security_repay": "credit_buy_secu_repay",
            "direct_secu_repay": "credit_direct_secu_repay",
            "direct_security_repay": "credit_direct_secu_repay",
            "sell_secu_repay": "credit_sell_secu_repay",
            "sell_security_repay": "credit_sell_secu_repay",
            "direct_cash_repay": "credit_direct_cash_repay",
            "cash_repay": "credit_direct_cash_repay",
            "fin_buy_special": "credit_fin_buy_special",
            "finance_buy_special": "credit_fin_buy_special",
            "margin_buy_special": "credit_fin_buy_special",
            "slo_sell_special": "credit_slo_sell_special",
            "short_sell_special": "credit_slo_sell_special",
            "buy_secu_repay_special": "credit_buy_secu_repay_special",
            "direct_secu_repay_special": "credit_direct_secu_repay_special",
            "sell_secu_repay_special": "credit_sell_secu_repay_special",
            "direct_cash_repay_special": "credit_direct_cash_repay_special",
        }


    def _credit_action_optype_map(self):
        return {
            "credit_buy": 33,
            "credit_sell": 34,
            "credit_fin_buy": 27,
            "credit_slo_sell": 28,
            "credit_buy_secu_repay": 29,
            "credit_direct_secu_repay": 30,
            "credit_sell_secu_repay": 31,
            "credit_direct_cash_repay": 32,
            "credit_fin_buy_special": 70,
            "credit_slo_sell_special": 71,
            "credit_buy_secu_repay_special": 72,
            "credit_direct_secu_repay_special": 73,
            "credit_sell_secu_repay_special": 74,
            "credit_direct_cash_repay_special": 75,
        }


    def _future_action_aliases(self):
        return {
            "open_long": "future_open_long",
            "buy_open": "future_open_long",
            "close_long": "future_close_long_history_first",
            "sell_close": "future_close_long_history_first",
            "close_long_history": "future_close_long_history",
            "close_long_today": "future_close_long_today",
            "open_short": "future_open_short",
            "sell_open": "future_open_short",
            "close_short": "future_close_short_history_first",
            "buy_close": "future_close_short_history_first",
            "close_short_history": "future_close_short_history",
            "close_short_today": "future_close_short_today",
            "open": "future_open",
            "close": "future_close",
            "exercise": "future_option_exercise",
            "future_option_exercise": "future_option_exercise",
            "option_future_option_exercise": "future_option_exercise",
        }


    def _future_action_optype_map(self):
        return {
            "future_open_long": 0,
            "future_close_long_history": 1,
            "future_close_long_today": 2,
            "future_open_short": 3,
            "future_close_short_history": 4,
            "future_close_short_today": 5,
            "future_close_long_today_first": 6,
            "future_close_long_history_first": 7,
            "future_close_short_today_first": 8,
            "future_close_short_history_first": 9,
            "future_close_long_today_history_then_open_short": 10,
            "future_close_long_history_today_then_open_short": 11,
            "future_close_short_today_history_then_open_long": 12,
            "future_close_short_history_today_then_open_long": 13,
            "future_open": 14,
            "future_close": 15,
            "future_arbitrage_open": 16,
            "future_arbitrage_close_history_first": 17,
            "future_arbitrage_close_today_first": 18,
            "future_renew_long_close_history_first": 19,
            "future_renew_long_close_today_first": 20,
            "future_renew_short_close_history_first": 21,
            "future_renew_short_close_today_first": 22,
            "future_hedge": 400,
        }


    def _future_option_action_optype_map(self):
        return {
            "future_option_exercise": 100,
        }


    def _stock_option_action_aliases(self):
        return {
            "buy_open": "stock_option_buy_open",
            "open_long": "stock_option_buy_open",
            "sell_close": "stock_option_sell_close",
            "close_long": "stock_option_sell_close",
            "sell_open": "stock_option_sell_open",
            "open_short": "stock_option_sell_open",
            "buy_close": "stock_option_buy_close",
            "close_short": "stock_option_buy_close",
            "covered_open": "stock_option_covered_open",
            "covered_close": "stock_option_covered_close",
            "call_exercise": "stock_option_call_exercise",
            "put_exercise": "stock_option_put_exercise",
            "secu_lock": "stock_option_secu_lock",
            "secu_unlock": "stock_option_secu_unlock",
            "lock": "stock_option_secu_lock",
            "unlock": "stock_option_secu_unlock",
        }


    def _stock_option_action_optype_map(self):
        return {
            "stock_option_buy_open": 50,
            "stock_option_sell_close": 51,
            "stock_option_sell_open": 52,
            "stock_option_buy_close": 53,
            "stock_option_covered_open": 54,
            "stock_option_covered_close": 55,
            "stock_option_call_exercise": 56,
            "stock_option_put_exercise": 57,
            "stock_option_secu_lock": 58,
            "stock_option_secu_unlock": 59,
        }

