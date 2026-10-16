# -*- coding: utf-8 -*-
"""Shared QMT auto-trade-callback registration guard.

A single all-in-one QMT entry can create both a normal bridge and a trade
bridge against the same ``ContextInfo``.  QMT treats every
``set_auto_trade_callback(True)`` call as a new registration, so the enabled
state has to be shared across bridge instances rather than tracked per bridge.
Without this guard a bridge that wraps an already-configured ``ContextInfo``
registers the callback a second time, which changes callback fan-out for the
whole QMT process.

The registry and helpers below match ``TxTradeBridge`` verbatim.  The LITE
standalone entries embed their own copies (see tools/sync_qmt_batch.py) and
keep the in-class/global implementations unchanged; this module is the
in-package source of truth for ``CfquantQmtBridge``, so mixing a normal bridge
and a trade bridge in one QMT process behaves identically either way.
"""
import threading


_AUTO_TRADE_CALLBACK_LOCK = threading.RLock()
_AUTO_TRADE_CALLBACK_REGISTRY = {}


class AutoTradeCallbackMixin(object):
    """Register ``set_auto_trade_callback`` at most once per ContextInfo.

    The host class must provide ``context``, ``auto_trade_callback_enabled``,
    ``_get_callable`` and ``_log``.
    """

    def _release_auto_trade_callback(self, context=None):
        context = context or self.context
        if context is None:
            self.auto_trade_callback_enabled = False
            return
        context_key = id(context)
        owner_key = id(self)
        with _AUTO_TRADE_CALLBACK_LOCK:
            record = _AUTO_TRADE_CALLBACK_REGISTRY.get(context_key)
            if record is not None and record.get("context") is context:
                owners = record.get("owners") or set()
                owners.discard(owner_key)
                # There is no portable QMT API to unregister this callback.
                # Keep the enabled marker even when all bridge objects close,
                # so a later bridge for the same ContextInfo does not register
                # the QMT callback a second time.
                record["owners"] = owners
        self.auto_trade_callback_enabled = False

    def _enable_auto_trade_callback(self):
        if self.context is None or self.auto_trade_callback_enabled:
            return

        context = self.context
        context_key = id(context)
        owner_key = id(self)
        with _AUTO_TRADE_CALLBACK_LOCK:
            record = _AUTO_TRADE_CALLBACK_REGISTRY.get(context_key)
            if record is not None and record.get("context") is context and record.get("enabled"):
                record.setdefault("owners", set()).add(owner_key)
                self.auto_trade_callback_enabled = True
                self._log("auto trade callback already enabled; reused shared registration")
                return
            if record is not None and record.get("context") is not context:
                _AUTO_TRADE_CALLBACK_REGISTRY.pop(context_key, None)

            func = getattr(context, "set_auto_trade_callback", None)
            call_with_context = False
            if not callable(func):
                func = self._get_callable("set_auto_trade_callback")
                call_with_context = True
            if not callable(func):
                self._log("auto trade callback enable skipped: set_auto_trade_callback not found")
                return

            try:
                if call_with_context:
                    try:
                        result = func(context, True)
                    except TypeError:
                        result = func(True)
                else:
                    result = func(True)
            except Exception as e:
                self._log("auto trade callback enable failed:%s" % e)
                return

            _AUTO_TRADE_CALLBACK_REGISTRY[context_key] = {
                "context": context,
                "enabled": True,
                "owners": {owner_key},
            }
            self.auto_trade_callback_enabled = True
            self._log("auto trade callback enabled result=%s" % result)
