# -*- coding: utf-8 -*-
"""Shared RPC client skeleton for the LTtx and named-pipe external clients.

Both transports speak the same request/response protocol and dispatch events
the same way; only the connection and framing differ.  This module holds the
common state, the request() template, callback registration, and the
response/event dispatch so the two concrete clients only implement transport
start/send/close details.
"""
import queue
import threading

from .callback_dispatcher import CallbackDispatcher, dispatch_callbacks
from .errors import CfquantError, CfquantTimeout
from .protocol import decode_value, new_id, pack_request


class BaseRpcClient(object):
    # Overridden by the pipe client to keep its distinct timeout message text.
    _TIMEOUT_HEAD = "cfquant request timeout"

    def __init__(self):
        self._pending = {}
        self._callbacks = {}
        self._lock = threading.RLock()
        self._pending_lock = threading.RLock()
        self._started = False
        self._recv_thread = None
        self._event_dispatcher = CallbackDispatcher(self._dispatch_event)

    def request(self, action, params=None, timeout=None, request_channel=None):
        effective_timeout = float(timeout or self.timeout)
        request_id = new_id("req")
        raw = pack_request(
            action,
            params=params,
            reply_channel=self.reply_channel,
            client_id=self.client_id,
            request_id=request_id,
            timeout=effective_timeout,
        )
        q = queue.Queue(maxsize=1)
        try:
            with self._lock:
                self.start()
                with self._pending_lock:
                    self._pending[request_id] = q
                try:
                    self._send_request(
                        raw,
                        request_channel or self.request_channel,
                        request_id=request_id,
                        action=action,
                    )
                except Exception:
                    self._on_send_failure()
                    raise
            try:
                msg = q.get(timeout=effective_timeout)
            except queue.Empty:
                raise CfquantTimeout("%s: %s" % (self._TIMEOUT_HEAD, action))
            if not msg.get("ok"):
                err = msg.get("error") or {}
                raise CfquantError(err.get("message") or str(err))
            if msg.pop("_cf_typed", True):
                return decode_value(msg.get("result"))
            return msg.get("result")
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)

    def add_callback(self, event, callback):
        if callback is None:
            return
        self._callbacks.setdefault(event, []).append(callback)

    def remove_callback(self, event, callback):
        callbacks = self._callbacks.get(event) or []
        if callback in callbacks:
            callbacks.remove(callback)

    def _send_request(self, payload, request_channel, request_id=None, action=None):
        raise NotImplementedError

    def _on_send_failure(self):
        """Hook for transports that reconnect/close on send failure (named pipe)."""

    def _dispatch_message(self, msg, generation):
        msg_type = msg.get("type")
        if msg_type == "response":
            with self._pending_lock:
                q = self._pending.pop(msg.get("id"), None)
            if q:
                q.put(msg)
        elif msg_type == "event":
            self._event_dispatcher.submit(generation, msg)

    def _dispatch_event(self, msg):
        dispatch_callbacks(self._callbacks, msg, self._event_dispatcher.is_current_callback)

    def _fail_pending(self, message):
        with self._pending_lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for q in pending:
            try:
                q.put_nowait({"ok": False, "error": {"message": message}})
            except Exception:
                pass
