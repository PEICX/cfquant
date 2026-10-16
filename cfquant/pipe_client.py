# -*- coding: utf-8 -*-
import os
import threading

from .config import get_config
from .errors import CfquantError
from ._fastjson import loads_message_typed
from .pipe_transport import (
    DEFAULT_PIPE_NAME,
    FRAME_FLAG_COLUMNAR,
    FRAME_TYPE_REQUEST,
    connect_pipe,
    decode_pipe_frame,
    dumps_pipe_message,
    encode_pipe_frame,
    normalize_pipe_name,
)
from .protocol import dumps_message, new_id
from .quote_wire import decode_whole_quote
from .rpc_base import BaseRpcClient


class PipeRpcClient(BaseRpcClient):
    """
    External Python RPC client over the cfquant named-pipe hub.
    """

    _TIMEOUT_HEAD = "cfquant pipe request timeout"

    def __init__(self, pipe_name=None, request_channel=None, timeout=None, client_id=None, connect_timeout_ms=None):
        cfg = get_config()
        self.pipe_name = normalize_pipe_name(pipe_name or cfg.get("pipe_name") or DEFAULT_PIPE_NAME)
        self.request_channel = request_channel or cfg["request_channel"]
        self.timeout = float(timeout or cfg["timeout"])
        self.client_id = client_id or cfg.get("client_id") or new_id("pipe_client")
        self.reply_channel = self.client_id
        self.connect_timeout_ms = int(connect_timeout_ms or cfg.get("pipe_connect_timeout_ms") or 3000)
        self.binary_enabled = str(
            os.environ.get("CFQUANT_PIPE_BINARY") or "1"
        ).strip().lower() not in ("0", "false", "no", "off")
        self._hub_binary = False
        self._rx_conn = None
        self._tx_conn = None
        super(PipeRpcClient, self).__init__()

    def start(self):
        with self._lock:
            self._event_dispatcher.check_request()
            if self._started and self._recv_thread is not None and self._recv_thread.is_alive():
                return
            if self._started:
                self._started = False
                self._close_conns_locked()
            self._hub_binary = False
            self._rx_conn = connect_pipe(self.pipe_name, timeout_ms=self.connect_timeout_ms)
            self._rx_conn.write_frame(dumps_pipe_message({
                "type": "hello",
                "role": "api_rx",
                "client_id": self.client_id,
                "binary_hotpath": self.binary_enabled,
            }))
            self._tx_conn = connect_pipe(self.pipe_name, timeout_ms=self.connect_timeout_ms)
            self._tx_conn.write_frame(dumps_pipe_message({
                "type": "hello",
                "role": "api_tx",
                "client_id": self.client_id,
                "binary_hotpath": self.binary_enabled,
            }))
            self._started = True
            generation = self._event_dispatcher.start()
            self._recv_thread = threading.Thread(target=self._recv_loop, args=(self._rx_conn, generation))
            self._recv_thread.daemon = True
            self._recv_thread.start()

    def close(self):
        with self._lock:
            self._event_dispatcher.stop()
            self._started = False
            self._close_conns_locked()
            self._fail_pending("cfquant pipe client closed")

    def publish_event(self, channel, payload):
        raw = dumps_message(payload)
        self.start()
        self._send_request(raw, channel)

    def _on_send_failure(self):
        self.close()

    def _send_request(self, payload, request_channel, request_id=None, action=None):
        conn = self._tx_conn
        if conn is None:
            raise CfquantError("cfquant pipe client not started")
        if self._hub_binary and request_id:
            # Only pack_request outputs take the binary hot path; everything
            # else keeps the JSON envelope so hub-side type checks still run.
            conn.write_frame(encode_pipe_frame(
                FRAME_TYPE_REQUEST,
                payload,
                request_id=request_id,
                client_id=self.client_id,
                channel=request_channel,
                action=action,
            ))
            return
        conn.write_frame(dumps_pipe_message({
            "type": "request",
            "role": "api_tx",
            "client_id": self.client_id,
            "request_channel": request_channel,
            "payload": payload,
        }))

    def _recv_loop(self, expected_conn, generation):
        disconnect_message = "cfquant pipe connection closed"
        while True:
            try:
                with self._lock:
                    if not self._started or self._rx_conn is not expected_conn:
                        return
                    conn = expected_conn
                if conn is None:
                    disconnect_message = "cfquant pipe receive connection missing"
                    break
                raw = conn.read_frame()
                if raw is None:
                    disconnect_message = "cfquant pipe receive connection closed"
                    break
                frame = decode_pipe_frame(raw)
                if frame is not None:
                    if frame.get("type") == "hello_ack":
                        self._hub_binary = self.binary_enabled and bool(frame.get("binary_hotpath"))
                        continue
                    payload = frame.get("payload")
                    if payload is None:
                        continue
                    if frame.get("flags", 0) & FRAME_FLAG_COLUMNAR:
                        msg = self._parse_columnar_event(frame, payload)
                    else:
                        msg = loads_message_typed(payload)
                else:
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8", errors="replace")
                    msg = loads_message_typed(raw)
                if not msg:
                    continue
                with self._lock:
                    if not self._started or self._rx_conn is not expected_conn:
                        return
                    self._dispatch_message(msg, generation)
            except Exception as e:
                disconnect_message = "cfquant pipe receive failed: %s" % e
                break
        self._mark_disconnected(disconnect_message, expected_conn=expected_conn)

    def _parse_columnar_event(self, frame, payload):
        """Rebuild a protocol event dict from a columnar whole-quote frame."""
        data = decode_whole_quote(payload)
        if data is None:
            return None
        subscription_id = frame.get("subscription_id")
        if subscription_id is not None:
            try:
                subscription_id = int(subscription_id)
            except (TypeError, ValueError):
                pass
        return {
            "type": "event",
            "event": frame.get("event_name") or "quote",
            "client_id": frame.get("client_id"),
            "subscription_id": subscription_id,
            "data": data,
            "meta": {},
            "protocol": "cfquant",
            "_cf_typed": False,
        }

    def _mark_disconnected(self, message, expected_conn=None):
        with self._lock:
            if expected_conn is not None and self._rx_conn is not expected_conn:
                return
            if not self._started and self._rx_conn is None and self._tx_conn is None:
                return
            self._started = False
            self._event_dispatcher.stop()
            self._close_conns_locked()
            self._fail_pending(message)

    def _close_conns_locked(self):
        conns = [self._rx_conn, self._tx_conn]
        self._rx_conn = None
        self._tx_conn = None
        for conn in conns:
            if conn is None:
                continue
            try:
                conn.close()
            except Exception:
                pass

