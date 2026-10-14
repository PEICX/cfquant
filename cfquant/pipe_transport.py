# -*- coding: utf-8 -*-
import ctypes
import json
import os
import queue
import re
import struct
import threading
import time
import uuid
from ctypes import wintypes

from .logging_i18n import get_log_enabled, translate_log


DEFAULT_PIPE_NAME = os.environ.get("CFQUANT_PIPE_NAME", r"\\.\pipe\cfquant_pipe_hub")
PIPE_MESSAGE_PREFIX = "cfpipe:"

GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
PIPE_ACCESS_DUPLEX = 0x00000003
PIPE_TYPE_BYTE = 0x00000000
PIPE_READMODE_BYTE = 0x00000000
PIPE_WAIT = 0x00000000
PIPE_UNLIMITED_INSTANCES = 255
NMPWAIT_WAIT_FOREVER = 0xFFFFFFFF

ERROR_FILE_NOT_FOUND = 2
ERROR_ACCESS_DENIED = 5
ERROR_BROKEN_PIPE = 109
ERROR_PIPE_BUSY = 231
ERROR_NO_DATA = 232
ERROR_PIPE_NOT_CONNECTED = 233
ERROR_PIPE_CONNECTED = 535

DEFAULT_BUFFER_SIZE = 65536
DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024


# ---------------------------------------------------------------------------
# Binary hot-path frame codec.
#
# Text envelopes ("cfpipe:" + JSON) stay the default so old peers keep working.
# A peer that advertised binary_hotpath in its hello and received a hello_ack
# may send hot messages (request / publish / delivery) as binary frames:
#
#   magic(2B "CF") ver(1B) msg_type(1B) flags(1B) payload_len(4B) tlv_len(2B)
#   TLVs (tag 1B + len 2B + value)   payload (opaque bytes)
#
# The hub routes on the TLV fields alone and forwards the payload untouched.
# ---------------------------------------------------------------------------

PIPE_FRAME_MAGIC = b"CF"
PIPE_FRAME_MAGIC_INT = 0x4346
PIPE_FRAME_VERSION = 1

FRAME_TYPE_REQUEST = 1
FRAME_TYPE_PUBLISH_RESPONSE = 2
FRAME_TYPE_PUBLISH_EVENT = 3
FRAME_TYPE_DELIVERY = 4

FRAME_FLAG_COLUMNAR = 0x01

TLV_REQUEST_ID = 1
TLV_CLIENT_ID = 2
TLV_CHANNEL = 3
TLV_ACTION = 4
TLV_EVENT_NAME = 5
TLV_SUBSCRIPTION_ID = 6

_FRAME_HEADER = struct.Struct("!HBBBIH")
_FRAME_TLV_HEAD = struct.Struct("!BH")

_FRAME_TYPE_NAMES = {
    FRAME_TYPE_REQUEST: "request",
    FRAME_TYPE_PUBLISH_RESPONSE: "publish",
    FRAME_TYPE_PUBLISH_EVENT: "publish",
    FRAME_TYPE_DELIVERY: "delivery",
}
_FRAME_TYPES_BY_NAME = {
    "request": FRAME_TYPE_REQUEST,
    "response": FRAME_TYPE_PUBLISH_RESPONSE,
    "event": FRAME_TYPE_PUBLISH_EVENT,
}

# Routing scans over the head of our own compact protocol messages. They rely
# on the fixed key order of pack_response/pack_event and always fall back to a
# full parse when they do not match.
_RESPONSE_ID_SCAN = re.compile(r'^cfquant:\{"type":"response","id":"([^"]+)"')
_EVENT_NAME_SCAN = re.compile(r'^cfquant:\{"type":"event","event":"([^"]+)"')


def scan_response_request_id(payload):
    match = _RESPONSE_ID_SCAN.match(payload) if isinstance(payload, str) else None
    return match.group(1) if match else None


def scan_event_name(payload):
    match = _EVENT_NAME_SCAN.match(payload) if isinstance(payload, str) else None
    return match.group(1) if match else None


def encode_pipe_frame(msg_type, payload, request_id=None, client_id=None,
                      channel=None, action=None, event_name=None,
                      subscription_id=None, flags=0):
    if isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    elif payload is None:
        payload_bytes = b""
    else:
        payload_bytes = bytes(payload)
    tlvs = []
    for tag, value in (
        (TLV_REQUEST_ID, request_id),
        (TLV_CLIENT_ID, client_id),
        (TLV_CHANNEL, channel),
        (TLV_ACTION, action),
        (TLV_EVENT_NAME, event_name),
        (TLV_SUBSCRIPTION_ID, subscription_id),
    ):
        if value is None:
            continue
        value_bytes = str(value).encode("utf-8")
        tlvs.append(_FRAME_TLV_HEAD.pack(tag, len(value_bytes)) + value_bytes)
    tlv_block = b"".join(tlvs)
    return _FRAME_HEADER.pack(
        PIPE_FRAME_MAGIC_INT,
        PIPE_FRAME_VERSION,
        int(msg_type),
        int(flags) & 0xFF,
        len(payload_bytes),
        len(tlv_block),
    ) + tlv_block + payload_bytes


def _decode_binary_frame(raw):
    if len(raw) < _FRAME_HEADER.size:
        return None
    magic, version, msg_type, flags, payload_len, tlv_len = _FRAME_HEADER.unpack_from(raw, 0)
    if magic != PIPE_FRAME_MAGIC_INT or version != PIPE_FRAME_VERSION:
        return None
    offset = _FRAME_HEADER.size
    tlv_end = offset + tlv_len
    if tlv_end > len(raw):
        return None
    fields = {}
    while offset + _FRAME_TLV_HEAD.size <= tlv_end:
        tag, value_len = _FRAME_TLV_HEAD.unpack_from(raw, offset)
        offset += _FRAME_TLV_HEAD.size
        if offset + value_len > tlv_end:
            return None
        value = raw[offset:offset + value_len]
        offset += value_len
        fields.setdefault(tag, value.decode("utf-8", "replace"))
    payload = raw[tlv_end:tlv_end + payload_len]
    envelope = {
        "protocol": "cfquant_pipe",
        "type": _FRAME_TYPE_NAMES.get(msg_type, "unknown"),
        "key": "response" if msg_type == FRAME_TYPE_PUBLISH_RESPONSE else (
            "event" if msg_type == FRAME_TYPE_PUBLISH_EVENT else None),
        "request_id": fields.get(TLV_REQUEST_ID),
        "client_id": fields.get(TLV_CLIENT_ID),
        "channel": fields.get(TLV_CHANNEL),
        "request_channel": fields.get(TLV_CHANNEL),
        "action": fields.get(TLV_ACTION),
        "event_name": fields.get(TLV_EVENT_NAME),
        "subscription_id": fields.get(TLV_SUBSCRIPTION_ID),
        "flags": flags,
        "payload": payload,
        "_binary": True,
    }
    return envelope


def decode_pipe_frame(raw):
    """Decode a frame payload (bytes, or legacy text from test doubles).

    Returns the envelope dict for both binary frames and cfpipe text frames,
    or None when the payload is neither, so callers keep their legacy
    raw-payload handling.
    """
    if isinstance(raw, (bytes, bytearray, memoryview)):
        raw = bytes(raw)
        if raw[:2] == PIPE_FRAME_MAGIC:
            return _decode_binary_frame(raw)
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return None
    return loads_pipe_message(raw)


def is_windows():
    return os.name == "nt"


def normalize_pipe_name(pipe_name=None):
    pipe_name = str(pipe_name or DEFAULT_PIPE_NAME)
    if pipe_name.startswith("\\\\.\\pipe\\"):
        return pipe_name
    name = pipe_name.strip("\\/")
    return r"\\.\pipe\%s" % name


def dumps_pipe_message(payload):
    data = dict(payload)
    data.setdefault("protocol", "cfquant_pipe")
    data.setdefault("ts", int(time.time() * 1000))
    return PIPE_MESSAGE_PREFIX + json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def loads_pipe_message(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str) or not raw.startswith(PIPE_MESSAGE_PREFIX):
        return None
    try:
        data = json.loads(raw[len(PIPE_MESSAGE_PREFIX):])
    except Exception:
        return None
    if data.get("protocol") != "cfquant_pipe":
        return None
    return data


class _Kernel32(object):
    def __init__(self):
        if not is_windows():
            raise OSError("named pipe transport requires Windows")
        self.dll = ctypes.WinDLL("kernel32", use_last_error=True)
        self.INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

        self.dll.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        self.dll.CreateFileW.restype = wintypes.HANDLE

        self.dll.CreateNamedPipeW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
        ]
        self.dll.CreateNamedPipeW.restype = wintypes.HANDLE

        self.dll.ConnectNamedPipe.argtypes = [wintypes.HANDLE, wintypes.LPVOID]
        self.dll.ConnectNamedPipe.restype = wintypes.BOOL

        self.dll.DisconnectNamedPipe.argtypes = [wintypes.HANDLE]
        self.dll.DisconnectNamedPipe.restype = wintypes.BOOL

        self.dll.ReadFile.argtypes = [
            wintypes.HANDLE,
            wintypes.LPVOID,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
            wintypes.LPVOID,
        ]
        self.dll.ReadFile.restype = wintypes.BOOL

        self.dll.WriteFile.argtypes = [
            wintypes.HANDLE,
            wintypes.LPCVOID,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
            wintypes.LPVOID,
        ]
        self.dll.WriteFile.restype = wintypes.BOOL

        self.dll.FlushFileBuffers.argtypes = [wintypes.HANDLE]
        self.dll.FlushFileBuffers.restype = wintypes.BOOL

        self.dll.CloseHandle.argtypes = [wintypes.HANDLE]
        self.dll.CloseHandle.restype = wintypes.BOOL

        self.dll.WaitNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD]
        self.dll.WaitNamedPipeW.restype = wintypes.BOOL

        self.cancel_io_ex = getattr(self.dll, "CancelIoEx", None)
        if self.cancel_io_ex is not None:
            self.cancel_io_ex.argtypes = [wintypes.HANDLE, wintypes.LPVOID]
            self.cancel_io_ex.restype = wintypes.BOOL

    def last_error(self):
        return ctypes.get_last_error()

    def raise_last_error(self, message):
        error = self.last_error()
        raise OSError(error, "%s failed with winerror=%s" % (message, error))

    def invalid_handle(self, handle):
        return handle in (None, 0, self.INVALID_HANDLE_VALUE)


_kernel32 = None
_kernel32_lock = threading.Lock()


def kernel32():
    global _kernel32
    with _kernel32_lock:
        if _kernel32 is None:
            _kernel32 = _Kernel32()
        return _kernel32


class NamedPipeConnection(object):
    def __init__(self, handle, name="", owner_server_side=False, max_frame_size=DEFAULT_MAX_FRAME_SIZE):
        self.handle = handle
        self.name = name
        self.owner_server_side = bool(owner_server_side)
        self.max_frame_size = int(max_frame_size)
        self.write_lock = threading.RLock()
        self.closed = False

    def read_frame(self):
        header = self._read_exact(8)
        if header is None:
            return None
        size = struct.unpack("!Q", header)[0]
        if size > self.max_frame_size:
            raise ValueError("named pipe frame too large: %s > %s" % (size, self.max_frame_size))
        if size == 0:
            return b""
        data = self._read_exact(size)
        if data is None:
            return None
        return data

    def write_frame(self, payload):
        if isinstance(payload, str):
            data = payload.encode("utf-8")
        else:
            data = bytes(payload)
        frame = struct.pack("!Q", len(data)) + data
        self._write_all(frame)

    def close(self):
        if self.closed:
            return
        self.closed = True
        k32 = kernel32()
        try:
            if k32.cancel_io_ex is not None:
                try:
                    k32.cancel_io_ex(self.handle, None)
                except Exception:
                    pass
            if self.owner_server_side:
                try:
                    k32.dll.DisconnectNamedPipe(self.handle)
                except Exception:
                    pass
        finally:
            try:
                k32.dll.CloseHandle(self.handle)
            except Exception:
                pass

    def _read_exact(self, size):
        chunks = []
        remaining = int(size)
        k32 = kernel32()
        while remaining > 0:
            chunk_size = min(remaining, DEFAULT_BUFFER_SIZE)
            buf = ctypes.create_string_buffer(chunk_size)
            read = wintypes.DWORD(0)
            ok = k32.dll.ReadFile(self.handle, buf, chunk_size, ctypes.byref(read), None)
            if not ok:
                error = k32.last_error()
                if error in (ERROR_BROKEN_PIPE, ERROR_NO_DATA, ERROR_PIPE_NOT_CONNECTED):
                    return None
                raise OSError(error, "ReadFile failed with winerror=%s" % error)
            if read.value == 0:
                return None
            chunks.append(buf.raw[:read.value])
            remaining -= read.value
        return b"".join(chunks)

    def _write_all(self, data):
        offset = 0
        total = len(data)
        k32 = kernel32()
        with self.write_lock:
            while offset < total:
                chunk = data[offset:offset + DEFAULT_BUFFER_SIZE]
                buf = ctypes.create_string_buffer(chunk)
                written = wintypes.DWORD(0)
                ok = k32.dll.WriteFile(self.handle, buf, len(chunk), ctypes.byref(written), None)
                if not ok:
                    error = k32.last_error()
                    raise OSError(error, "WriteFile failed with winerror=%s" % error)
                if written.value <= 0:
                    raise OSError("WriteFile wrote zero bytes")
                offset += written.value


def connect_pipe(pipe_name=None, timeout_ms=3000):
    pipe_name = normalize_pipe_name(pipe_name)
    k32 = kernel32()
    deadline = time.time() + max(float(timeout_ms), 1.0) / 1000.0
    last_error = None
    while True:
        handle = k32.dll.CreateFileW(
            pipe_name,
            GENERIC_READ | GENERIC_WRITE,
            0,
            None,
            OPEN_EXISTING,
            0,
            None,
        )
        if not k32.invalid_handle(handle):
            return NamedPipeConnection(handle, pipe_name, owner_server_side=False)
        last_error = k32.last_error()
        if last_error == ERROR_PIPE_BUSY:
            wait_ms = min(250, max(1, int((deadline - time.time()) * 1000)))
            k32.dll.WaitNamedPipeW(pipe_name, wait_ms)
        elif last_error in (ERROR_FILE_NOT_FOUND, ERROR_ACCESS_DENIED):
            time.sleep(0.05)
        else:
            time.sleep(0.05)
        if time.time() >= deadline:
            raise OSError(last_error or 0, "connect named pipe timeout pipe=%s winerror=%s" % (pipe_name, last_error))


def create_pipe_instance(pipe_name=None, in_buffer_size=DEFAULT_BUFFER_SIZE, out_buffer_size=DEFAULT_BUFFER_SIZE):
    pipe_name = normalize_pipe_name(pipe_name)
    k32 = kernel32()
    handle = k32.dll.CreateNamedPipeW(
        pipe_name,
        PIPE_ACCESS_DUPLEX,
        PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
        PIPE_UNLIMITED_INSTANCES,
        int(out_buffer_size),
        int(in_buffer_size),
        0,
        None,
    )
    if k32.invalid_handle(handle):
        k32.raise_last_error("CreateNamedPipeW")
    return NamedPipeConnection(handle, pipe_name, owner_server_side=True)


def wait_for_pipe_client(connection):
    k32 = kernel32()
    ok = k32.dll.ConnectNamedPipe(connection.handle, None)
    if ok:
        return True
    error = k32.last_error()
    if error == ERROR_PIPE_CONNECTED:
        return True
    raise OSError(error, "ConnectNamedPipe failed with winerror=%s" % error)


class PipeTxClient(object):
    """
    QMT-side tx-like adapter over a named pipe hub.

    It intentionally exposes start_tx/start_txg/push/close so bridge classes can
    reuse the existing LTtx-oriented dispatch code without changing behavior.
    """

    def __init__(
        self,
        pipe_name=None,
        request_channel="cfquant.request",
        request_channels=None,
        bridge_id="default",
        endpoint_name="qmt",
        show=True,
        connect_timeout_ms=3000,
        reconnect_interval=1.0,
        heartbeat_interval=None,
    ):
        self.pipe_name = normalize_pipe_name(pipe_name)
        self.request_channel = request_channel
        self.request_channels = self._normalize_channels(request_channels or [request_channel])
        self.bridge_id = bridge_id or "default"
        self.endpoint_name = endpoint_name or "qmt"
        self.show = show
        self.connect_timeout_ms = int(connect_timeout_ms)
        self.reconnect_interval = float(reconnect_interval)
        if heartbeat_interval is None:
            heartbeat_interval = os.environ.get("CFQUANT_PIPE_HEARTBEAT_SECONDS", "10")
        try:
            self.heartbeat_interval = max(0.0, float(heartbeat_interval))
        except Exception:
            self.heartbeat_interval = 10.0
        self.process_id = os.getpid()
        self.instance_id = os.environ.get("CFQUANT_PIPE_INSTANCE_ID") or "%s.%s.%s.%s" % (
            self.bridge_id,
            self.endpoint_name,
            self.process_id,
            uuid.uuid4().hex[:12],
        )
        self.binary_enabled = str(
            os.environ.get("CFQUANT_PIPE_BINARY") or "1"
        ).strip().lower() not in ("0", "false", "no", "off")
        self._hub_binary = False
        self.Q = queue.Queue(maxsize=10000)
        self.running = False
        self.rx_conn = None
        self.tx_conn = None
        self.connection_generation = 0
        self.last_connected_at = 0.0
        self.last_error = ""
        self.conn_lock = threading.RLock()
        self.thread = None
        self.heartbeat_thread = None

    def start(self):
        if self.running:
            return self
        self.running = True
        self.thread = threading.Thread(target=self._connect_loop)
        self.thread.daemon = True
        self.thread.start()
        if self.heartbeat_interval > 0:
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()
        return self

    def start_tx(self):
        return self.start()

    def start_txg(self, request_channel=None):
        if request_channel:
            self.request_channel = request_channel
        return {"code": 0, "msg": "pipe request channel registered"}

    def push(self, key, payload, channel):
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")
        request_id = None
        event_name = None
        if self._hub_binary:
            if key == "response":
                request_id = scan_response_request_id(payload)
            elif key == "event":
                event_name = scan_event_name(payload)
        return self.push_routed(key, payload, channel, request_id=request_id, event_name=event_name)

    def push_routed(self, key, payload, channel, request_id=None, action=None,
                    event_name=None, subscription_id=None, flags=0):
        """Send a publish frame; callers that know the routing fields skip the head scan."""
        conn, generation = self._get_tx_state()
        if conn is None:
            rx_conn, rx_generation = self._get_rx_state()
            if rx_conn is not None:
                self._drop_pair(rx_generation, reason="pipe tx connection missing", conns=(rx_conn,))
            return {"code": -1, "msg": "pipe not connected"}
        if self._hub_binary:
            frame = encode_pipe_frame(
                _FRAME_TYPES_BY_NAME.get(str(key), FRAME_TYPE_PUBLISH_EVENT),
                payload,
                request_id=request_id,
                client_id=channel,
                channel=channel,
                action=action,
                event_name=event_name,
                subscription_id=subscription_id,
                flags=flags,
            )
            try:
                conn.write_frame(frame)
                return {"code": 0, "msg": "ok"}
            except Exception as e:
                message = "pipe push failed: %s" % e
                self._log(message)
                self._drop_pair(generation, reason=message, conns=(conn,))
                return {"code": -1, "msg": str(e)}
        envelope = dumps_pipe_message(self._pipe_envelope("publish", "qmt_tx", {
            "key": key,
            "channel": channel,
            "payload": payload,
        }))
        try:
            conn.write_frame(envelope)
            return {"code": 0, "msg": "ok"}
        except Exception as e:
            message = "pipe push failed: %s" % e
            self._log(message)
            self._drop_pair(generation, reason=message, conns=(conn,))
            return {"code": -1, "msg": str(e)}

    def push_columnar(self, event_name, data_bytes, client_id, subscription_id=None, channel=None):
        """Push a columnar whole-quote batch as a binary event frame.

        Returns False when the hot path is unavailable so the caller can fall
        back to the JSON event path.
        """
        if not self._hub_binary:
            return False
        conn, generation = self._get_tx_state()
        if conn is None:
            return False
        frame = encode_pipe_frame(
            FRAME_TYPE_PUBLISH_EVENT,
            data_bytes,
            client_id=client_id,
            channel=channel or client_id,
            event_name=event_name,
            subscription_id=subscription_id,
            flags=FRAME_FLAG_COLUMNAR,
        )
        try:
            conn.write_frame(frame)
            return True
        except Exception as e:
            message = "pipe push failed: %s" % e
            self._log(message)
            self._drop_pair(generation, reason=message, conns=(conn,))
            return False

    def close(self):
        self.running = False
        try:
            self.Q.put_nowait(None)
        except Exception:
            pass
        self._drop_pair(None, reason="pipe tx client closed", conns=self._get_conns())

    def _connect_loop(self):
        while self.running:
            rx_conn = None
            tx_conn = None
            generation = None
            try:
                self._hub_binary = False
                rx_conn = connect_pipe(self.pipe_name, timeout_ms=self.connect_timeout_ms)
                rx_conn.write_frame(dumps_pipe_message(self._pipe_envelope("hello", "qmt_rx")))
                tx_conn = connect_pipe(self.pipe_name, timeout_ms=self.connect_timeout_ms)
                tx_conn.write_frame(dumps_pipe_message(self._pipe_envelope("hello", "qmt_tx")))
                with self.conn_lock:
                    self.connection_generation += 1
                    generation = self.connection_generation
                    self.rx_conn = rx_conn
                    self.tx_conn = tx_conn
                    self.last_connected_at = time.time()
                    self.last_error = ""
                self._log(
                    "pipe connected pipe=%s request_channel=%s bridge_id=%s"
                    % (self.pipe_name, self.request_channel, self.bridge_id)
                )
                self._read_loop(rx_conn, generation)
            except Exception as e:
                self.last_error = str(e)
                if self.running:
                    self._log("pipe connect/read failed: %s" % e)
            finally:
                if generation is None:
                    self._close_conns((rx_conn, tx_conn))
                else:
                    self._drop_pair(generation, conns=(rx_conn, tx_conn))
            if self.running:
                time.sleep(self.reconnect_interval)

    def _read_loop(self, conn, generation):
        while self.running and self._get_rx_state() == (conn, generation):
            raw = conn.read_frame()
            if raw is None:
                break
            frame = decode_pipe_frame(raw)
            if frame is not None:
                if frame.get("type") == "hello_ack":
                    self._hub_binary = self.binary_enabled and bool(frame.get("binary_hotpath"))
                    continue
                payload = frame.get("payload")
                if payload is None:
                    continue
                if isinstance(payload, (bytes, bytearray)):
                    if not (frame.get("flags", 0) & FRAME_FLAG_COLUMNAR):
                        payload = payload.decode("utf-8", errors="replace")
                    self.Q.put(payload)
                elif payload:
                    self.Q.put(payload)
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            self.Q.put(raw)

    def _get_rx_conn(self):
        with self.conn_lock:
            return self.rx_conn

    def _get_tx_conn(self):
        with self.conn_lock:
            return self.tx_conn

    def _get_rx_state(self):
        with self.conn_lock:
            return self.rx_conn, self.connection_generation

    def _get_tx_state(self):
        with self.conn_lock:
            return self.tx_conn, self.connection_generation

    def _get_conn(self):
        with self.conn_lock:
            if self.rx_conn is not None and self.tx_conn is not None:
                return self.rx_conn
            return None

    def _get_conns(self):
        with self.conn_lock:
            return self.rx_conn, self.tx_conn

    def _drop_conn(self, conn):
        if conn is None:
            return
        with self.conn_lock:
            generation = self.connection_generation if conn is self.rx_conn or conn is self.tx_conn else None
        if generation is None:
            self._close_conns((conn,))
            return
        self._drop_pair(generation, conns=(conn,))

    def _drop_conns(self, conns):
        self._drop_pair(None, conns=conns)

    def _drop_pair(self, generation=None, reason="", conns=()):
        close_conns = []
        with self.conn_lock:
            if generation is None or self.connection_generation == generation:
                for conn in (self.rx_conn, self.tx_conn):
                    if conn is not None and conn not in close_conns:
                        close_conns.append(conn)
                self.rx_conn = None
                self.tx_conn = None
                if reason:
                    self.last_error = reason
            for conn in conns or ():
                if conn is None:
                    continue
                if conn not in close_conns:
                    close_conns.append(conn)
        self._close_conns(close_conns)

    def _close_conns(self, conns):
        for conn in conns or ():
            if conn is None:
                continue
            try:
                conn.close()
            except Exception:
                pass

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(max(0.1, self.heartbeat_interval))
            if not self.running:
                break
            conn, generation = self._get_tx_state()
            if conn is None:
                continue
            try:
                conn.write_frame(dumps_pipe_message(self._pipe_envelope("heartbeat", "qmt_tx")))
            except Exception as e:
                message = "pipe heartbeat failed: %s" % e
                self._log(message)
                self._drop_pair(generation, reason=message, conns=(conn,))

    def _pipe_envelope(self, msg_type, role, extra=None):
        data = {
            "type": msg_type,
            "role": role,
            "bridge_id": self.bridge_id,
            "request_channel": self.request_channel,
            "request_channels": self.request_channels,
            "endpoint_name": self.endpoint_name,
            "instance_id": self.instance_id,
            "process_id": self.process_id,
            "heartbeat_interval": self.heartbeat_interval,
            "binary_hotpath": self.binary_enabled,
        }
        if extra:
            data.update(extra)
        return data

    def _log(self, msg):
        if self.show and get_log_enabled():
            print("cfquant pipe tx %s" % translate_log(msg))

    def _normalize_channels(self, channels):
        result = []
        for channel in channels or []:
            channel = str(channel or "").strip()
            if channel and channel not in result:
                result.append(channel)
        return result or [self.request_channel]
