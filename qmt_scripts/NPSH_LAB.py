#coding:gbk
#! /usr/bin/python
# NPSH_LAB.py
# Self-contained named-pipe single-hop lab entry for QMT whitelist environments.
# This file intentionally avoids importing anything outside the Python stdlib.
#
# Experiment: forward the QMT builtin get_etf_iopv(stockcode) / passorder(...)
# to ONE external process over a local named pipe - no pipe hub process, no
# two-segment pipes, no LTtx server, no cfquant package imports. The external
# caller uses qmt_scripts/npsh_lab_client.py (or NpshClient from
# qmt_scripts/npsh_lite.py) and the calls read like local functions.
#
# Single-hop design (named-pipe rework of the AF_UNIX IOPV_LAB experiment,
# same architecture and wire protocol):
#   - the strategy runs NpshServer: ONE named-pipe instance, so at most one
#     external client is connected at any time; a vanished client re-arms the
#     same instance (no QMT restart needed)
#   - queries (get_etf_iopv ...) execute directly on the pipe serve thread -
#     no queue, no QMT main-thread pump, no schedule_run involvement
#   - passorder keeps QMT thread affinity: the serve thread queues it and the
#     schedule_run timer (10 ms) / handlebar pump executes it on the QMT
#     strategy thread, exactly like the cfquant bridge main_thread_queue
#
# The transport block between the BEGIN/END NPSH_LITE_TRANSPORT markers is a
# verbatim embed of qmt_scripts/npsh_lite.py - keep the two in sync (edit
# npsh_lite.py, then re-splice with tools/deploy_npsh_lab.py --sync).

import os
import sys
sys.dont_write_bytecode = True
import datetime as dt
import inspect
import queue
import time
import traceback

# BEGIN NPSH_LITE_TRANSPORT (embedded from qmt_scripts/npsh_lite.py - keep in sync)
"""npsh_lite: zero-dependency single-hop named-pipe RPC for Windows.

Single-hop bridge transport between ONE external client process and the
QMT terminal's embedded Python. The QMT-side strategy embeds this module
verbatim and runs NpshServer: exactly one named-pipe instance, no hub
process, no two-segment pipes, no rx/tx connection pair. The external
process runs NpshClient: one full-duplex connection with a resident
reader thread.

Design notes (architecture mirrors the AF_UNIX experiment afunix_lite /
IOPV_LAB, tag v0.1 - since removed; same framing and wire protocol):
  - stdlib only (ctypes / json / struct / threading); no cfquant imports
  - pure-ASCII source so the QMT terminal can safely recode it (coding:gbk)
  - compatible with the QMT embedded Python 3.6 and repo venv Python 3.12
  - all handles are FILE_FLAG_OVERLAPPED and every ReadFile/WriteFile/
    ConnectNamedPipe runs through an OVERLAPPED + GetOverlappedResult
    helper. This is REQUIRED for the resident-reader design: on a
    synchronous (non-overlapped) named-pipe handle, Windows serializes
    operations per handle, so a ReadFile parked in the reader thread
    blocks a WriteFile from any other thread on the same handle until it
    returns (measured: write stalls forever behind a parked read). With
    overlapped handles the two directions run concurrently, and ONE
    connection carries pipelined requests, responses and server events -
    no lock-step, no rx/tx split.
  - single client by construction: the server creates exactly ONE pipe
    instance, so a second caller gets ERROR_PIPE_BUSY until the first
    disconnects. Concurrency is the client's business (in-flight requests
    multiplexed on its reader thread), not the server's.

Server lifecycle: the serve thread owns the pipe instance handle for its
whole life - wait_for_pipe_client -> serve frames inline -> DisconnectNamedPipe
-> wait for the next client on the same handle. A vanished client shows up
as ReadFile completing with ERROR_BROKEN_PIPE, so the external client may
restart freely without restarting QMT. Requests execute directly on the
serve thread (no queue, no pump); the embedding strategy decides which
actions need main-thread pumping and does that inside its handler.

Wire protocol (deliberately tiny; "!Q" length-prefixed frames, identical
framing to cfquant.pipe_transport.NamedPipeConnection):
  frame   = struct "!Q" payload_length + payload
  payload = b"npsh1:" + compact JSON (ensure_ascii)
  request  {"t":"req","id":..,"a":action,"p":[args],"k":{kwargs}}
  response {"t":"res","id":..,"ok":bool,"r":result,"e":error,"c":true-if-str-coerced}
  event    {"t":"evt","n":name,"d":data}   (server -> client push)

Pipe name resolution: explicit arg > env CFQUANT_SINGLEHOP_PIPE >
DEFAULT_PIPE_NAME. Both sides must agree on the name; the QMT-side entry
embeds this module verbatim and therefore shares the same default.
"""

import ctypes
import json
import os
import queue
import struct
import threading
import time
import uuid
from ctypes import wintypes

__all__ = [
    "NpshError",
    "NpshConnectError",
    "NpshTimeout",
    "NpshRemoteError",
    "DEFAULT_PIPE_NAME",
    "PIPE_NAME_ENV",
    "resolve_pipe_name",
    "NpshServer",
    "NpshClient",
    "create_pipe_instance",
    "wait_for_pipe_client",
    "connect_pipe",
]

DEFAULT_PIPE_NAME = r"\\.\pipe\cfquant_singlehop"
PIPE_NAME_ENV = "CFQUANT_SINGLEHOP_PIPE"

WIRE_MAGIC = b"npsh1:"
_MAX_FRAME = 16 * 1024 * 1024
_FRAME_HEAD = struct.Struct("!Q")

_PING_ACTION = "__ping__"

GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
FILE_FLAG_OVERLAPPED = 0x40000000
PIPE_ACCESS_DUPLEX = 0x00000003
PIPE_TYPE_BYTE = 0x00000000
PIPE_READMODE_BYTE = 0x00000000
PIPE_WAIT = 0x00000000
NMPWAIT_USE_DEFAULT_WAIT = 0

ERROR_FILE_NOT_FOUND = 2
ERROR_ACCESS_DENIED = 5
ERROR_BROKEN_PIPE = 109
ERROR_PIPE_BUSY = 231
ERROR_NO_DATA = 232
ERROR_PIPE_NOT_CONNECTED = 233
ERROR_PIPE_CONNECTED = 535
ERROR_IO_PENDING = 997

DEFAULT_BUFFER_SIZE = 65536


class NpshError(Exception):
    """Base error for the npsh_lite transport."""


class NpshConnectError(NpshError):
    """The pipe could not be connected to or the stream broke."""


class NpshTimeout(NpshError):
    """A request did not receive its response within the timeout."""


class NpshRemoteError(NpshError):
    """The server executed the action and it raised."""


def resolve_pipe_name(explicit=None):
    name = explicit or os.environ.get(PIPE_NAME_ENV) or DEFAULT_PIPE_NAME
    name = str(name)
    if name.startswith("\\\\.\\pipe\\"):
        return name
    return r"\\.\pipe\%s" % name.strip("\\/")


# --------------------------------------------------------------------------
# kernel32 via ctypes (overlapped IO)
# --------------------------------------------------------------------------


class _OVERLAPPED(ctypes.Structure):
    _fields_ = [
        ("Internal", wintypes.LPVOID),
        ("InternalHigh", wintypes.LPVOID),
        ("Offset", wintypes.DWORD),
        ("OffsetHigh", wintypes.DWORD),
        ("hEvent", wintypes.HANDLE),
    ]


class _Kernel32(object):
    def __init__(self):
        if os.name != "nt":
            raise NpshError("npsh_lite only supports Windows (ctypes kernel32)")
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

        self.dll.CreateEventW.argtypes = [
            wintypes.LPVOID,
            wintypes.BOOL,
            wintypes.BOOL,
            wintypes.LPCWSTR,
        ]
        self.dll.CreateEventW.restype = wintypes.HANDLE

        self.dll.GetOverlappedResult.argtypes = [
            wintypes.HANDLE,
            wintypes.LPVOID,
            ctypes.POINTER(wintypes.DWORD),
            wintypes.BOOL,
        ]
        self.dll.GetOverlappedResult.restype = wintypes.BOOL

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

    def invalid_handle(self, handle):
        return handle in (None, 0, self.INVALID_HANDLE_VALUE)


_kernel32 = None
_kernel32_lock = threading.Lock()


def kernel32():
    global _kernel32
    with _kernel32_lock:
        if _kernel32 is None:
            # 1 ms sleep granularity for pump timers and deadline waits
            # (default Windows timer resolution is 15.6 ms; the lite bridge
            # and the retired afunix lab did the same)
            try:
                ctypes.WinDLL("winmm").timeBeginPeriod(1)
            except Exception:
                pass
            _kernel32 = _Kernel32()
        return _kernel32


def _run_overlapped(handle, io_func, buf, size):
    """Run ReadFile/WriteFile on an overlapped handle and wait for it.

    Returns (ok, bytes_done, winerror). ok=False with winerror set means
    the operation failed outright (broken pipe, aborted by CancelIoEx, ...);
    a broken-pipe style error is reported, the caller decides what it means.
    """
    k32 = kernel32()
    overlapped = _OVERLAPPED()
    event = k32.dll.CreateEventW(None, True, False, None)
    if k32.invalid_handle(event):
        return False, 0, k32.last_error() or 0
    overlapped.hEvent = event
    try:
        transferred = wintypes.DWORD(0)
        ok = io_func(handle, buf, size, ctypes.byref(transferred),
                     ctypes.byref(overlapped))
        if ok:
            return True, transferred.value, 0
        error = k32.last_error()
        if error != ERROR_IO_PENDING:
            return False, 0, error
        done = wintypes.DWORD(0)
        ok = k32.dll.GetOverlappedResult(handle, ctypes.byref(overlapped),
                                         ctypes.byref(done), True)
        if ok:
            return True, done.value, 0
        return False, 0, k32.last_error()
    finally:
        try:
            k32.dll.CloseHandle(event)
        except Exception:
            pass


def _is_disconnect_error(error):
    return error in (ERROR_BROKEN_PIPE, ERROR_NO_DATA, ERROR_PIPE_NOT_CONNECTED)


# --------------------------------------------------------------------------
# connection + message codec
# --------------------------------------------------------------------------


class NpshConnection(object):
    """One overlapped named-pipe handle with length-prefixed frame IO.

    Full duplex: a ReadFile parked in one thread never blocks a WriteFile
    from another thread because every operation carries its own OVERLAPPED
    (this is the whole reason the handles are opened overlapped). All
    writes serialize on an internal lock so concurrent senders never
    interleave frames.
    """

    def __init__(self, handle, name="", owner_server_side=False,
                 max_frame_size=_MAX_FRAME, buffer_size=DEFAULT_BUFFER_SIZE):
        self.handle = handle
        self.name = name
        self.owner_server_side = bool(owner_server_side)
        self.max_frame_size = int(max_frame_size)
        self.buffer_size = int(buffer_size)
        self.write_lock = threading.Lock()
        self.closed = False

    def read_frame(self):
        """Read one frame; None means the peer went away (EOF/broken pipe)."""
        header = self._read_exact(_FRAME_HEAD.size)
        if header is None:
            return None
        (size,) = _FRAME_HEAD.unpack(header)
        if size > self.max_frame_size:
            raise NpshError("named pipe frame too large: %s > %s"
                            % (size, self.max_frame_size))
        if size == 0:
            return b""
        return self._read_exact(size)

    def write_frame(self, payload):
        if isinstance(payload, str):
            data = payload.encode("utf-8")
        else:
            data = bytes(payload)
        self._write_all(_FRAME_HEAD.pack(len(data)) + data)

    def disconnect(self):
        """Server side: drop the current client but keep the instance handle
        valid so ConnectNamedPipe can re-arm it for the next client."""
        if self.closed or not self.owner_server_side:
            return
        try:
            kernel32().dll.DisconnectNamedPipe(self.handle)
        except Exception:
            pass

    def close(self):
        if self.closed:
            return
        self.closed = True
        k32 = kernel32()
        try:
            if k32.cancel_io_ex is not None:
                # cancel first so a parked overlapped IO in another thread
                # completes (with ERROR_OPERATION_ABORTED) before the handle
                # goes away - CloseHandle alone on a handle with parked IO
                # can wedge the closing thread
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
            chunk_size = min(remaining, self.buffer_size)
            buf = ctypes.create_string_buffer(chunk_size)
            ok, done, error = _run_overlapped(self.handle, k32.dll.ReadFile,
                                              buf, chunk_size)
            if not ok:
                if _is_disconnect_error(error):
                    return None
                raise OSError(error, "ReadFile failed with winerror=%s" % error)
            if done == 0:
                return None
            chunks.append(buf.raw[:done])
            remaining -= done
        return b"".join(chunks)

    def _write_all(self, data):
        offset = 0
        total = len(data)
        k32 = kernel32()
        with self.write_lock:
            while offset < total:
                chunk = data[offset:offset + self.buffer_size]
                buf = ctypes.create_string_buffer(chunk)
                ok, done, error = _run_overlapped(self.handle, k32.dll.WriteFile,
                                                  buf, len(chunk))
                if not ok:
                    raise OSError(error, "WriteFile failed with winerror=%s" % error)
                if done <= 0:
                    raise OSError("WriteFile wrote zero bytes")
                offset += done


def encode_message(obj):
    return WIRE_MAGIC + json.dumps(
        obj, ensure_ascii=True, separators=(",", ":")).encode("ascii")


def decode_message(payload):
    """Decode one frame payload; None when it is not our wire protocol."""
    if not payload.startswith(WIRE_MAGIC):
        return None
    try:
        return json.loads(payload[len(WIRE_MAGIC):].decode("utf-8"))
    except Exception:
        return None


def create_pipe_instance(pipe_name=None, max_instances=1,
                         in_buffer_size=DEFAULT_BUFFER_SIZE,
                         out_buffer_size=DEFAULT_BUFFER_SIZE):
    """Create ONE overlapped server-side pipe instance (max_instances=1
    keeps the single-client policy: a second server start fails loudly
    instead of splitting traffic between two instances)."""
    pipe_name = resolve_pipe_name(pipe_name)
    k32 = kernel32()
    handle = k32.dll.CreateNamedPipeW(
        pipe_name,
        PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
        PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
        int(max_instances),
        int(out_buffer_size),
        int(in_buffer_size),
        NMPWAIT_USE_DEFAULT_WAIT,
        None,
    )
    if k32.invalid_handle(handle):
        error = k32.last_error()
        raise NpshError("CreateNamedPipeW failed winerror=%s pipe=%s"
                        % (error, pipe_name))
    try:
        return NpshConnection(handle, pipe_name, owner_server_side=True,
                              buffer_size=out_buffer_size)
    except Exception:
        # never leak a created instance: an orphan listening handle accepts
        # clients that then hang forever on their first write
        try:
            k32.dll.CloseHandle(handle)
        except Exception:
            pass
        raise


def wait_for_pipe_client(connection):
    """Block until a client connects to this instance.

    ConnectNamedPipe on an overlapped handle must be given an OVERLAPPED;
    the wait itself is the overlapped completion via GetOverlappedResult.
    CancelIoEx (close()) aborts it with ERROR_OPERATION_ABORTED.
    """
    k32 = kernel32()
    handle = connection.handle
    overlapped = _OVERLAPPED()
    event = k32.dll.CreateEventW(None, True, False, None)
    if k32.invalid_handle(event):
        raise OSError(k32.last_error() or 0, "CreateEventW failed")
    overlapped.hEvent = event
    try:
        ok = k32.dll.ConnectNamedPipe(handle, ctypes.byref(overlapped))
        error = 0 if ok else k32.last_error()
        if not ok and error not in (ERROR_IO_PENDING, ERROR_PIPE_CONNECTED):
            raise OSError(error, "ConnectNamedPipe failed with winerror=%s" % error)
        if not ok and error == ERROR_IO_PENDING:
            done = wintypes.DWORD(0)
            ok = k32.dll.GetOverlappedResult(handle, ctypes.byref(overlapped),
                                             ctypes.byref(done), True)
            if not ok:
                error = k32.last_error()
                if error == ERROR_PIPE_CONNECTED:
                    return True
                raise OSError(error, "ConnectNamedPipe wait failed with winerror=%s" % error)
        return True
    finally:
        try:
            k32.dll.CloseHandle(event)
        except Exception:
            pass


def connect_pipe(pipe_name=None, timeout_ms=3000):
    """Client side: open the pipe (CreateFileW), retrying while the single
    server instance is busy or not yet listening."""
    pipe_name = resolve_pipe_name(pipe_name)
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
            FILE_FLAG_OVERLAPPED,
            None,
        )
        if not k32.invalid_handle(handle):
            return NpshConnection(handle, pipe_name, owner_server_side=False)
        last_error = k32.last_error()
        if last_error == ERROR_PIPE_BUSY:
            wait_ms = min(250, max(1, int((deadline - time.time()) * 1000)))
            if wait_ms > 0:
                k32.dll.WaitNamedPipeW(pipe_name, wait_ms)
        else:
            time.sleep(0.05)
        if time.time() >= deadline:
            raise NpshConnectError(
                "connect named pipe timeout pipe=%s winerror=%s (is the "
                "QMT-side server running?)" % (pipe_name, last_error))


# --------------------------------------------------------------------------
# server
# --------------------------------------------------------------------------


class NpshServer(object):
    """Single-hop named-pipe server: one instance, one client at a time.

    handler(action, args, kwargs) -> json-serializable result; raise any
    exception to answer that request with an error response. The handler
    runs directly on the serve thread - this is the no-pump query path.

    send_event(name, data) may be called from ANY thread (e.g. a QMT
    callback) to push an event frame to the connected client.
    """

    def __init__(self, pipe_name=None, handler=None, log=None):
        self.pipe_name = resolve_pipe_name(pipe_name)
        self.handler = handler
        self.log = log
        self.running = False
        self.client_connected = False
        self.last_error = ""
        self._conn = None
        self._thread = None

    def start(self):
        if self.running:
            return self
        # create the instance synchronously so init() sees create errors
        # immediately instead of logging "ready" for a dead server thread
        conn = create_pipe_instance(self.pipe_name)
        self._conn = conn
        self.running = True
        self._thread = threading.Thread(target=self._serve_loop, args=(conn,))
        self._thread.daemon = True
        self._thread.start()
        return self

    def send_event(self, name, data):
        """Push an event to the connected client; False when disconnected."""
        conn = self._conn
        if conn is None or not self.client_connected:
            return False
        try:
            conn.write_frame(encode_message({"t": "evt", "n": name, "d": data}))
            return True
        except Exception as exc:
            self.last_error = "send_event failed: %s" % exc
            return False

    def close(self):
        self.running = False
        conn = self._conn
        self._conn = None
        if conn is not None:
            conn.close()  # CancelIoEx unblocks a parked Connect/ReadFile
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(0.5)

    def _serve_loop(self, conn):
        self._log("pipe instance ready pipe=%s" % self.pipe_name)
        while self.running:
            try:
                wait_for_pipe_client(conn)
            except Exception as exc:
                if self.running:
                    self.last_error = str(exc)
                    self._log("wait_for_pipe_client failed: %s" % exc)
                break
            if not self.running:
                break
            self.client_connected = True
            self._log("client connected")
            try:
                while self.running:
                    payload = conn.read_frame()
                    if payload is None:
                        break
                    msg = decode_message(payload)
                    if msg is None:
                        self._log("client sent a non-npsh frame; dropping it")
                        break
                    if msg.get("t") != "req":
                        continue
                    reply = self._handle(msg)
                    if reply is None:
                        continue
                    conn.write_frame(encode_message(reply))
            except Exception as exc:
                if self.running:
                    self.last_error = str(exc)
                    self._log("client loop failed: %s" % exc)
            finally:
                self.client_connected = False
                conn.disconnect()
                self._log("client disconnected; re-arming pipe instance")
        conn.close()

    def _handle(self, msg):
        msg_id = msg.get("id")
        action = msg.get("a")
        args = msg.get("p") or []
        kwargs = msg.get("k") or {}
        if not isinstance(args, list) or not isinstance(kwargs, dict):
            return {"t": "res", "id": msg_id, "ok": False,
                    "e": "args must be list/dict"}
        if action == _PING_ACTION:
            return {"t": "res", "id": msg_id, "ok": True,
                    "r": {"pong": True, "server_time": round(time.time(), 6)}}
        if self.handler is None:
            return {"t": "res", "id": msg_id, "ok": False,
                    "e": "no handler configured"}
        try:
            result = self.handler(action, args, kwargs)
        except Exception as exc:
            return {"t": "res", "id": msg_id, "ok": False,
                    "e": "%s: %s" % (type(exc).__name__, exc)}
        try:
            json.dumps(result)
        except (TypeError, ValueError):
            # QMT builtins can return exotic objects; degrade to str() so the
            # caller at least sees something instead of killing the connection
            return {"t": "res", "id": msg_id, "ok": True, "r": str(result), "c": True}
        return {"t": "res", "id": msg_id, "ok": True, "r": result}

    def _log(self, message):
        callback = self.log
        if callback is not None:
            try:
                callback(message)
            except Exception:
                pass


# --------------------------------------------------------------------------
# client
# --------------------------------------------------------------------------


class NpshClient(object):
    """Single-hop named-pipe client: one connection, resident reader thread.

    client.get_etf_iopv("510050.SH") forwards to the QMT-side handler and
    reads like a local call (attribute proxy -> request(action, args, kwargs)).
    Multiple threads may call at once: requests are pipelined on the same
    connection and responses are correlated by request id on the reader
    thread. A request timeout does NOT drop the connection - a late reply
    for an already-failed request id is simply discarded.
    """

    def __init__(self, pipe_name=None, timeout=5.0, connect_timeout_ms=3000):
        self.pipe_name = resolve_pipe_name(pipe_name)
        self.default_timeout = timeout
        self.connect_timeout_ms = int(connect_timeout_ms)
        self.on_event = None      # callable(name, data), reader thread
        self.on_disconnect = None  # callable(reason), reader thread
        self._conn = None
        self._state_lock = threading.Lock()
        self._pending = {}

    @property
    def connected(self):
        return self._conn is not None

    def connect(self):
        self._ensure_connected()

    def _ensure_connected(self):
        with self._state_lock:
            if self._conn is None:
                conn = connect_pipe(self.pipe_name,
                                    timeout_ms=self.connect_timeout_ms)
                self._conn = conn
                reader = threading.Thread(target=self._reader_loop, args=(conn,))
                reader.daemon = True
                reader.start()

    def _drop(self, conn):
        with self._state_lock:
            if self._conn is conn:
                self._conn = None
        conn.close()

    def request(self, action, args=None, kwargs=None, timeout=None):
        args = list(args) if args else []
        kwargs = dict(kwargs) if kwargs else {}
        effective = timeout if (timeout is not None and timeout > 0) else self.default_timeout
        rid = "r%d_%s" % (int(time.time() * 1000) % 1000000000,
                          uuid.uuid4().hex[:8])
        msg = {"t": "req", "id": rid, "a": action, "p": args, "k": kwargs}
        self._ensure_connected()
        with self._state_lock:
            conn = self._conn
            wait = queue.Queue(1)
            if conn is not None:
                self._pending[rid] = wait
        if conn is None:
            raise NpshConnectError("connection lost before send (%s)"
                                   % self.pipe_name)
        try:
            conn.write_frame(encode_message(msg))
        except Exception:
            with self._state_lock:
                self._pending.pop(rid, None)
            self._drop(conn)
            raise
        try:
            reply = wait.get(timeout=effective)
        except queue.Empty:
            with self._state_lock:
                self._pending.pop(rid, None)
            raise NpshTimeout("request '%s' timed out after %.3fs (%s)"
                              % (action, effective, self.pipe_name))
        if reply.get("x") == "disconnect":
            raise NpshConnectError("connection closed while '%s' was in "
                                   "flight: %s" % (action, reply.get("e")))
        if not reply.get("ok"):
            raise NpshRemoteError("remote '%s' failed: %s"
                                  % (action, reply.get("e")))
        return reply.get("r")

    def ping(self, timeout=None):
        return self.request(_PING_ACTION, timeout=timeout)

    def close(self):
        with self._state_lock:
            conn = self._conn
            self._conn = None
            pending = self._pending
            self._pending = {}
        if conn is not None:
            conn.close()
        for wait in pending.values():
            wait.put({"t": "res", "id": None, "ok": False,
                      "e": "client closed", "x": "disconnect"})

    def _reader_loop(self, conn):
        reason = "connection closed"
        try:
            while True:
                try:
                    payload = conn.read_frame()
                except Exception as exc:
                    reason = "read failed: %s" % exc
                    break
                if payload is None:
                    break
                msg = decode_message(payload)
                if msg is None:
                    reason = "bad frame magic (protocol mismatch?)"
                    break
                kind = msg.get("t")
                if kind == "res":
                    with self._state_lock:
                        wait = self._pending.pop(msg.get("id"), None)
                    if wait is not None:
                        wait.put(msg)
                elif kind == "evt":
                    callback = self.on_event
                    if callback is not None:
                        try:
                            callback(msg.get("n"), msg.get("d"))
                        except Exception:
                            pass
        finally:
            with self._state_lock:
                if self._conn is conn:
                    self._conn = None
                pending = self._pending
                self._pending = {}
            for wait in pending.values():
                wait.put({"t": "res", "id": None, "ok": False,
                          "e": reason, "x": "disconnect"})
            callback = self.on_disconnect
            if callback is not None:
                try:
                    callback(reason)
                except Exception:
                    pass

    def __getattr__(self, name):
        # attribute proxy: any non-underscore attribute call is forwarded as
        # an action with the same name and positional/keyword arguments.
        # The reserved keyword _timeout is consumed as the per-call request
        # timeout (a plain "timeout" kwarg belongs to the remote function).
        if name.startswith("_"):
            raise AttributeError(name)

        def _proxy(*args, **kwargs):
            timeout = kwargs.pop("_timeout", None)
            return self.request(name, args, kwargs, timeout=timeout)

        _proxy.__name__ = name
        return _proxy

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

# END NPSH_LITE_TRANSPORT

# --------------------------------------------------------------------------
# QMT lab glue
# --------------------------------------------------------------------------

NPSH_ENTRY_VERSION = "npsh_lab_20261017"

# One name per forwarded QMT builtin; add entries here to expose more.
NPSH_ALLOWED_ACTIONS = ("get_etf_iopv", "passorder")

# passorder has QMT-thread affinity: the pipe serve thread must not call it
# directly. Requests are queued and executed by handlebar()/the schedule_run
# tick on the QMT strategy thread (same pattern as the cfquant bridge's
# main_thread_queue).
_nps_main_queue = None
_nps_passorder_wait = 10.0

_nps_server = None
_nps_context = None


def _nps_log(message):
    print("[npsh_lab %s] %s" % (time.strftime("%H:%M:%S"), message))


def _nps_get_callable(*names):
    # resolve a QMT builtin the same way the lite bridge does:
    # module globals -> ContextInfo -> ContextInfo.context -> builtins
    owners = [globals()]
    if _nps_context is not None:
        owners.append(_nps_context)
        inner = getattr(_nps_context, "context", None)
        if inner is not None and inner is not _nps_context:
            owners.append(inner)
    builtins_ns = getattr(__builtins__, "__dict__", __builtins__)
    owners.append(builtins_ns)
    for owner in owners:
        for name in names:
            if isinstance(owner, dict):
                func = owner.get(name)
            else:
                func = getattr(owner, name, None)
            if callable(func):
                return func
    return None


def _nps_pump():
    # drain the main-thread queue (called from handlebar / the schedule_run
    # tick on the QMT strategy thread)
    global _nps_main_queue
    if _nps_main_queue is None:
        return
    while True:
        try:
            wait, func, args, kwargs = _nps_main_queue.get_nowait()
        except Exception:
            break
        try:
            result = _nps_call_passorder(func, args, kwargs)
        except Exception as exc:
            result = {"__nps_error__": "%s: %s" % (type(exc).__name__, exc)}
        wait.put(result)


# passorder positional layout (order matches the QMT builtin signature,
# trailing ContextInfo is appended separately as a variant). The 10th
# argument is the caller-chosen order id (cfquant uses the same slot for
# its sync order-id resolution); there is no validType/validDate here.
_PASSORDER_ARG_KEYS = ("op_type", "order_type", "account", "code", "price_type",
                       "price", "volume", "strategy_name", "quick_trade",
                       "user_order_id")
_PASSORDER_ARG_DEFAULTS = (23, 1101, "", "", 44, 0, 100, "", 2, "")


def _nps_default_order_id():
    return "npsh_lab_%s" % int(time.time() * 1000)


def _nps_call_passorder(func, args, kwargs):
    # kwargs -> positional tuple (the QMT builtin only accepts positional
    # args; the attribute proxy sends everything as kwargs)
    if args:
        ordered = list(args)
        while len(ordered) < len(_PASSORDER_ARG_KEYS):
            ordered.append(None)
    else:
        merged = []
        for i, key in enumerate(_PASSORDER_ARG_KEYS):
            merged.append(kwargs.get(key, _PASSORDER_ARG_DEFAULTS[i]))
        ordered = merged
    if not ordered[-1]:
        ordered[-1] = _nps_default_order_id()
    ordered = tuple(ordered)
    variants = [(ordered + (_nps_context,), {}), (ordered, {})]
    # mirror cfquant._call_variants: when the signature is introspectable,
    # pre-bind to pick the matching variant; for C builtins fall back to a
    # TypeError probe. Never run a side-effecting call twice.
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        signature = None
    last_error = None
    for var_args, var_kwargs in variants:
        if signature is not None:
            try:
                signature.bind(*var_args, **var_kwargs)
            except TypeError as e:
                last_error = e
                continue
            return func(*var_args, **var_kwargs)
        try:
            return func(*var_args, **var_kwargs)
        except TypeError as e:
            last_error = e
    if last_error is not None:
        raise last_error
    raise RuntimeError("no usable passorder signature")


def _nps_passorder(func, args, kwargs):
    global _nps_main_queue
    if _nps_main_queue is None:
        raise RuntimeError("passorder pump not initialized")
    wait = queue.Queue(1)
    _nps_main_queue.put((wait, func, args, kwargs))
    try:
        result = wait.get(timeout=_nps_passorder_wait)
    except queue.Empty:
        raise NpshTimeout("passorder not executed on QMT thread within %.0fs"
                          % _nps_passorder_wait)
    if isinstance(result, dict) and result.get("__nps_error__"):
        raise RuntimeError(result["__nps_error__"])
    return result


_nps_schedule_key = None


def _nps_on_timer(*args, **kwargs):
    # QMT strategy-thread tick (registered via ContextInfo.schedule_run);
    # this is the fast path for the passorder pump - handlebar on a daily
    # chart only fires every ~11 s, far too slow for interactive orders
    _nps_pump()


def _nps_schedule_timer():
    global _nps_schedule_key
    if _nps_context is None or _nps_schedule_key is not None:
        return
    try:
        first_time = dt.datetime.now() + dt.timedelta(seconds=1)
        _nps_schedule_key = _nps_context.schedule_run(
            _nps_on_timer,
            first_time,
            repeat_times=-1,
            interval=dt.timedelta(milliseconds=10),
            name="npsh_lab_passorder_pump",
        )
        _nps_log("passorder timer scheduled key=%s interval_ms=10" % _nps_schedule_key)
    except Exception as exc:
        _nps_log("passorder timer schedule failed: %s" % exc)


def _nps_handler(action, args, kwargs):
    # runs on the pipe serve thread. get_etf_iopv is a plain data query and
    # executes right here - no pump, no QMT main-thread hop. passorder is
    # routed back to the QMT strategy thread (schedule_run tick / handlebar)
    # so the terminal's thread affinity check passes.
    if action not in NPSH_ALLOWED_ACTIONS:
        raise ValueError("action not allowed in lab: %s" % action)
    func = _nps_get_callable(action)
    if func is None:
        raise RuntimeError("QMT function '%s' not found (did init() finish?)" % action)
    if action == "passorder":
        return _nps_passorder(func, args, kwargs)
    return func(*args, **kwargs)


def _nps_tune_gil():
    # Shrink the GIL switch interval (default 5 ms). After the serve thread
    # wakes from overlapped IO it must re-acquire the GIL to run the request;
    # with several 10 ms timers in this process (our pump, the LITE bridge
    # pump; during measurement also the since-removed IOPV_LAB pump)
    # responses quantize into ~10-30 ms steps - the
    # same mechanism as the LITE note where a 500 ms timer quantized
    # responses to 0.5 s multiples. Measured: real-QMT query p50 drops well
    # below 30 ms with a 1 ms switch interval. Process-wide, so the LITE
    # bridge benefits too.
    try:
        sys.setswitchinterval(0.001)
    except Exception:
        pass


def init(ContextInfo):
    global _nps_server, _nps_context, _nps_main_queue

    _nps_tune_gil()
    _nps_context = ContextInfo
    if _nps_main_queue is None:
        _nps_main_queue = queue.Queue(maxsize=10000)
    _nps_schedule_timer()
    if _nps_server is None:
        try:
            _nps_server = NpshServer(handler=_nps_handler, log=_nps_log)
            _nps_server.start()
            _nps_log("npsh lab server ready version:%s pipe:%s"
                     % (NPSH_ENTRY_VERSION, _nps_server.pipe_name))
        except Exception:
            _nps_server = None
            _nps_log("npsh lab server failed to start:")
            traceback.print_exc()
    else:
        _nps_log("npsh lab server already running, context refreshed")


def after_init(ContextInfo):
    pass


def handlebar(ContextInfo):
    # QMT strategy thread: drain passorder requests queued by the serve thread
    _nps_pump()


def stop(ContextInfo):
    global _nps_server, _nps_schedule_key

    if _nps_server is not None:
        _nps_server.close()
        _nps_server = None
        _nps_log("npsh lab server stopped")
    # allow a later init() (strategy re-run) to schedule a fresh pump timer
    _nps_schedule_key = None
