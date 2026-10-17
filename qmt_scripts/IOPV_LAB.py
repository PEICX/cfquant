#coding:gbk
#! /usr/bin/python
# IOPV_LAB.py
# Self-contained AF_UNIX socket-file lab entry for QMT whitelist environments.
# This file intentionally avoids importing anything outside the Python stdlib.
#
# Experiment: forward the QMT builtin get_etf_iopv(stockcode) to external
# processes over a local unix-domain socket file - no pipe hub process,
# no LTtx server, no cfquant package imports.
#
# Deploy: copy this single file to <qmt_root>/python/IOPV_LAB.py and attach
# it as a model in QMT (any chart works, e.g. SH.000300 daily). Once init()
# has run, the socket file appears at DEFAULT_SOCK_PATH below and external
# callers can use qmt_scripts/iopv_lab_client.py.
#
# The transport block between the BEGIN/END AFUNIX_LITE_TRANSPORT markers is
# a verbatim embed of qmt_scripts/afunix_lite.py - keep the two in sync
# (edit afunix_lite.py, then re-splice the block into this file).

import os
import sys
sys.dont_write_bytecode = True
import datetime as dt
import inspect
import time
import traceback

# BEGIN AFUNIX_LITE_TRANSPORT (embedded from qmt_scripts/afunix_lite.py - keep in sync)
"""afunix_lite: zero-dependency local AF_UNIX socket-file RPC for Windows.

Lab transport for forwarding QMT terminal builtins (e.g. get_etf_iopv)
to external processes over a unix-domain socket file, without the
named-pipe hub / LTtx server machinery.

Design notes (mirrors the CFQUANT_LITE entry style):
  - stdlib only (json / struct / ctypes / threading); no cfquant imports
  - pure-ASCII source so the QMT terminal can safely recode it (coding:gbk)
  - compatible with the QMT embedded Python 3.6 and repo venv Python 3.12
  - Windows exposes AF_UNIX at the OS level but CPython has no
    socket.AF_UNIX on Windows, so the transport talks to ws2_32 via ctypes
  - one duplex AF_UNIX connection per client; server binds one socket file,
    listens, and spawns one thread per connection (no hub process needed)

Windows AF_UNIX quirks (all verified empirically, afunix.sys on Win11):
  - a socket that has a recv() pending in one thread blocks send() from
    another thread on the SAME socket, so full-duplex reader-thread designs
    deadlock. This transport therefore runs strict lock-step request/
    response on a single connection: the client only sends while it has no
    recv posted, and the server only replies after a full request arrived.
    Requests on one client are serialized; use several LabClient instances
    for parallelism. Server push / pipelining would need the lite-style
    separate rx/tx connections - out of scope for this lab.
  - setsockopt(SO_RCVTIMEO/SO_SNDTIMEO) returns success but is silently
    ignored. Client-side timeouts are therefore implemented with
    ioctlsocket(FIONBIO) non-blocking sockets plus deadline polling.

Wire protocol (deliberately tiny):
  frame   = struct "!Q" payload_length + payload        (same framing as lite)
  payload = b"aflab1:" + compact JSON (ensure_ascii)
  request  {"t":"req","id":..,"a":action,"p":[args],"k":{kwargs}}
  response {"t":"res","id":..,"ok":bool,"r":result,"e":error,"c":true-if-str-coerced}

Socket path resolution: explicit arg > env CFQUANT_AFUNIX_SOCK >
DEFAULT_SOCK_PATH. Both sides must agree on the path; the QMT-side entry
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

__all__ = [
    "LabError",
    "LabConnectError",
    "LabTimeout",
    "DEFAULT_SOCK_PATH",
    "SOCK_PATH_ENV",
    "resolve_sock_path",
    "LabServer",
    "LabClient",
]

DEFAULT_SOCK_PATH = r"D:\cfquant\runtime\cfquant_afunix_lab.sock"
SOCK_PATH_ENV = "CFQUANT_AFUNIX_SOCK"

WIRE_MAGIC = b"aflab1:"
_MAX_FRAME = 16 * 1024 * 1024
_FRAME_HEAD = struct.Struct("!Q")

_PING_ACTION = "__ping__"

_AF_UNIX = 1
_SOCK_STREAM = 1
_SD_BOTH = 2
_FIONBIO = 0x8004667E
_INVALID_SOCKET = 0xFFFFFFFFFFFFFFFF
_WSAEWOULDBLOCK = 10035


class LabError(Exception):
    """Base error for the afunix_lite lab transport."""


class LabConnectError(LabError):
    """The socket file could not be connected to or the stream broke."""


class LabTimeout(LabError):
    """A request did not receive its response within the timeout."""


def resolve_sock_path(explicit=None):
    path = explicit or os.environ.get(SOCK_PATH_ENV) or DEFAULT_SOCK_PATH
    path = os.path.abspath(path)
    raw = os.fsencode(path)
    if len(raw) + 1 > 108:
        raise LabError("socket path too long for sockaddr_un (max 107 bytes): %s" % path)
    return path


# --------------------------------------------------------------------------
# winsock via ctypes
# --------------------------------------------------------------------------


class _WSADATA(ctypes.Structure):
    _fields_ = [
        ("wVersion", ctypes.c_ushort),
        ("wHighVersion", ctypes.c_ushort),
        ("iMaxSockets", ctypes.c_ushort),
        ("iMaxUdpDg", ctypes.c_ushort),
        ("lpVendorInfo", ctypes.c_char_p),
        ("szDescription", ctypes.c_char * 257),
        ("szSystemStatus", ctypes.c_char * 129),
    ]


class _SockAddrUn(ctypes.Structure):
    _fields_ = [("sun_family", ctypes.c_ushort), ("sun_path", ctypes.c_char * 108)]


_ws2 = None
_ws2_lock = threading.Lock()


def _load_ws2():
    """Return the ws2_32 dll with prototypes set; WSAStartup once.

    SOCKET is UINT_PTR (64-bit on x64); ctypes' default c_int restype
    would silently truncate socket handles, so every entry point that
    returns or takes a SOCKET gets an explicit prototype.
    """
    global _ws2
    with _ws2_lock:
        if _ws2 is not None:
            return _ws2
        if os.name != "nt":
            raise LabError("afunix_lite only supports Windows (ctypes ws2_32)")
        dll = ctypes.WinDLL("ws2_32")
        st = ctypes.c_uint64
        dll.WSAStartup.argtypes = [ctypes.c_ushort, ctypes.c_void_p]
        dll.WSASocketW.restype = st
        dll.WSASocketW.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                   ctypes.c_void_p, ctypes.c_uint, ctypes.c_uint]
        dll.bind.argtypes = [st, ctypes.c_void_p, ctypes.c_int]
        dll.listen.argtypes = [st, ctypes.c_int]
        dll.accept.restype = st
        dll.accept.argtypes = [st, ctypes.c_void_p, ctypes.c_void_p]
        dll.connect.argtypes = [st, ctypes.c_void_p, ctypes.c_int]
        dll.send.restype = ctypes.c_int
        dll.send.argtypes = [st, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
        dll.recv.restype = ctypes.c_int
        dll.recv.argtypes = [st, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
        dll.ioctlsocket.restype = ctypes.c_int
        dll.ioctlsocket.argtypes = [st, ctypes.c_int, ctypes.c_void_p]
        dll.shutdown.argtypes = [st, ctypes.c_int]
        dll.closesocket.argtypes = [st]
        data = _WSADATA()
        rc = dll.WSAStartup(0x0202, ctypes.byref(data))
        if rc != 0:
            raise LabError("WSAStartup failed: %d" % rc)
        # 1 ms time.sleep granularity for the deadline polling below
        # (default Windows timer resolution is 15.6 ms; lite does the same)
        try:
            ctypes.WinDLL("winmm").timeBeginPeriod(1)
        except Exception:
            pass
        _ws2 = dll
        return dll


def _af_socket():
    dll = _load_ws2()
    sock = dll.WSASocketW(_AF_UNIX, _SOCK_STREAM, 0, None, 0, 0)
    if sock == _INVALID_SOCKET:
        raise LabConnectError("WSASocketW(AF_UNIX) failed err=%d" % dll.WSAGetLastError())
    return sock


def _sock_addr(path):
    raw = os.fsencode(path)
    addr = _SockAddrUn()
    addr.sun_family = _AF_UNIX
    addr.sun_path = raw
    return addr


# --------------------------------------------------------------------------
# connection + message codec
# --------------------------------------------------------------------------


class LabConnection(object):
    """One AF_UNIX socket handle with length-prefixed IO.

    Lock-step usage only (see module docstring): never post a recv on a
    socket while another thread sends on it - Windows AF_UNIX deadlocks.
    blocking=False (client mode) switches the socket to non-blocking and
    implements timeouts via deadline polling.
    """

    def __init__(self, sock, path="", blocking=True):
        self._dll = _load_ws2()
        self._s = sock
        self._path = path
        self._closed = False
        self._send_lock = threading.Lock()
        if not blocking:
            one = ctypes.c_ulong(1)
            rc = self._dll.ioctlsocket(sock, _FIONBIO, ctypes.byref(one))
            if rc != 0:
                raise LabError("ioctlsocket(FIONBIO) failed err=%d"
                               % self._dll.WSAGetLastError())

    def send_all(self, data, timeout=None):
        deadline = (time.time() + timeout) if timeout else None
        view = memoryview(data)
        while view:
            buf = (ctypes.c_char * len(view)).from_buffer_copy(view)
            n = self._dll.send(self._s, buf, len(view), 0)
            if n > 0:
                view = view[n:]
                continue
            err = self._dll.WSAGetLastError()
            if err == _WSAEWOULDBLOCK and deadline is not None:
                if time.time() >= deadline:
                    raise LabTimeout("send timed out (%s)" % self._path)
                time.sleep(0.001)
                continue
            raise LabConnectError("send failed err=%d (%s)" % (err, self._path))

    def recv_exact(self, size, timeout=None):
        deadline = (time.time() + timeout) if timeout else None
        out = bytearray()
        while len(out) < size:
            buf = ctypes.create_string_buffer(size - len(out))
            n = self._dll.recv(self._s, buf, size - len(out), 0)
            if n > 0:
                out += buf.raw[:n]
                continue
            if n == 0:
                raise LabConnectError("connection closed (%s)" % self._path)
            err = self._dll.WSAGetLastError()
            if err == _WSAEWOULDBLOCK and deadline is not None:
                if time.time() >= deadline:
                    raise LabTimeout("recv timed out (%s)" % self._path)
                time.sleep(0.001)
                continue
            raise LabConnectError("recv failed err=%d (%s)" % (err, self._path))
        return bytes(out)

    def close(self):
        if self._closed:
            return
        self._closed = True
        # shutdown first so a peer blocked in recv wakes up, then drop the handle
        try:
            self._dll.shutdown(self._s, _SD_BOTH)
        except Exception:
            pass
        try:
            self._dll.closesocket(self._s)
        except Exception:
            pass


def send_message(conn, obj, timeout=None):
    payload = WIRE_MAGIC + json.dumps(obj, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    conn.send_all(_FRAME_HEAD.pack(len(payload)) + payload, timeout)


def recv_message(conn, timeout=None):
    head = conn.recv_exact(_FRAME_HEAD.size, timeout)
    (length,) = _FRAME_HEAD.unpack(head)
    if length > _MAX_FRAME:
        raise LabError("frame too large: %d" % length)
    payload = conn.recv_exact(length, timeout)
    if not payload.startswith(WIRE_MAGIC):
        raise LabError("bad frame magic (protocol mismatch?)")
    return json.loads(payload[len(WIRE_MAGIC):].decode("utf-8"))


# --------------------------------------------------------------------------
# server
# --------------------------------------------------------------------------


class LabServer(object):
    """Bind one AF_UNIX socket file and serve requests on daemon threads.

    handler(action, args, kwargs) -> json-serializable result; raise any
    exception to return an error response to the caller.

    Server-side connections are blocking and have no recv timeout: a
    vanished client leaves its daemon worker blocked in recv until
    process exit (lab-acceptable).
    """

    def __init__(self, sock_path=None, handler=None):
        self.path = resolve_sock_path(sock_path)
        self.handler = handler
        self.running = False
        self._listener = None
        self._thread = None
        self._conns = set()
        self._conn_lock = threading.Lock()

    def start(self):
        if self.running:
            return
        dll = _load_ws2()
        if os.path.exists(self.path):
            try:
                os.remove(self.path)  # stale file left by a dead server
            except OSError:
                pass
        sock = _af_socket()
        addr = _sock_addr(self.path)
        if dll.bind(sock, ctypes.byref(addr), ctypes.sizeof(addr)) != 0:
            err = dll.WSAGetLastError()
            dll.closesocket(sock)
            raise LabError("bind failed err=%d on %s" % (err, self.path))
        if dll.listen(sock, 8) != 0:
            err = dll.WSAGetLastError()
            dll.closesocket(sock)
            raise LabError("listen failed err=%d on %s" % (err, self.path))
        self._listener = sock
        self.running = True
        self._thread = threading.Thread(target=self._serve_loop)
        self._thread.daemon = True
        self._thread.start()

    def _serve_loop(self):
        dll = _load_ws2()
        while self.running:
            conn_sock = dll.accept(self._listener, None, None)
            if conn_sock == _INVALID_SOCKET:
                if not self.running:
                    break
                time.sleep(0.05)
                continue
            if not self.running:
                dll.closesocket(conn_sock)
                break
            conn = LabConnection(conn_sock, self.path)
            with self._conn_lock:
                self._conns.add(conn)
            worker = threading.Thread(target=self._conn_loop, args=(conn,))
            worker.daemon = True
            worker.start()

    def _conn_loop(self, conn):
        try:
            while self.running:
                msg = recv_message(conn)
                reply = self._handle(msg)
                send_message(conn, reply)
        except Exception:
            pass
        finally:
            with self._conn_lock:
                self._conns.discard(conn)
            conn.close()

    def _handle(self, msg):
        msg_id = msg.get("id")
        if msg.get("t") != "req":
            return {"t": "res", "id": msg_id, "ok": False, "e": "bad message type"}
        action = msg.get("a")
        args = msg.get("p") or []
        kwargs = msg.get("k") or {}
        if not isinstance(args, list) or not isinstance(kwargs, dict):
            return {"t": "res", "id": msg_id, "ok": False, "e": "args must be list/dict"}
        if action == _PING_ACTION:
            return {"t": "res", "id": msg_id, "ok": True,
                    "r": {"pong": True, "server_time": round(time.time(), 6)}}
        if self.handler is None:
            return {"t": "res", "id": msg_id, "ok": False, "e": "no handler configured"}
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

    def _wake_accept(self):
        # a throwaway connect unblocks an accept() that is currently parked
        # inside winsock, so the serve loop can observe self.running == False
        try:
            dll = _load_ws2()
            wake = _af_socket()
            addr = _sock_addr(self.path)
            dll.connect(wake, ctypes.byref(addr), ctypes.sizeof(addr))
            dll.closesocket(wake)
        except Exception:
            pass

    def close(self):
        self.running = False
        listener = self._listener
        self._listener = None
        if listener is not None:
            dll = _load_ws2()
            # the wake connect may itself block in rare states, so run it on
            # a daemon thread and never let it stall close()
            wake = threading.Thread(target=self._wake_accept)
            wake.daemon = True
            wake.start()
            wake.join(0.2)
            try:
                dll.shutdown(listener, _SD_BOTH)
            except Exception:
                pass
            try:
                dll.closesocket(listener)
            except Exception:
                pass
        with self._conn_lock:
            conns = list(self._conns)
            self._conns.clear()
        for conn in conns:
            conn.close()
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
        except OSError:
            pass


# --------------------------------------------------------------------------
# client
# --------------------------------------------------------------------------


class LabClient(object):
    """AF_UNIX lab client (lock-step, one in-flight request at a time).

    client.get_etf_iopv("510050.SH") forwards to the QMT-side handler and
    reads like a local call (attribute proxy -> request(action, args, kwargs)).
    """

    def __init__(self, sock_path=None, timeout=5.0):
        self.path = resolve_sock_path(sock_path)
        self.default_timeout = timeout
        self._conn = None
        self._state_lock = threading.Lock()
        self._req_lock = threading.Lock()

    @property
    def connected(self):
        return self._conn is not None

    def connect(self):
        self._ensure_connected()

    def _ensure_connected(self):
        with self._state_lock:
            if self._conn is None:
                self._conn = self._connect()

    def _connect(self):
        # caller holds _state_lock
        dll = _load_ws2()
        sock = _af_socket()
        addr = _sock_addr(self.path)
        rc = dll.connect(sock, ctypes.byref(addr), ctypes.sizeof(addr))
        if rc != 0:
            err = dll.WSAGetLastError()
            dll.closesocket(sock)
            raise LabConnectError(
                "cannot connect %s err=%d (is the QMT-side server running?)"
                % (self.path, err))
        # non-blocking + deadline polling gives the request timeout that
        # afunix.sys's ignored SO_RCVTIMEO cannot (see module docstring)
        return LabConnection(sock, self.path, blocking=False)

    def _drop(self, conn):
        with self._state_lock:
            if self._conn is conn:
                self._conn = None
        conn.close()

    def request(self, action, args=None, kwargs=None, timeout=None):
        args = list(args) if args else []
        kwargs = dict(kwargs) if kwargs else {}
        effective = timeout if (timeout is not None and timeout > 0) else self.default_timeout
        rid = "r%d_%s" % (int(time.time() * 1000) % 1000000000, uuid.uuid4().hex[:8])
        msg = {"t": "req", "id": rid, "a": action, "p": args, "k": kwargs}
        self._ensure_connected()
        with self._req_lock:
            with self._state_lock:
                conn = self._conn
            if conn is None:
                raise LabConnectError("connection lost before send (%s)" % self.path)
            # lock-step: send the request, then (and only then) post the recv;
            # on any failure the connection is dropped because a late reply
            # would desync the frame stream of the next request
            try:
                send_message(conn, msg, timeout=effective)
                reply = recv_message(conn, timeout=effective)
            except Exception:
                self._drop(conn)
                raise
        if reply.get("id") != rid:
            self._drop(conn)
            raise LabError("reply id mismatch (protocol desync)")
        if not reply.get("ok"):
            raise LabError("remote '%s' failed: %s" % (action, reply.get("e")))
        return reply.get("r")

    def ping(self, timeout=None):
        return self.request(_PING_ACTION, timeout=timeout)

    def close(self):
        with self._state_lock:
            conn = self._conn
            self._conn = None
        if conn is not None:
            conn.close()

    def __getattr__(self, name):
        # attribute proxy: any non-underscore method call is forwarded as an
        # action with the same name and positional/keyword arguments
        if name.startswith("_"):
            raise AttributeError(name)

        def _proxy(*args, **kwargs):
            return self.request(name, args, kwargs)

        _proxy.__name__ = name
        return _proxy

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
# END AFUNIX_LITE_TRANSPORT

# --------------------------------------------------------------------------
# QMT lab glue
# --------------------------------------------------------------------------

LAB_ENTRY_VERSION = "iopv_lab_20261017"

# One name per forwarded QMT builtin; add entries here to expose more.
LAB_ALLOWED_ACTIONS = ("get_etf_iopv", "passorder")

# passorder has QMT-thread affinity: the socket worker must not call it
# directly. Requests are queued and executed by handlebar() on the QMT
# strategy thread (same pattern as the cfquant bridge's main_thread_queue).
_lab_main_queue = None
_lab_passorder_wait = 10.0

_lab_server = None
_lab_context = None


def _lab_log(message):
    print("[iopv_lab %s] %s" % (time.strftime("%H:%M:%S"), message))


def _lab_get_callable(*names):
    # resolve a QMT builtin the same way the lite bridge does:
    # module globals -> ContextInfo -> ContextInfo.context -> builtins
    owners = [globals()]
    if _lab_context is not None:
        owners.append(_lab_context)
        inner = getattr(_lab_context, "context", None)
        if inner is not None and inner is not _lab_context:
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


def _lab_pump():
    # drain the main-thread queue (called from handlebar on the QMT thread)
    global _lab_main_queue
    if _lab_main_queue is None:
        return
    while True:
        try:
            wait, func, args, kwargs = _lab_main_queue.get_nowait()
        except Exception:
            break
        try:
            result = _lab_call_passorder(func, args, kwargs)
        except Exception as exc:
            result = {"__lab_error__": "%s: %s" % (type(exc).__name__, exc)}
        wait.put(result)


# passorder positional layout (order matches the QMT builtin signature,
# trailing ContextInfo is appended separately as a variant). The 10th
# argument is the caller-chosen order id (cfquant uses the same slot for
# its sync order-id resolution); there is no validType/validDate here.
_PASSORDER_ARG_KEYS = ("op_type", "order_type", "account", "code", "price_type",
                       "price", "volume", "strategy_name", "quick_trade",
                       "user_order_id")
_PASSORDER_ARG_DEFAULTS = (23, 1101, "", "", 44, 0, 100, "", 2, "")


def _lab_default_order_id():
    return "iopv_lab_%s" % int(time.time() * 1000)


def _lab_call_passorder(func, args, kwargs):
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
        ordered[-1] = _lab_default_order_id()
    ordered = tuple(ordered)
    variants = [(ordered + (_lab_context,), {}), (ordered, {})]
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


def _lab_passorder(func, args, kwargs):
    global _lab_main_queue
    if _lab_main_queue is None:
        raise RuntimeError("passorder pump not initialized")
    wait = queue.Queue(1)
    _lab_main_queue.put((wait, func, args, kwargs))
    try:
        result = wait.get(timeout=_lab_passorder_wait)
    except queue.Empty:
        raise LabTimeout("passorder not executed on QMT thread within %.0fs"
                         % _lab_passorder_wait)
    if isinstance(result, dict) and result.get("__lab_error__"):
        raise RuntimeError(result["__lab_error__"])
    return result


_lab_schedule_key = None


def _lab_on_timer(*args, **kwargs):
    # QMT strategy-thread tick (registered via ContextInfo.schedule_run);
    # this is the fast path for the passorder pump - handlebar on a daily
    # chart only fires every ~11 s, far too slow for interactive orders
    _lab_pump()


def _lab_schedule_timer():
    global _lab_schedule_key
    if _lab_context is None or _lab_schedule_key is not None:
        return
    try:
        first_time = dt.datetime.now() + dt.timedelta(seconds=1)
        _lab_schedule_key = _lab_context.schedule_run(
            _lab_on_timer,
            first_time,
            repeat_times=-1,
            interval=dt.timedelta(milliseconds=10),
            name="iopv_lab_passorder_pump",
        )
        _lab_log("passorder timer scheduled key=%s interval_ms=10" % _lab_schedule_key)
    except Exception as exc:
        _lab_log("passorder timer schedule failed: %s" % exc)


def _lab_handler(action, args, kwargs):
    # runs on a server worker thread. get_etf_iopv is a plain data query and
    # is safe off the QMT thread; passorder is routed back to the QMT
    # strategy thread (schedule_run tick / handlebar) so the terminal's
    # thread affinity check passes.
    if action not in LAB_ALLOWED_ACTIONS:
        raise ValueError("action not allowed in lab: %s" % action)
    func = _lab_get_callable(action)
    if func is None:
        raise RuntimeError("QMT function '%s' not found (did init() finish?)" % action)
    if action == "passorder":
        return _lab_passorder(func, args, kwargs)
    return func(*args, **kwargs)


def init(ContextInfo):
    global _lab_server, _lab_context, _lab_main_queue

    _lab_context = ContextInfo
    if _lab_main_queue is None:
        _lab_main_queue = queue.Queue(maxsize=10000)
    _lab_schedule_timer()
    if _lab_server is None:
        try:
            _lab_server = LabServer(handler=_lab_handler)
            _lab_server.start()
            _lab_log("afunix lab server ready version:%s sock:%s"
                     % (LAB_ENTRY_VERSION, _lab_server.path))
        except Exception:
            _lab_server = None
            _lab_log("afunix lab server failed to start:")
            traceback.print_exc()
    else:
        _lab_log("afunix lab server already running, context refreshed")


def after_init(ContextInfo):
    pass


def handlebar(ContextInfo):
    # QMT strategy thread: drain passorder requests queued by socket workers
    _lab_pump()


def stop(ContextInfo):
    global _lab_server

    if _lab_server is not None:
        _lab_server.close()
        _lab_server = None
        _lab_log("afunix lab server stopped")
