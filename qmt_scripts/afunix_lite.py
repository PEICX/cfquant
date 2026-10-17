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
from concurrent.futures import ThreadPoolExecutor

__all__ = [
    "LabError",
    "LabConnectError",
    "LabTimeout",
    "DEFAULT_SOCK_PATH",
    "SOCK_PATH_ENV",
    "resolve_sock_path",
    "LabServer",
    "LabClient",
    "LabDuplexClient",
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
        # closesocket alone (no shutdown): calling shutdown(SD_BOTH) on a
        # socket that has a recv parked in another thread BLOCKS on Windows
        # AF_UNIX (verified with the duplex rx reader). closesocket releases
        # the parked recv with an error, which is all the peer needs.
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

    def __init__(self, sock_path=None, handler=None, worker_threads=0):
        self.path = resolve_sock_path(sock_path)
        self.handler = handler
        self.running = False
        self._listener = None
        self._thread = None
        self._conns = set()
        self._conn_lock = threading.Lock()
        self._clients = {}  # client_id -> {"rx": conn, "tx": conn}
        # worker_threads > 0 handles requests on a pool so in-flight duplex
        # requests run in parallel; 0 keeps the classic serial per-connection
        # processing (backwards compatible with the deployed QMT version)
        self._executor = ThreadPoolExecutor(max_workers=worker_threads) if worker_threads > 0 else None

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

    def _reply_target(self, msg, conn):
        # single-connection clients (no hello) get the reply on the same
        # socket; duplex clients get it on their rx connection
        cid = msg.get("cid") or msg.get("client_id")
        if cid:
            with self._conn_lock:
                entry = self._clients.get(cid)
                if entry and entry.get("rx") is not None:
                    return entry["rx"]
        return conn

    def _process_request(self, msg, conn):
        try:
            reply = self._handle(msg)
        except Exception as exc:
            reply = {"t": "res", "id": msg.get("id"), "ok": False,
                     "e": "%s: %s" % (type(exc).__name__, exc)}
        try:
            send_message(self._reply_target(msg, conn), reply)
        except Exception:
            pass

    def _conn_loop(self, conn):
        try:
            while self.running:
                msg = recv_message(conn)
                if msg.get("t") == "hello":
                    # duplex (rx/tx) connection handshake. a request arriving on
                    # the tx connection is answered on the rx connection, which
                    # keeps recv/send on separate sockets so Windows AF_UNIX
                    # never has a pending recv blocking a send.
                    role = msg.get("role", "tx")
                    cid = msg.get("client_id", "")
                    with self._conn_lock:
                        entry = self._clients.setdefault(cid, {})
                        entry[role] = conn
                        both = entry.get("tx") is not None and entry.get("rx") is not None
                        rx_conn = entry.get("rx")
                    if both and rx_conn is not None:
                        # only ack once both halves are registered: the client
                        # waits for this before sending requests, so request
                        # routing to the rx connection can never race the
                        # handshake
                        send_message(rx_conn, {"t": "hello_ack", "client_id": cid})
                    if role == "rx":
                        # the rx socket is a passive response channel: it must
                        # NEVER post a recv here, or that pending recv blocks
                        # the sends that route replies onto this same socket
                        # (the Windows AF_UNIX full-duplex trap again, server
                        # side). Park the worker until shutdown instead.
                        while self.running:
                            time.sleep(0.1)
                        break
                    continue
                if self._executor is not None:
                    self._executor.submit(self._process_request, msg, conn)
                else:
                    self._process_request(msg, conn)
        except Exception:
            pass
        finally:
            with self._conn_lock:
                self._conns.discard(conn)
                for cid, entry in list(self._clients.items()):
                    for role, candidate in list(entry.items()):
                        if candidate is conn:
                            entry.pop(role, None)
                    if not entry:
                        self._clients.pop(cid, None)
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
            self._clients.clear()
        for conn in conns:
            conn.close()
        if self._executor is not None:
            self._executor.shutdown(wait=False)
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


class LabDuplexClient(object):
    """AF_UNIX client with separate rx/tx connections (duplex mode).

    Two sockets to the same socket file: tx carries requests only, rx carries
    responses only. Because no single socket ever has a recv pending while a
    send runs on it, the Windows AF_UNIX full-duplex deadlock is avoided and
    the client can hold many in-flight requests at once (pipelining) instead
    of the lock-step one-at-a-time serialization of LabClient. Responses are
    correlated by request id on the rx reader thread.

    The QMT-side LabServer answers a request on the caller's rx connection,
    so one duplex client is roughly equivalent to N parallel single clients,
    without the socket-per-thread overhead. Server-initiated events arrive
    on rx too and are dispatched to on_event(action, data).
    """

    def __init__(self, sock_path=None, timeout=5.0, client_id=""):
        self.path = resolve_sock_path(sock_path)
        self.default_timeout = timeout
        self.client_id = client_id or ("lab_%s" % uuid.uuid4().hex[:8])
        self._tx = None
        self._rx = None
        self._state_lock = threading.Lock()
        self._pending = {}
        self._ready = threading.Event()
        self.on_event = None

    @property
    def connected(self):
        return self._tx is not None and self._rx is not None

    def connect(self):
        self._ensure_connected()

    def _ensure_connected(self):
        with self._state_lock:
            if self._tx is None:
                self._connect_pair()

    def _connect_pair(self):
        # caller holds _state_lock; both sockets non-blocking. A blocking recv
        # parked on rx would make closesocket() (and shutdown) block on
        # Windows AF_UNIX, so the reader polls with a soft timeout instead.
        dll = _load_ws2()
        tx_sock = self._dial(dll)
        rx_sock = self._dial(dll)
        tx = LabConnection(tx_sock, self.path, blocking=False)
        rx = LabConnection(rx_sock, self.path, blocking=False)
        self._ready.clear()
        send_message(tx, {"t": "hello", "role": "tx", "client_id": self.client_id})
        send_message(rx, {"t": "hello", "role": "rx", "client_id": self.client_id})
        self._tx = tx
        self._rx = rx
        reader = threading.Thread(target=self._reader_loop, args=(rx,))
        reader.daemon = True
        reader.start()
        # wait for the server to confirm both handshake halves before any
        # request is sent, so request routing can never race the registration
        if not self._ready.wait(timeout=self.default_timeout):
            raise LabConnectError("duplex handshake not acknowledged (%s)" % self.path)

    def _dial(self, dll):
        sock = _af_socket()
        addr = _sock_addr(self.path)
        rc = dll.connect(sock, ctypes.byref(addr), ctypes.sizeof(addr))
        if rc != 0:
            err = dll.WSAGetLastError()
            dll.closesocket(sock)
            raise LabConnectError(
                "cannot connect %s err=%d (is the QMT-side server running?)"
                % (self.path, err))
        return sock

    def _reader_loop(self, rx):
        try:
            while True:
                try:
                    msg = recv_message(rx, timeout=1.0)
                except LabTimeout:
                    # non-blocking poll with a soft deadline; keep waiting
                    continue
                except Exception:
                    break
                if msg.get("t") == "hello_ack":
                    self._ready.set()
                elif msg.get("t") == "res":
                    with self._state_lock:
                        wait = self._pending.pop(msg.get("id"), None)
                    if wait is not None:
                        wait.put(msg)
                else:
                    # server-initiated event / push
                    callback = self.on_event
                    if callback is not None:
                        try:
                            callback(msg.get("e") or msg.get("a"), msg.get("d"))
                        except Exception:
                            pass
        except Exception:
            pass
        finally:
            with self._state_lock:
                self._tx = None
                self._rx = None
                pending = self._pending
                self._pending = {}
            for wait in pending.values():
                wait.put({"t": "res", "id": None, "ok": False, "e": "connection closed"})

    def request(self, action, args=None, kwargs=None, timeout=None):
        args = list(args) if args else []
        kwargs = dict(kwargs) if kwargs else {}
        effective = timeout if (timeout is not None and timeout > 0) else self.default_timeout
        rid = "r%d_%s" % (int(time.time() * 1000) % 1000000000, uuid.uuid4().hex[:8])
        msg = {"t": "req", "id": rid, "a": action, "p": args, "k": kwargs,
               "cid": self.client_id}
        self._ensure_connected()
        with self._state_lock:
            tx = self._tx
            # hold the wait queue in a local: a fast server may reply before
            # request() re-reads _pending, and the reader already popped it
            wait = queue.Queue(1)
            self._pending[rid] = wait
        if tx is None:
            with self._state_lock:
                self._pending.pop(rid, None)
            raise LabConnectError("connection lost before send (%s)" % self.path)
        try:
            send_message(tx, msg, timeout=effective)
        except Exception:
            with self._state_lock:
                self._pending.pop(rid, None)
            raise
        try:
            reply = wait.get(timeout=effective)
        except queue.Empty:
            with self._state_lock:
                self._pending.pop(rid, None)
            raise LabTimeout("request '%s' timed out after %.3fs (%s)"
                             % (action, effective, self.path))
        if not reply.get("ok"):
            raise LabError("remote '%s' failed: %s" % (action, reply.get("e")))
        return reply.get("r")

    def ping(self, timeout=None):
        return self.request(_PING_ACTION, timeout=timeout)

    def close(self):
        with self._state_lock:
            tx = self._tx
            rx = self._rx
            self._tx = None
            self._rx = None
            pending = self._pending
            self._pending = {}
        for conn in (tx, rx):
            if conn is not None:
                conn.close()
        for wait in pending.values():
            wait.put({"t": "res", "id": None, "ok": False, "e": "client closed"})

    def __getattr__(self, name):
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
