"""Probe: does a parked ReadFile block a concurrent WriteFile on the same
named-pipe handle? Compares synchronous vs FILE_FLAG_OVERLAPPED handles."""

import ctypes
import threading
import time
from ctypes import wintypes

k32 = ctypes.WinDLL("kernel32", use_last_error=True)
for fn, at, rt in [
    ("CreateNamedPipeW", [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD,
                          wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID], wintypes.HANDLE),
    ("CreateFileW", [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                     wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE], wintypes.HANDLE),
    ("ConnectNamedPipe", [wintypes.HANDLE, wintypes.LPVOID], wintypes.BOOL),
    ("ReadFile", [wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD,
                  ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID], wintypes.BOOL),
    ("WriteFile", [wintypes.HANDLE, wintypes.LPCVOID, wintypes.DWORD,
                   ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID], wintypes.BOOL),
    ("CreateEventW", [wintypes.LPVOID, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
    ("GetOverlappedResult", [wintypes.HANDLE, wintypes.LPVOID,
                             ctypes.POINTER(wintypes.DWORD), wintypes.BOOL], wintypes.BOOL),
    ("CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
]:
    f = getattr(k32, fn)
    f.argtypes = at
    f.restype = rt

INVALID = ctypes.c_void_p(-1).value


class OVERLAPPED(ctypes.Structure):
    _fields_ = [("Internal", wintypes.LPVOID), ("InternalHigh", wintypes.LPVOID),
                ("Offset", wintypes.DWORD), ("OffsetHigh", wintypes.DWORD),
                ("hEvent", wintypes.HANDLE)]


FILE_FLAG_OVERLAPPED = 0x40000000
ERROR_IO_PENDING = 997
PIPE_ACCESS_DUPLEX = 3
GENERIC_RW = 0xC0000000


def is_bad(h):
    return h is None or h == 0 or h == INVALID


def ov_connect(h):
    """ConnectNamedPipe on an overlapped server handle needs an OVERLAPPED."""
    ov = OVERLAPPED()
    ov.hEvent = k32.CreateEventW(None, True, False, None)
    try:
        ok = k32.ConnectNamedPipe(h, ctypes.byref(ov))
        err = ctypes.get_last_error()
        if not ok and err != ERROR_IO_PENDING and err != 535:
            raise OSError(err, "ovl ConnectNamedPipe start")
        if err == 535:  # ERROR_PIPE_CONNECTED: client already attached
            return True
        m = wintypes.DWORD(0)
        ok = k32.GetOverlappedResult(h, ctypes.byref(ov), ctypes.byref(m), True)
        if not ok and ctypes.get_last_error() != 535:
            raise OSError(ctypes.get_last_error(), "ovl ConnectNamedPipe result")
        return True
    finally:
        k32.CloseHandle(ov.hEvent)


def ov_read(h, size):
    ov = OVERLAPPED()
    ov.hEvent = k32.CreateEventW(None, True, False, None)
    try:
        buf = ctypes.create_string_buffer(size)
        n = wintypes.DWORD(0)
        ok = k32.ReadFile(h, buf, size, ctypes.byref(n), ctypes.byref(ov))
        if not ok and ctypes.get_last_error() != ERROR_IO_PENDING:
            raise OSError(ctypes.get_last_error(), "ovl ReadFile start")
        m = wintypes.DWORD(0)
        ok = k32.GetOverlappedResult(h, ctypes.byref(ov), ctypes.byref(m), True)
        if not ok:
            raise OSError(ctypes.get_last_error(), "ovl ReadFile result")
        return buf.raw[:m.value]
    finally:
        k32.CloseHandle(ov.hEvent)


def ov_write(h, data):
    ov = OVERLAPPED()
    ov.hEvent = k32.CreateEventW(None, True, False, None)
    try:
        buf = ctypes.create_string_buffer(data)
        n = wintypes.DWORD(0)
        ok = k32.WriteFile(h, buf, len(data), ctypes.byref(n), ctypes.byref(ov))
        if not ok and ctypes.get_last_error() != ERROR_IO_PENDING:
            raise OSError(ctypes.get_last_error(), "ovl WriteFile start")
        m = wintypes.DWORD(0)
        ok = k32.GetOverlappedResult(h, ctypes.byref(ov), ctypes.byref(m), True)
        if not ok:
            raise OSError(ctypes.get_last_error(), "ovl WriteFile result")
        return m.value
    finally:
        k32.CloseHandle(ov.hEvent)


def probe(overlapped):
    name = r"\\.\pipe\npsh_probe3_" + ("ovl" if overlapped else "sync")
    sflags = PIPE_ACCESS_DUPLEX | (FILE_FLAG_OVERLAPPED if overlapped else 0)
    hs = k32.CreateNamedPipeW(name, sflags, 0, 1, 65536, 65536, 0, None)
    if is_bad(hs):
        print("server create FAILED err=%d" % ctypes.get_last_error())
        return
    result = {}

    def serve():
        try:
            if overlapped:
                ov_connect(hs)
            else:
                k32.ConnectNamedPipe(hs, None)
            if overlapped:
                result["srv_read"] = repr(ov_read(hs, 64))
            else:
                buf = ctypes.create_string_buffer(64)
                n = wintypes.DWORD(0)
                ok = k32.ReadFile(hs, buf, 64, ctypes.byref(n), None)
                if ok:
                    result["srv_read"] = "ok data=%r" % buf.raw[:n.value]
                else:
                    result["srv_read"] = "FAILED err=%d" % ctypes.get_last_error()
        except Exception as e:
            result["srv_read"] = "EXC %r" % e

    t = threading.Thread(target=serve)
    t.daemon = True
    t.start()
    # the instance is listening from CreateNamedPipeW; connect right away

    cflags = FILE_FLAG_OVERLAPPED if overlapped else 0
    hc = k32.CreateFileW(name, GENERIC_RW, 0, None, 3, cflags, None)
    if is_bad(hc):
        print("client connect FAILED err=%d" % ctypes.get_last_error())
        k32.CloseHandle(hs)
        return

    def reader():
        try:
            if overlapped:
                result["cli_reader"] = repr(ov_read(hc, 64))
            else:
                buf = ctypes.create_string_buffer(64)
                n = wintypes.DWORD(0)
                ok = k32.ReadFile(hc, buf, 64, ctypes.byref(n), None)
                if ok:
                    result["cli_reader"] = "ok data=%r" % buf.raw[:n.value]
                else:
                    result["cli_reader"] = "FAILED err=%d" % ctypes.get_last_error()
        except Exception as e:
            result["cli_reader"] = "EXC %r" % e

    rt = threading.Thread(target=reader)
    rt.daemon = True
    rt.start()
    time.sleep(0.4)  # reader's ReadFile definitely parked now

    data = b"hello-duplex"
    t0 = time.perf_counter()
    try:
        if overlapped:
            nb = ov_write(hc, data)
            outcome = "wrote %d bytes" % nb
        else:
            n = wintypes.DWORD(0)
            done = {}

            def do_write():
                buf = ctypes.create_string_buffer(data)
                ok = k32.WriteFile(hc, buf, len(data), ctypes.byref(n), None)
                done["ok"] = bool(ok)
                done["err"] = ctypes.get_last_error()

            wt = threading.Thread(target=do_write)
            wt.daemon = True
            wt.start()
            wt.join(2.0)
            if wt.is_alive():
                outcome = "BLOCKED-2s (write waits for parked read)"
            else:
                outcome = "wrote ok=%s err=%d n=%d" % (done.get("ok"), done.get("err", 0), n.value)
    except Exception as e:
        outcome = "EXC %r" % e
    ms = (time.perf_counter() - t0) * 1000
    time.sleep(0.6)
    print("overlapped=%-5s client_write: %-38s %7.1f ms | srv_read=%s | cli_reader=%s"
          % (overlapped, outcome, ms, result.get("srv_read", "<none>"), result.get("cli_reader", "<none>")))
    k32.CloseHandle(hc)
    k32.CloseHandle(hs)


if __name__ == "__main__":
    probe(False)
    probe(True)
