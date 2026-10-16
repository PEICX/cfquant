# -*- coding: utf-8 -*-
"""Optional orjson-accelerated parsing for the external SDK / Web side only.

QMT-side bridges must stay pure stdlib, so no module that runs inside QMT
(including the standalone LITE entries) may import this module. The import
guard falls back to the standard library transparently; set
CFQUANT_NO_ORJSON=1 to force the fallback for debugging.
"""
import json
import os

try:
    if str(os.environ.get("CFQUANT_NO_ORJSON") or "").strip().lower() in ("1", "true", "yes", "on"):
        raise ImportError("orjson disabled by CFQUANT_NO_ORJSON")
    import orjson
except ImportError:
    orjson = None

from .protocol import MESSAGE_PREFIX, has_typed_payload


_PREFIX_BYTES = MESSAGE_PREFIX.encode("utf-8")


def _loads_json(payload):
    """json.loads semantics (bytes or str input); orjson when available."""
    if orjson is not None:
        try:
            return orjson.loads(payload)
        except Exception:
            pass
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    return json.loads(payload)


def loads_message_typed(raw):
    """protocol.loads_message plus the ``_cf_typed`` marker annotation.

    Mirrors loads_message's tolerant behavior: returns None for anything that
    is not a cfquant protocol message. The ``_cf_typed`` flag records whether
    the raw text mentions a ``__cf_type__`` envelope so decode_value's deep
    restore can be skipped for plain-scalar payloads.
    """
    typed = has_typed_payload(raw)
    if isinstance(raw, (bytes, bytearray, memoryview)):
        body = bytes(raw)
        bar = body.find(b"|")
        if bar != -1:
            body = body[bar + 1:]
        if not body.startswith(_PREFIX_BYTES):
            return None
        payload = body[len(_PREFIX_BYTES):]
    elif isinstance(raw, str):
        text = raw
        if "|" in text:
            _, text = text.split("|", 1)
        if not text.startswith(MESSAGE_PREFIX):
            return None
        payload = text[len(MESSAGE_PREFIX):]
    else:
        return None
    try:
        data = _loads_json(payload)
    except Exception:
        return None
    if not isinstance(data, dict) or data.get("protocol") != "cfquant":
        return None
    data["_cf_typed"] = typed
    return data
