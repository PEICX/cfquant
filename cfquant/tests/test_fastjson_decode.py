"""Decode fastpath and orjson fallback; no sockets or QMT involved."""
import json

import pytest

import cfquant._fastjson as fastjson
from cfquant._fastjson import loads_message_typed
from cfquant.callback_dispatcher import dispatch_callbacks
from cfquant.protocol import (
    decode_value,
    dumps_message,
    has_typed_payload,
    pack_event,
    pack_request,
    pack_response,
)


def test_has_typed_payload_detects_marker_in_text_and_bytes():
    plain = pack_event("quote:1", data={"x": 1}, client_id="c")
    typed = dumps_message({"type": "event", "data": {"__cf_type__": "bytes", "data": "aGk="}})
    assert has_typed_payload(plain) is False
    assert has_typed_payload(typed) is True
    assert has_typed_payload(plain.encode("utf-8")) is False
    assert has_typed_payload(typed.encode("utf-8")) is True
    assert has_typed_payload(None) is False
    assert has_typed_payload(123) is False


def test_loads_message_typed_matches_loads_message():
    raw = "response|" + pack_response("req_1", ok=True, result={"pong": True})
    msg = loads_message_typed(raw)
    assert msg["type"] == "response"
    assert msg["result"] == {"pong": True}
    assert msg["_cf_typed"] is False
    assert loads_message_typed("not-a-message") is None
    assert loads_message_typed(None) is None


def test_loads_message_typed_flags_typed_payloads():
    raw = dumps_message({
        "type": "event",
        "data": {"blob": {"__cf_type__": "bytes", "data": "aGk="}},
    })
    msg = loads_message_typed(raw)
    assert msg["_cf_typed"] is True
    assert decode_value(msg["data"])["blob"] == b"hi"


def test_loads_message_typed_accepts_bytes_and_lttx_prefix():
    raw = ("response|" + pack_response("req_2", ok=True, result=[1, 2])).encode("utf-8")
    msg = loads_message_typed(raw)
    assert msg["result"] == [1, 2]
    no_prefix = pack_request("cfquant.ping", client_id="c").encode("utf-8")
    assert loads_message_typed(no_prefix)["type"] == "request"


def test_loads_message_typed_orjson_and_stdlib_paths_agree(monkeypatch):
    raw = "response|" + pack_response("req_3", ok=True, result={"a": [1.5, None, "x"]})
    with_orjson = loads_message_typed(raw)
    monkeypatch.setattr(fastjson, "orjson", None)
    without_orjson = loads_message_typed(raw)
    without_orjson.pop("_cf_typed")
    expected = dict(with_orjson)
    expected.pop("_cf_typed")
    assert without_orjson == expected


def test_loads_message_typed_falls_back_on_orjson_failure(monkeypatch):
    raw = pack_response("req_4", ok=True, result={"pong": True})

    class BrokenOrjson(object):
        @staticmethod
        def loads(payload):
            raise ValueError("orjson refused")

    monkeypatch.setattr(fastjson, "orjson", BrokenOrjson)
    msg = loads_message_typed(raw)
    assert msg["result"] == {"pong": True}


def test_dispatch_callbacks_skips_decode_for_plain_events():
    received = []
    dispatch_callbacks(
        {"custom.event": [received.append]},
        {"event": "custom.event", "data": {"x": 1}, "_cf_typed": False},
        lambda: True,
    )
    assert received == [{"x": 1}]


def test_dispatch_callbacks_still_decodes_typed_events():
    received = []
    dispatch_callbacks(
        {"custom.event": [received.append]},
        {
            "event": "custom.event",
            "data": {"blob": {"__cf_type__": "bytes", "data": "aGk="}},
            "_cf_typed": True,
        },
        lambda: True,
    )
    assert received == [{"blob": b"hi"}]


def test_dispatch_callbacks_defaults_to_decode_without_flag():
    received = []
    dispatch_callbacks(
        {"custom.event": [received.append]},
        {"event": "custom.event", "data": {"blob": {"__cf_type__": "bytes", "data": "aGk="}}},
        lambda: True,
    )
    assert received == [{"blob": b"hi"}]


def test_loads_message_typed_survives_nan_free_roundtrip():
    data = {"000001.SZ": {"lastPrice": 12.5, "askPrice": [12.4, 12.5], "flag": None}}
    raw = pack_event("quote:9", data=data, client_id="c", subscription_id=9)
    msg = loads_message_typed(raw)
    assert msg["_cf_typed"] is False
    assert msg["data"] == data
