"""Columnar whole-quote wire format: roundtrip, fallback, end-to-end push."""
import json

import pytest

from cfquant.normal_bridge import NormalQmtBridge
from cfquant.pipe_client import PipeRpcClient
from cfquant.pipe_transport import (
    FRAME_FLAG_COLUMNAR,
    FRAME_TYPE_PUBLISH_EVENT,
    PIPE_FRAME_MAGIC,
    PipeTxClient,
    decode_pipe_frame,
    encode_pipe_frame,
    loads_pipe_message,
)
from cfquant.protocol import loads_message, pack_request
from cfquant.quote_wire import decode_whole_quote, encode_whole_quote


class FakeQmt(object):
    def __init__(self):
        self.subscribed = []
        self.callbacks = {}

    def subscribe_whole_quote(self, codes, callback):
        native_id = 100 + len(self.subscribed)
        self.subscribed.append(codes)
        self.callbacks[native_id] = callback
        return native_id

    def unsubscribe_quote(self, native_id):
        pass


class RecordingTx(object):
    def __init__(self):
        self.events = []
        self.columnar = []

    def push(self, key, payload, channel):
        self.events.append((key, payload, channel))
        return {"code": 0}

    def close(self):
        pass


class ColumnarTx(RecordingTx):
    def push_columnar(self, event_name, data_bytes, client_id, subscription_id=None, channel=None):
        self.columnar.append((event_name, bytes(data_bytes), client_id, subscription_id))
        return True


def make_bridge(tx):
    bridge = NormalQmtBridge(FakeQmt(), show=False, schedule_timer=False)
    bridge._log = lambda message: None
    bridge.tx = tx
    bridge.running = True
    return bridge


def rpc(bridge, action, params=None, client="client-a"):
    message = {
        "type": "request",
        "id": "req_1",
        "action": action,
        "params": params or {},
        "client_id": client,
        "reply_channel": client,
    }
    return bridge._dispatch(action, message["params"], message)


def sample_batch():
    return {
        "000001.SZ": {
            "lastPrice": 12.5, "open": 12.0, "volume": 1000, "amount": 12500.0,
            "askPrice": [12.4, 12.5], "stockName": "PINGAN", "flag": True,
        },
        "600000.SH": {
            "lastPrice": 8.3, "open": 8.2, "volume": 2000, "amount": 16600.0,
            "askPrice": [8.25, 8.3], "stockName": "SPDB", "flag": False,
        },
        "830799.BJ": {
            "lastPrice": None, "open": None, "volume": 0, "amount": 0.0,
            "askPrice": None, "stockName": None, "flag": None,
        },
    }


def test_roundtrip_all_column_types():
    original = sample_batch()
    payload = encode_whole_quote(original)
    assert payload is not None
    assert payload[:4] == b"CFQ1"
    decoded = decode_whole_quote(payload)
    assert decoded == original


def test_roundtrip_is_smaller_than_json():
    original = sample_batch()
    payload = encode_whole_quote(original)
    json_payload = json.dumps(original, ensure_ascii=False, separators=(",", ":"))
    assert len(payload) < len(json_payload.encode("utf-8"))


def test_missing_fields_decode_as_none():
    original = {"A.SH": {"x": 1}, "B.SZ": {"y": 2}}
    payload = encode_whole_quote(original)
    decoded = decode_whole_quote(payload)
    assert decoded["A.SH"]["x"] == 1 and decoded["A.SH"]["y"] is None
    assert decoded["B.SZ"]["y"] == 2 and decoded["B.SZ"]["x"] is None


def test_unencodable_values_return_none():
    assert encode_whole_quote({}) is None
    assert encode_whole_quote({"A": []}) is None
    assert encode_whole_quote({"A": {"x": {"nested": 1}}}) is None
    assert encode_whole_quote({"A": {"x": "s"}, "B": {"x": 1}}) is None  # mixed column
    assert encode_whole_quote({123: {"x": 1}}) is None
    assert encode_whole_quote({"A": None}) is None


def test_decode_rejects_bad_payloads():
    assert decode_whole_quote(None) is None
    assert decode_whole_quote(b"CFQ2....") is None
    assert decode_whole_quote(b"CFQ1") is None
    assert decode_whole_quote(b"CFQ1\x00\x00\x00\x05\x00\x00") is None  # truncated


def test_bridge_pushes_columnar_when_enabled():
    tx = ColumnarTx()
    bridge = make_bridge(tx)
    result = rpc(bridge, "xtdata.subscribe_whole_quote",
                 {"code_list": ["SH"], "wire_format": "columnar"})
    bridge.context.callbacks[result["internal_subscribe_id"]](sample_batch())

    assert len(tx.columnar) == 1
    event_name, payload, client_id, subscription_id = tx.columnar[0]
    assert event_name == "quote:%s" % result["subscribe_id"]
    assert client_id == "client-a"
    assert subscription_id == result["subscribe_id"]
    assert decode_whole_quote(payload) == sample_batch()
    assert tx.events == []  # no JSON fallback


def test_bridge_falls_back_when_batch_not_encodable():
    tx = ColumnarTx()
    bridge = make_bridge(tx)
    result = rpc(bridge, "xtdata.subscribe_whole_quote",
                 {"code_list": ["SH"], "wire_format": "columnar"})
    bridge.context.callbacks[result["internal_subscribe_id"]](["not", "a", "dict"])

    assert tx.columnar == []
    assert len(tx.events) == 1
    assert loads_message(tx.events[0][1])["type"] == "event"


def test_bridge_env_var_enables_columnar(monkeypatch):
    monkeypatch.setenv("CFQUANT_WHOLE_QUOTE_WIRE", "columnar")
    tx = ColumnarTx()
    bridge = make_bridge(tx)
    result = rpc(bridge, "xtdata.subscribe_whole_quote", {"code_list": ["SH"]})
    bridge.context.callbacks[result["internal_subscribe_id"]](sample_batch())
    assert len(tx.columnar) == 1


def test_bridge_without_columnar_transport_uses_json():
    tx = RecordingTx()
    bridge = make_bridge(tx)
    result = rpc(bridge, "xtdata.subscribe_whole_quote",
                 {"code_list": ["SH"], "wire_format": "columnar"})
    bridge.context.callbacks[result["internal_subscribe_id"]](sample_batch())
    assert tx.columnar == [] if hasattr(tx, "columnar") else True
    assert len(tx.events) == 1
    assert loads_message(tx.events[0][1])["data"] == sample_batch()


def test_pipe_tx_push_columnar_requires_binary_ack():
    client = PipeTxClient(pipe_name="unit_pipe", heartbeat_interval=0)

    class Conn(object):
        def __init__(self):
            self.writes = []

        def write_frame(self, payload):
            self.writes.append(payload)

    conn = Conn()
    with client.conn_lock:
        client.tx_conn = conn
        client.connection_generation = 1
    assert client.push_columnar("quote:1", b"CFQ1xxxx", "client-a") is False

    client._hub_binary = True
    assert client.push_columnar("quote:1", b"CFQ1xxxx", "client-a", subscription_id=3) is True
    envelope = decode_pipe_frame(conn.writes[-1])
    assert envelope["key"] == "event"
    assert envelope["event_name"] == "quote:1"
    assert envelope["subscription_id"] == "3"
    assert envelope["flags"] & FRAME_FLAG_COLUMNAR
    assert envelope["payload"] == b"CFQ1xxxx"


def test_pipe_client_reconstructs_columnar_event():
    client = PipeRpcClient.__new__(PipeRpcClient)
    data = sample_batch()
    frame = decode_pipe_frame(encode_pipe_frame(
        FRAME_TYPE_PUBLISH_EVENT,
        encode_whole_quote(data),
        client_id="client-a",
        channel="client-a",
        event_name="quote:5",
        subscription_id="5",
        flags=FRAME_FLAG_COLUMNAR,
    ))
    msg = client._parse_columnar_event(frame, frame["payload"])
    assert msg["type"] == "event"
    assert msg["event"] == "quote:5"
    assert msg["subscription_id"] == 5
    assert msg["data"] == data
    assert msg["_cf_typed"] is False

    assert client._parse_columnar_event(frame, b"CFQ1garbage") is None


def test_hub_routes_columnar_event_to_binary_client():
    from cfquant.pipe_hub import CfquantPipeHub

    class FakeConn(object):
        def __init__(self, binary):
            self.binary_hotpath = binary
            self.writes = []

        def write_frame(self, payload):
            self.writes.append(payload)

    hub = CfquantPipeHub(pipe_name="unit_pipe", show=False)
    hub._write_status = lambda: None
    hub.running = True
    api_rx = FakeConn(True)
    qmt_tx = FakeConn(False)
    with hub.state_lock:
        hub.client_by_id["client-a"] = api_rx

    data = sample_batch()
    payload = encode_whole_quote(data)
    hub._handle_qmt_publish(qmt_tx, decode_pipe_frame(encode_pipe_frame(
        FRAME_TYPE_PUBLISH_EVENT, payload,
        client_id="client-a", channel="client-a",
        event_name="quote:5", subscription_id="5",
        flags=FRAME_FLAG_COLUMNAR,
    )))

    assert len(api_rx.writes) == 1
    delivered = decode_pipe_frame(api_rx.writes[0])
    assert delivered["type"] == "delivery"
    assert delivered["flags"] & FRAME_FLAG_COLUMNAR
    assert delivered["payload"] == payload


def test_hub_drops_columnar_for_legacy_client():
    from cfquant.pipe_hub import CfquantPipeHub

    class FakeConn(object):
        binary_hotpath = False

        def __init__(self):
            self.writes = []

        def write_frame(self, payload):
            self.writes.append(payload)

    hub = CfquantPipeHub(pipe_name="unit_pipe", show=False)
    hub._write_status = lambda: None
    hub.running = True
    api_rx = FakeConn()
    with hub.state_lock:
        hub.client_by_id["client-a"] = api_rx

    hub._handle_qmt_publish(FakeConn(), decode_pipe_frame(encode_pipe_frame(
        FRAME_TYPE_PUBLISH_EVENT, encode_whole_quote(sample_batch()),
        client_id="client-a", channel="client-a",
        event_name="quote:5", flags=FRAME_FLAG_COLUMNAR,
    )))
    assert api_rx.writes == []


def test_xtdata_subscribe_whole_quote_forwards_wire_format(monkeypatch):
    from types import SimpleNamespace

    from cfquant import xtdata

    captured = {}
    client = SimpleNamespace(
        request=lambda action, params: (
            captured.update({"action": action, "params": params}),
            {"subscribe_id": 7, "callback_event": "quote:7"},
        )[1],
        add_callback=lambda event, callback: None,
        remove_callback=lambda event, callback: None,
    )
    monkeypatch.setattr(xtdata, "get_client", lambda: client)
    monkeypatch.setattr(xtdata, "_subscription_callbacks", {})
    xtdata.subscribe_whole_quote(["SH"], wire_format="columnar")
    assert captured["params"]["wire_format"] == "columnar"
