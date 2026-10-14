"""Binary hot-path frame codec and hub pass-through routing; no real pipes."""
from cfquant.pipe_hub import CfquantPipeHub
from cfquant.pipe_transport import (
    FRAME_FLAG_COLUMNAR,
    FRAME_TYPE_DELIVERY,
    FRAME_TYPE_PUBLISH_EVENT,
    FRAME_TYPE_PUBLISH_RESPONSE,
    FRAME_TYPE_REQUEST,
    PIPE_FRAME_MAGIC,
    PipeTxClient,
    decode_pipe_frame,
    dumps_pipe_message,
    encode_pipe_frame,
    loads_pipe_message,
    scan_event_name,
    scan_response_request_id,
)
from cfquant.protocol import pack_event, pack_request, pack_response


class FakePipeConn(object):
    def __init__(self, name):
        self.name = name
        self.closed = False
        self.writes = []

    def read_frame(self):
        raise AssertionError("unexpected read on %s" % self.name)

    def write_frame(self, payload):
        self.writes.append(payload)

    def close(self):
        self.closed = True


class FeedingConn(FakePipeConn):
    def __init__(self, name, frames):
        super(FeedingConn, self).__init__(name)
        self.frames = list(frames)

    def read_frame(self):
        return self.frames.pop(0) if self.frames else None


def qmt_hello(role, channel="cfquant.normal.request", binary=False):
    hello = {
        "type": "hello",
        "role": role,
        "bridge_id": "bridge-1",
        "endpoint_name": "normal",
        "instance_id": "instance-1",
        "process_id": "instance-1",
        "request_channel": channel,
        "request_channels": [channel],
        "heartbeat_interval": 10,
    }
    if binary:
        hello["binary_hotpath"] = True
    return hello


def api_hello(role, client_id="client-a", binary=False):
    hello = {"type": "hello", "role": role, "client_id": client_id}
    if binary:
        hello["binary_hotpath"] = True
    return hello


def make_hub():
    hub = CfquantPipeHub(pipe_name="unit_pipe", show=False)
    hub._write_status = lambda: None
    hub.running = True
    return hub


def binary_writes(conn):
    return [decode_pipe_frame(item) for item in conn.writes if isinstance(item, (bytes, bytearray))]


def test_frame_codec_roundtrip_all_tlvs():
    payload = 'cfquant:{"type":"request"}'
    frame = encode_pipe_frame(
        FRAME_TYPE_REQUEST,
        payload,
        request_id="req_1",
        client_id="client-a",
        channel="cfquant.normal.request",
        action="xtdata.get_full_tick",
    )
    assert frame[:2] == PIPE_FRAME_MAGIC
    envelope = decode_pipe_frame(frame)
    assert envelope["_binary"] is True
    assert envelope["type"] == "request"
    assert envelope["request_id"] == "req_1"
    assert envelope["client_id"] == "client-a"
    assert envelope["request_channel"] == "cfquant.normal.request"
    assert envelope["action"] == "xtdata.get_full_tick"
    assert envelope["payload"] == payload.encode("utf-8")


def test_frame_codec_flags_and_bytes_payload():
    frame = encode_pipe_frame(
        FRAME_TYPE_PUBLISH_EVENT,
        b"\x00\x01columnar-bytes",
        client_id="client-a",
        flags=FRAME_FLAG_COLUMNAR,
    )
    envelope = decode_pipe_frame(frame)
    assert envelope["type"] == "publish"
    assert envelope["key"] == "event"
    assert envelope["flags"] & FRAME_FLAG_COLUMNAR
    assert envelope["payload"] == b"\x00\x01columnar-bytes"


def test_decode_pipe_frame_discriminates_text_and_raw():
    text = dumps_pipe_message({"type": "hello", "role": "api_rx"})
    assert decode_pipe_frame(text)["type"] == "hello"
    assert decode_pipe_frame(text.encode("utf-8"))["type"] == "hello"
    assert decode_pipe_frame(b"not-a-frame") is None
    assert decode_pipe_frame("not-a-frame") is None


def test_routing_scans_match_own_protocol_messages():
    response = pack_response("req_9", ok=True, result={"pong": True})
    assert scan_response_request_id(response) == "req_9"
    event = pack_event("quote:1", data={"000001.SZ": {"lastPrice": 1}}, client_id="client-a")
    assert scan_event_name(event) == "quote:1"
    assert scan_response_request_id(event) is None
    assert scan_response_request_id("cfquant:not-json") is None


def test_hello_with_binary_hotpath_receives_ack_and_capability():
    hub = make_hub()
    conn = FakePipeConn("api-rx")
    hub._handle_hello(conn, api_hello("api_rx", binary=True), "api")
    assert conn.binary_hotpath is True
    acks = [loads_pipe_message(item) for item in conn.writes if isinstance(item, str)]
    assert len(acks) == 1
    assert acks[0]["type"] == "hello_ack"
    assert acks[0]["binary_hotpath"] is True


def test_hello_without_binary_flag_gets_no_ack():
    hub = make_hub()
    conn = FakePipeConn("api-rx")
    hub._handle_hello(conn, api_hello("api_rx"), "api")
    assert conn.binary_hotpath is False
    assert conn.writes == []


def test_status_reports_binary_capability():
    hub = make_hub()
    conn = FakePipeConn("qmt-rx")
    hub._handle_hello(conn, qmt_hello("qmt_rx", binary=True), "api")
    status = hub.status()
    assert status["hub_status_schema"] == 4
    assert status["hub_binary_hotpath"] is True
    assert status["qmt_channel_states"]["cfquant.normal.request"]["rx"]["binary_hotpath"] is True


def register_pair(hub, binary_qmt=True, binary_api=True):
    api_rx = FakePipeConn("api-rx")
    api_tx = FakePipeConn("api-tx")
    qmt_rx = FakePipeConn("qmt-rx")
    qmt_tx = FakePipeConn("qmt-tx")
    hub._handle_hello(api_rx, api_hello("api_rx", binary=binary_api), "api")
    hub._handle_hello(api_tx, api_hello("api_tx"), "api")
    hub._handle_hello(qmt_rx, qmt_hello("qmt_rx", binary=binary_qmt), "api")
    hub._handle_hello(qmt_tx, qmt_hello("qmt_tx"), "api")
    return api_rx, api_tx, qmt_rx, qmt_tx


def send_binary_request(hub, conn, request_id="req_42", action="xtdata.get_full_tick"):
    payload = pack_request(
        action,
        params={"code_list": ["000001.SZ"]},
        client_id="client-a",
        request_id=request_id,
    )
    frame = encode_pipe_frame(
        FRAME_TYPE_REQUEST,
        payload,
        request_id=request_id,
        client_id="client-a",
        channel="cfquant.normal.request",
        action=action,
    )
    hub._handle_api_request(conn, decode_pipe_frame(frame))
    return payload


def test_binary_request_forwarded_untouched_to_binary_qmt():
    hub = make_hub()
    api_rx, api_tx, qmt_rx, qmt_tx = register_pair(hub)
    payload = send_binary_request(hub, api_tx)

    deliveries = [item for item in binary_writes(qmt_rx) if item["type"] == "delivery"]
    assert len(deliveries) == 1
    assert deliveries[0]["payload"] == payload.encode("utf-8")
    with hub.state_lock:
        assert "req_42" in hub.pending
        assert hub.pending["req_42"]["action"] == "xtdata.get_full_tick"


def test_binary_request_falls_back_to_json_envelope_for_legacy_qmt():
    hub = make_hub()
    api_rx, api_tx, qmt_rx, qmt_tx = register_pair(hub, binary_qmt=False)
    payload = send_binary_request(hub, api_tx)

    assert binary_writes(qmt_rx) == []
    delivered = loads_pipe_message(qmt_rx.writes[-1])
    assert delivered["type"] == "delivery"
    assert delivered["payload"] == payload


def test_binary_response_routed_back_to_pending_client():
    hub = make_hub()
    api_rx, api_tx, qmt_rx, qmt_tx = register_pair(hub)
    send_binary_request(hub, api_tx)

    response = pack_response("req_42", ok=True, result={"pong": True})
    frame = encode_pipe_frame(
        FRAME_TYPE_PUBLISH_RESPONSE,
        response,
        request_id="req_42",
        client_id="client-a",
        channel="client-a",
    )
    hub._handle_qmt_publish(qmt_tx, decode_pipe_frame(frame))

    deliveries = [item for item in binary_writes(api_rx) if item["type"] == "delivery"]
    assert len(deliveries) == 1
    assert deliveries[0]["payload"] == response.encode("utf-8")
    with hub.state_lock:
        assert "req_42" not in hub.pending


def test_binary_event_routed_by_client_id():
    hub = make_hub()
    api_rx, api_tx, qmt_rx, qmt_tx = register_pair(hub)
    event = pack_event("quote:7", data={"000001.SZ": {"lastPrice": 10}}, client_id="client-a")
    frame = encode_pipe_frame(
        FRAME_TYPE_PUBLISH_EVENT,
        event,
        client_id="client-a",
        channel="client-a",
        event_name="quote:7",
    )
    hub._handle_qmt_publish(qmt_tx, decode_pipe_frame(frame))

    deliveries = [item for item in binary_writes(api_rx) if item["type"] == "delivery"]
    assert len(deliveries) == 1
    assert deliveries[0]["payload"] == event.encode("utf-8")


def test_binary_publish_without_request_id_falls_back_to_payload_parse():
    hub = make_hub()
    api_rx, api_tx, qmt_rx, qmt_tx = register_pair(hub)
    send_binary_request(hub, api_tx)

    response = pack_response("req_42", ok=True, result={"pong": True})
    frame = encode_pipe_frame(FRAME_TYPE_PUBLISH_RESPONSE, response, client_id="client-a")
    hub._handle_qmt_publish(qmt_tx, decode_pipe_frame(frame))

    deliveries = [item for item in binary_writes(api_rx) if item["type"] == "delivery"]
    assert len(deliveries) == 1
    assert deliveries[0]["payload"] == response.encode("utf-8")
    with hub.state_lock:
        assert "req_42" not in hub.pending


def test_binary_publish_raw_payload_uses_legacy_callback_path():
    hub = make_hub()
    api_rx, api_tx, qmt_rx, qmt_tx = register_pair(hub)
    frame = encode_pipe_frame(
        FRAME_TYPE_PUBLISH_EVENT,
        '{"event":"on_order","client_id":"client-a"}',
        client_id="client-a",
        channel="client-a",
    )
    hub._handle_qmt_publish(qmt_tx, decode_pipe_frame(frame))

    deliveries = [item for item in binary_writes(api_rx) if item["type"] == "delivery"]
    assert len(deliveries) == 1
    assert b"on_order" in deliveries[0]["payload"]


def test_pipe_tx_client_push_routed_uses_binary_and_scans_request_id():
    client = PipeTxClient(pipe_name="unit_pipe", heartbeat_interval=0)
    conn = FakePipeConn("qmt-tx")
    with client.conn_lock:
        client.tx_conn = conn
        client.connection_generation = 1
    client._hub_binary = True

    response = pack_response("req_7", ok=True, result=1)
    assert client.push("response", response, "client-a")["code"] == 0
    envelope = decode_pipe_frame(conn.writes[-1])
    assert envelope["type"] == "publish"
    assert envelope["key"] == "response"
    assert envelope["request_id"] == "req_7"
    assert envelope["client_id"] == "client-a"
    assert envelope["payload"] == response.encode("utf-8")

    event = pack_event("quote:3", data={"x": 1}, client_id="client-a")
    assert client.push("event", event, "client-a")["code"] == 0
    envelope = decode_pipe_frame(conn.writes[-1])
    assert envelope["key"] == "event"
    assert envelope["event_name"] == "quote:3"


def test_pipe_tx_client_falls_back_to_json_envelope_without_ack():
    client = PipeTxClient(pipe_name="unit_pipe", heartbeat_interval=0)
    conn = FakePipeConn("qmt-tx")
    with client.conn_lock:
        client.tx_conn = conn
        client.connection_generation = 1
    assert client._hub_binary is False

    response = pack_response("req_7", ok=True, result=1)
    assert client.push("response", response, "client-a")["code"] == 0
    envelope = loads_pipe_message(conn.writes[-1])
    assert envelope["type"] == "publish"
    assert envelope["payload"] == response


def test_pipe_tx_client_read_loop_sets_binary_from_ack_and_decodes_delivery():
    client = PipeTxClient(pipe_name="unit_pipe", heartbeat_interval=0)
    ack = dumps_pipe_message({"type": "hello_ack", "role": "qmt_rx", "binary_hotpath": True})
    payload = pack_response("req_7", ok=True, result=1)
    delivery = encode_pipe_frame(FRAME_TYPE_DELIVERY, payload, client_id="client-a")
    conn = FeedingConn("qmt-rx", [ack, delivery])
    with client.conn_lock:
        client.rx_conn = conn
        client.connection_generation = 1
    client.running = True

    client._read_loop(conn, 1)

    assert client._hub_binary is True
    assert client.Q.get_nowait() == payload
    assert conn.frames == []
