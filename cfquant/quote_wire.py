# -*- coding: utf-8 -*-
"""Columnar wire format for whole-quote push batches (pure stdlib).

Layout (big-endian):

    magic(4B "CFQ1") version(1B) reserved(1B) n_fields(2B) n_codes(2B)
    field table: n_fields * (len2 + utf-8 name)
    code table:  n_codes  * (len2 + utf-8 code)
    one column per field, in field order:
        col_type(1B)
        0 all-None : no cell data
        1 int64    : none-bitmap + n * 8B
        2 float64  : none-bitmap + n * 8B
        3 str      : n * (len2 + utf-8, 0xFFFF marks None)
        4 bool     : none-bitmap + n * 1B
        5 num list : n * (count2 + count * 8B, count 0 marks None)

encode_whole_quote returns None whenever a value does not fit (nested dicts,
mixed-type columns, oversized strings), so the caller falls back to the JSON
event path. Missing fields decode as None values.
"""
import struct


MAGIC = b"CFQ1"
VERSION = 1

COL_NONE = 0
COL_INT = 1
COL_FLOAT = 2
COL_STR = 3
COL_BOOL = 4
COL_NUM_LIST = 5

_STR_NONE = 0xFFFF

_HEAD = struct.Struct("!4sBBHH")
_U16 = struct.Struct("!H")


def _none_bitmap(values):
    bits = bytearray((len(values) + 7) // 8)
    for index, value in enumerate(values):
        if value is None:
            bits[index >> 3] |= 1 << (index & 7)
    return bytes(bits)


def _is_none(bitmap, index):
    return (bitmap[index >> 3] >> (index & 7)) & 1 == 1


def _column_type(values):
    kinds = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            kinds.add("b")
        elif isinstance(value, int):
            kinds.add("i")
        elif isinstance(value, float):
            kinds.add("f")
        elif isinstance(value, str):
            kinds.add("s")
        elif (
            isinstance(value, list)
            and value
            and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value)
        ):
            kinds.add("n")
        else:
            return None
    if not kinds:
        return COL_NONE
    if kinds == {"b"}:
        return COL_BOOL
    if kinds == {"i"}:
        return COL_INT
    if kinds in ({"f"}, {"i", "f"}):
        return COL_FLOAT
    if kinds == {"s"}:
        return COL_STR
    if kinds == {"n"}:
        return COL_NUM_LIST
    return None


def _put_text(out, value):
    raw = value.encode("utf-8")
    if len(raw) > _STR_NONE:
        return False
    out.extend(_U16.pack(len(raw)))
    out.extend(raw)
    return True


def encode_whole_quote(data):
    """Encode a {code: {field: value}} batch; None when it does not fit."""
    if not isinstance(data, dict) or not data:
        return None
    codes = []
    rows = []
    fields = []
    seen = set()
    for code, row in data.items():
        if not isinstance(code, str) or not isinstance(row, dict):
            return None
        codes.append(code)
        rows.append(row)
        for name in row:
            if not isinstance(name, str):
                return None
            if name not in seen:
                seen.add(name)
                fields.append(name)
    columns = []
    for name in fields:
        values = [row.get(name) for row in rows]
        col_type = _column_type(values)
        if col_type is None:
            return None
        columns.append((col_type, values))

    out = bytearray(_HEAD.pack(MAGIC, VERSION, 0, len(fields), len(codes)))
    for name in fields:
        if not _put_text(out, name):
            return None
    for code in codes:
        if not _put_text(out, code):
            return None
    for col_type, values in columns:
        out.append(col_type)
        if col_type == COL_NONE:
            continue
        if col_type in (COL_INT, COL_FLOAT, COL_BOOL):
            out.extend(_none_bitmap(values))
        if col_type == COL_INT:
            out.extend(struct.pack(
                "!%dq" % len(values), *[0 if v is None else int(v) for v in values]))
        elif col_type == COL_FLOAT:
            out.extend(struct.pack(
                "!%dd" % len(values), *[0.0 if v is None else float(v) for v in values]))
        elif col_type == COL_BOOL:
            out.extend(bytes(0 if v is None else (1 if v else 0) for v in values))
        elif col_type == COL_STR:
            for value in values:
                if value is None:
                    out.extend(_U16.pack(_STR_NONE))
                elif not _put_text(out, value):
                    return None
        elif col_type == COL_NUM_LIST:
            for value in values:
                if value is None:
                    out.extend(_U16.pack(0))
                    continue
                out.extend(_U16.pack(len(value)))
                out.extend(struct.pack("!%dd" % len(value), *[float(item) for item in value]))
    return bytes(out)


def decode_whole_quote(payload):
    """Decode back to {code: {field: value}}; None when the payload is invalid."""
    if not isinstance(payload, (bytes, bytearray)) or len(payload) < _HEAD.size:
        return None
    try:
        return _decode(payload)
    except (struct.error, IndexError):
        return None


def _decode(payload):
    magic, version, _reserved, n_fields, n_codes = _HEAD.unpack_from(payload, 0)
    if magic != MAGIC or version != VERSION:
        return None
    offset = _HEAD.size

    def read_text():
        nonlocal offset
        (length,) = _U16.unpack_from(payload, offset)
        offset += 2
        if length == _STR_NONE:
            return None
        value = payload[offset:offset + length].decode("utf-8", "replace")
        offset += length
        return value

    fields = [read_text() for _ in range(n_fields)]
    codes = [read_text() for _ in range(n_codes)]
    bitmap_size = (n_codes + 7) // 8
    columns = []
    for _ in range(n_fields):
        col_type = payload[offset]
        offset += 1
        if col_type == COL_NONE:
            columns.append(None)
            continue
        bitmap = None
        if col_type in (COL_INT, COL_FLOAT, COL_BOOL):
            bitmap = payload[offset:offset + bitmap_size]
            offset += bitmap_size
        if col_type == COL_INT:
            raw = struct.unpack_from("!%dq" % n_codes, payload, offset)
            offset += 8 * n_codes
            values = [None if _is_none(bitmap, i) else v for i, v in enumerate(raw)]
        elif col_type == COL_FLOAT:
            raw = struct.unpack_from("!%dd" % n_codes, payload, offset)
            offset += 8 * n_codes
            values = [None if _is_none(bitmap, i) else v for i, v in enumerate(raw)]
        elif col_type == COL_BOOL:
            raw = payload[offset:offset + n_codes]
            offset += n_codes
            values = [None if _is_none(bitmap, i) else bool(v) for i, v in enumerate(raw)]
        elif col_type == COL_STR:
            values = []
            for _ in range(n_codes):
                values.append(read_text())
        elif col_type == COL_NUM_LIST:
            values = []
            for _ in range(n_codes):
                (count,) = _U16.unpack_from(payload, offset)
                offset += 2
                if count == 0:
                    values.append(None)
                    continue
                values.append(list(struct.unpack_from("!%dd" % count, payload, offset)))
                offset += 8 * count
        else:
            return None
        columns.append(values)

    result = {}
    for row_index, code in enumerate(codes):
        row = {}
        for col_index, name in enumerate(fields):
            column = columns[col_index]
            row[name] = None if column is None else column[row_index]
        result[code] = row
    return result
