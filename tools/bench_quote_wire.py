# -*- coding: utf-8 -*-
"""Compare JSON vs columnar encoding for whole-quote batches.

Usage: python tools/bench_quote_wire.py [--codes 1000] [--fields 30] [--runs 20]
"""
import argparse
import json
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from cfquant.quote_wire import decode_whole_quote, encode_whole_quote


def make_batch(n_codes, n_numeric):
    random.seed(7)
    suffixes = (".SH", ".SZ", ".BJ")
    batch = {}
    for index in range(n_codes):
        code = "%06d%s" % (index + 1, suffixes[index % 3])
        row = {}
        for field in range(n_numeric):
            name = "f%02d" % field
            if field % 10 == 0:
                row[name] = index * 1000 + field          # int
            elif field % 10 == 1:
                row[name] = None if index % 97 == 0 else round(random.uniform(1, 100), 4)
            elif field % 10 == 2:
                row[name] = "S%d" % (index % 53)          # short str
            elif field % 10 == 3:
                row[name] = index % 7 == 0                # bool
            elif field % 10 == 4:
                row[name] = None if index % 89 == 0 else [round(random.uniform(1, 100), 3) for _ in range(5)]
            else:
                row[name] = round(random.uniform(1, 1000), 6)
        batch[code] = row
    return batch


def bench(func, runs):
    times = []
    result = None
    for _ in range(runs):
        started = time.perf_counter()
        result = func()
        times.append((time.perf_counter() - started) * 1000.0)
    times.sort()
    return times[len(times) // 2], result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codes", type=int, default=1000)
    parser.add_argument("--fields", type=int, default=30)
    parser.add_argument("--runs", type=int, default=20)
    args = parser.parse_args()

    batch = make_batch(args.codes, args.fields)
    columnar = encode_whole_quote(batch)
    if columnar is None:
        print("synthetic batch is not columnar-encodable; bench aborted")
        return 1
    json_text = json.dumps(batch, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    encode_ms, _ = bench(lambda: encode_whole_quote(batch), args.runs)
    json_ms, _ = bench(lambda: json.dumps(batch, ensure_ascii=False, separators=(",", ":"), allow_nan=False), args.runs)
    decode_ms, decoded = bench(lambda: decode_whole_quote(columnar), args.runs)
    json_decode_ms, _ = bench(lambda: json.loads(json_text), args.runs)

    assert decoded == batch
    print("codes=%s fields=%s" % (args.codes, args.fields))
    print("size     columnar=%8d B   json=%8d B   ratio=%.2f"
          % (len(columnar), len(json_text.encode("utf-8")), len(columnar) / max(1, len(json_text.encode("utf-8")))))
    print("encode   columnar=%8.3f ms  json=%8.3f ms  speedup=%.2fx" % (encode_ms, json_ms, json_ms / max(encode_ms, 1e-9)))
    print("decode   columnar=%8.3f ms  json=%8.3f ms  speedup=%.2fx" % (decode_ms, json_decode_ms, json_decode_ms / max(decode_ms, 1e-9)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
