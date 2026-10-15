"""Regenerate the embedded fallback copy inside LTtx/tx from cfquant/tx.py.

Usage:
    python tools/sync_tx_wrapper.py          # write if changed
    python tools/sync_tx_wrapper.py --check  # exit 1 when drifted (CI guard)

cfquant/tx.py is the single source of truth for the LTtx client. LTtx/tx/tx.py
is a thin wrapper that imports it; LTtx/tx/_tx_embedded.py is an offline copy
for standalone LTtx deployments without the cfquant package. This tool keeps
that copy in sync so the two can never silently drift apart again.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "cfquant" / "tx.py"
TARGET = ROOT / "LTtx" / "tx" / "_tx_embedded.py"

HEADER = '''# -*- coding: utf-8 -*-
"""cfquant/tx.py 的内嵌副本（由 tools/sync_tx_wrapper.py 生成，勿手改）。

仅在运行环境缺少 cfquant 包时由 LTtx/tx/tx.py 兜底加载。修改请到
cfquant/tx.py，然后运行 python tools/sync_tx_wrapper.py 重新生成本文件。
"""
'''


def embedded_source() -> str:
    body = SOURCE.read_text(encoding="utf-8")
    # 去掉原文件头部的 coding 声明与模块 docstring，换成生成标记头
    lines = body.splitlines(keepends=True)
    if lines and lines[0].startswith("# -*- coding"):
        lines = lines[1:]
    text = "".join(lines).lstrip("\n")
    if text.startswith('"""'):
        end = text.find('"""', 3)
        if end != -1:
            text = text[end + 3:].lstrip("\n")
    return HEADER + "\n\n" + text


def main() -> int:
    check_only = "--check" in sys.argv[1:]
    generated = embedded_source()
    if TARGET.exists() and TARGET.read_text(encoding="utf-8") == generated:
        print("embedded copy already in sync")
        return 0
    if check_only:
        print("DRIFTED: LTtx/tx/_tx_embedded.py does not match cfquant/tx.py")
        print("run: python tools/sync_tx_wrapper.py")
        return 1
    compile(generated, str(TARGET), "exec")
    TARGET.write_text(generated, encoding="utf-8", newline="")
    print("regenerated", TARGET)
    return 0


if __name__ == "__main__":
    sys.exit(main())
