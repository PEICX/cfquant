# -*- coding: utf-8 -*-
"""main_push_queue 的回归测试（子进程内加载服务端模块做单元测试）。

历史问题：投递循环边遍历 group 边 remove 失效连接，remove 后迭代器会
跳过相邻元素——失效连接恰好排在存活连接前面时，那条推送会整体漏投递。

服务端模块 import 即有自动装依赖/写目录等副作用，因此放到子进程中
加载（与 test_lttx_v2_protocol 相同的桩模块方案），结果经 JSON 文件
回传。
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVER_SOURCE = REPO_ROOT / "LTtx" / "tx" / "LTtx_server.py"
STUB_MODULES = ("psutil", "pandas", "tabulate", "pandas_market_calendars", "pytz")

RUNNER_TEMPLATE = """
import os, runpy, sys, types, queue, threading, json

for name in %r:
    mod = types.ModuleType(name)
    mod.__version__ = '9.9.9'
    sys.modules[name] = mod

server = runpy.run_path(%r, run_name='lttx_push_queue_test')

class Sock:
    pass

dead, alive = Sock(), Sock()
delivery = queue.Queue()
server['dict_client_que'][alive] = delivery
server['dict_client_push_group']['chan'] = {'group': [dead, alive], 'pwd': '', 'que': queue.Queue()}

feed = queue.Queue()
thread = threading.Thread(target=server['main_push_queue'], args=(feed, 'chan'), daemon=True)
thread.start()
feed.put((None, 'msg-1'))
first = delivery.get(timeout=5)
group_after_first = list(server['dict_client_push_group']['chan']['group'])
feed.put((None, 'msg-2'))
second = delivery.get(timeout=5)
# 清空 group 后再投一条，触发循环顶部的空信道清理并退出线程
server['dict_client_push_group']['chan']['group'].clear()
feed.put((None, 'stop'))
thread.join(timeout=5)

result = {
    'first': first,
    'second': second,
    'dead_removed_after_first': dead not in group_after_first,
    'alive_kept_after_first': alive in group_after_first,
    'thread_stopped': not thread.is_alive(),
}
with open(%r, 'w', encoding='utf-8') as f:
    f.write(json.dumps(result))
"""


@pytest.fixture()
def push_queue_result(tmp_path):
    result_path = tmp_path / "result.json"
    runner = tmp_path / "runner.py"
    runner.write_text(
        RUNNER_TEMPLATE % (STUB_MODULES, str(SERVER_SOURCE), str(result_path)),
        encoding="utf-8",
    )
    runtime_dir = tmp_path / "runtime"
    env = dict(os.environ)
    env.update({
        "CFQUANT_LTTX_RUNTIME_DIR": str(runtime_dir),
        "CFQUANT_LTTX_FILE_DATA_DIR": str(runtime_dir / "file_data"),
        "CFQUANT_LTTX_DATAFRAME_DIR": str(runtime_dir / "dataframe_data"),
        "CFQUANT_LTTX_DICT_VAR_FILE": str(runtime_dir / "data0.txt"),
        "CFQUANT_LTTX_LOG_DIR": str(tmp_path / "server_logs"),
    })
    proc = subprocess.run(
        [sys.executable, str(runner)],
        env=env,
        cwd=str(tmp_path),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr.decode("utf-8", "replace")
    return json.loads(result_path.read_text(encoding="utf-8"))


def test_push_queue_delivers_past_dead_subscriber(push_queue_result):
    r = push_queue_result
    # 旧实现：dead 排在 alive 前面时 remove(dead) 使迭代器跳过 alive，消息漏投递
    assert r["first"] == "msg-1"
    assert r["second"] == "msg-2"
    assert r["dead_removed_after_first"] is True
    assert r["alive_kept_after_first"] is True
    assert r["thread_stopped"] is True
