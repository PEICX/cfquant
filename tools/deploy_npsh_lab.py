#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""NPSH_LAB 单跳命名管道策略的部署工具。

子命令::

    python tools/deploy_npsh_lab.py --check                 # 校验 NPSH_LAB.py 内嵌传输块与 npsh_lite.py 同步
    python tools/deploy_npsh_lab.py --sync                  # 重新拼接内嵌传输块
    python tools/deploy_npsh_lab.py --build --qmt-root PATH # 生成 <qmt_root>/formulas/NPSH_LAB.rzrk
    python tools/deploy_npsh_lab.py --register --qmt-root PATH
                                                            # 登记 indexUserConfig.xml（QMT 必须已停止）
    python tools/deploy_npsh_lab.py --redeploy --qmt-root PATH
                                                            # 更新已导入策略：重建 rzrk + 清 catalog/py，
                                                            # 下次启动 QMT 重新导入（QMT 必须已停止）
    python tools/deploy_npsh_lab.py --status --qmt-root PATH
                                                            # 查看当前登记状态

策略名 NPSH_LAB 故意不带 CFQ_ 前缀：CFQ_* 是 cfquant 网页部署器的托管命名空间
（会被自动改写/下线），单跳实验策略不归它管，与 IOPV_LAB 同一模式。

登记条目抄 IOPV_LAB（其次 CFQ_10110148_LITE）：catalog 条目加进 IOPV_LAB 所在
FormulaCatalog 分组；strategyTrade item 克隆后改 name（NPSH_LAB）、id（现有最大
id + 1，不重复）、FromulaExpandData、备注，并置 startupAutorun=1。写入前备份到
<qmt_root>/cfquant_managed/backups/，写回用 minidom.parseString 复验。

真机部署顺序（详见 docs/single-hop-named-pipe.md）:
    1. 停止 QMT（bin.x64/XtItClient.exe 完全退出）
    2. --build   写入 formulas/NPSH_LAB.rzrk（启动时 importFormula 导入）
    3. --register 登记模型并开机自启
    4. 启动 QMT，日志确认 importFormula end + 模型 doRun
    5. 外部用 qmt_scripts/npsh_lab_client.py 验证
"""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from xml.dom import minidom

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

STRATEGY_NAME = "NPSH_LAB"
LAB_FILE = _REPO / "qmt_scripts" / (STRATEGY_NAME + ".py")
TRANSPORT_FILE = _REPO / "qmt_scripts" / "npsh_lite.py"
BEGIN_MARK = "# BEGIN NPSH_LITE_TRANSPORT"
END_MARK = "# END NPSH_LITE_TRANSPORT"
LAB_NAME_PATTERN = r"[A-Z][A-Z0-9_]{0,59}"
REMARK = "npsh single-hop lab %s" % time.strftime("%Y-%m-%d")


def load_embedded_transport():
    lab = LAB_FILE.read_bytes().decode("ascii").replace("\r\n", "\n")
    if BEGIN_MARK not in lab or END_MARK not in lab:
        raise SystemExit("NPSH_LAB.py 缺少 %s/%s 标记" % (BEGIN_MARK, END_MARK))
    begin = lab.index(BEGIN_MARK)
    end = lab.index(END_MARK)
    return lab, lab[begin + lab[begin:].index("\n") + 1:end].strip("\n"), begin, end


def verify_sync():
    lab, embedded, _, _ = load_embedded_transport()
    transport = TRANSPORT_FILE.read_bytes().decode("ascii").replace("\r\n", "\n").strip("\n")
    if embedded == transport:
        print("同步校验通过：%s 内嵌传输块与 npsh_lite.py 一致" % LAB_FILE.name)
        return True
    print("同步校验失败：NPSH_LAB.py 内嵌传输块与 npsh_lite.py 不一致")
    print("  运行 python tools/deploy_npsh_lab.py --sync 重新拼接")
    return False


def sync_embedded():
    lab, _, begin, end = load_embedded_transport()
    transport = TRANSPORT_FILE.read_bytes().decode("ascii").replace("\r\n", "\n").strip("\n")
    head = lab[:begin]
    head = head[:head.rindex("\n") + 1]  # 去掉 BEGIN 行本身
    # 保留原有 BEGIN/END 注释行；内部统一 LF，落盘统一 CRLF（QMT 侧惯例）
    begin_line = BEGIN_MARK + " (embedded from qmt_scripts/npsh_lite.py - keep in sync)"
    updated = head + begin_line + "\n" + transport + "\n\n" + END_MARK + lab[end + len(END_MARK):]
    updated = updated.replace("\r\n", "\n")
    if not updated.isascii():
        raise SystemExit("拼接结果不是纯 ASCII，拒绝写入")
    import ast
    ast.parse(updated)
    LAB_FILE.write_bytes(updated.replace("\n", "\r\n").encode("ascii"))
    print("已重新拼接 %s（%d 行）" % (LAB_FILE.name, updated.count("\n")))
    return True


def build_rzrk(qmt_root=None, out=None):
    from cfquant.qmt_strategy_package import build_package

    source = LAB_FILE.read_bytes().decode("ascii")
    package = build_package(
        STRATEGY_NAME,
        source,
        stock="SH000300",
        period=86400,
        description="cfquant single-hop named-pipe lab (npsh_lite)",
        name_pattern=LAB_NAME_PATTERN,
    )
    if out:
        path = Path(out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(package)
    else:
        path = Path(qmt_root) / "formulas" / (STRATEGY_NAME + ".rzrk")
        path.write_bytes(package)
    print("已生成 %s（%d 字节）" % (path, len(package)))
    return path


def _read_document(path):
    content = path.read_bytes()
    if len(content) > 16 * 1024 * 1024 or b"<!DOCTYPE" in content.upper() or b"<!ENTITY" in content.upper():
        raise SystemExit("不支持的 QMT XML 配置: %s" % path)
    document = minidom.parseString(content)
    if document.documentElement.tagName != "ICUserConfigFile":
        raise SystemExit("QMT XML 根节点异常: %s" % path)
    return document, content


def _atomic_write(path, data):
    temporary = path.with_name(path.name + ".tmp-%s" % uuid.uuid4().hex[:8])
    with open(temporary, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(str(temporary), str(path))


def _catalog_parent(document):
    """IOPV_LAB catalog 条目所在的 FormulaCatalog 分组（保持同组）。"""
    for group in document.getElementsByTagName("FormulaCatalog"):
        for entry in group.getElementsByTagName("catalog"):
            if entry.getAttribute("name") in ("IOPV_LAB", STRATEGY_NAME):
                return entry.parentNode
    groups = document.getElementsByTagName("FormulaCatalog")
    if groups:
        return groups[0]
    return None


def _strategy_items(document):
    sections = document.getElementsByTagName("strategyTrade")
    if len(sections) > 1:
        raise SystemExit("多个 strategyTrade 分节，请人工确认后重试")
    if not sections:
        section = document.createElement("strategyTrade")
        document.documentElement.appendChild(section)
    else:
        section = sections[0]
    return section, list(section.getElementsByTagName("item"))


def _remove_catalog_entry(document, name):
    changed = False
    for group in document.getElementsByTagName("FormulaCatalog"):
        for entry in list(group.getElementsByTagName("catalog")):
            if entry.getAttribute("name") == name:
                parent = entry.parentNode
                if parent is not None:
                    parent.removeChild(entry)
                    changed = True
    return changed


def redeploy(qmt_root):
    """更新已导入的策略：重建 rzrk、删除 python/ 文件与 catalog 条目，
    下次启动 QMT 时重新 importFormula。要求 QMT 已停止。"""
    from cfquant.qmt_strategy_deploy import qmt_is_running, qmt_root as normalize_root

    root = normalize_root(qmt_root, validate=True)
    if qmt_is_running(root):
        raise SystemExit("QMT 正在运行（XtItClient.exe），停止后再 redeploy")

    build_rzrk(str(root))
    py = root / "python" / (STRATEGY_NAME + ".py")
    if py.exists():
        py.unlink()
        print("已删除 %s（触发重导入）" % py)

    config = root / "config" / "indexUserConfig.xml"
    document, original = _read_document(config)
    if _remove_catalog_entry(document, STRATEGY_NAME):
        backup = root / "cfquant_managed" / "backups" / ("npsh_lab_%s.xml" % uuid.uuid4().hex[:12])
        backup.parent.mkdir(parents=True, exist_ok=True)
        backup.write_bytes(original)
        updated = document.toxml(encoding="utf-8")
        minidom.parseString(updated)
        _atomic_write(config, updated)
        print("已移除 catalog 条目（备份: %s），strategyTrade item 保留待重导后生效" % backup)
    else:
        print("catalog 条目不存在（首次部署无需移除）")
    print("下次启动 QMT 时将重新导入 %s.rzrk 并按 startupAutorun 自启" % STRATEGY_NAME)


def register(qmt_root):
    from cfquant.qmt_strategy_deploy import qmt_is_running, qmt_root as normalize_root

    root = normalize_root(qmt_root, validate=True)
    if qmt_is_running(root):
        raise SystemExit("QMT 正在运行（XtItClient.exe），停止后再登记 indexUserConfig.xml")

    config = root / "config" / "indexUserConfig.xml"
    document, original = _read_document(config)
    changed = False

    # 1) catalog 条目
    parent = _catalog_parent(document)
    if parent is None:
        raise SystemExit("indexUserConfig.xml 中未找到 FormulaCatalog 分组")
    existing = [entry for entry in parent.getElementsByTagName("catalog")
                if entry.getAttribute("name") == STRATEGY_NAME]
    if existing:
        print("catalog 条目已存在: %s" % STRATEGY_NAME)
    else:
        entry = document.createElement("catalog")
        for key, value in (("scriptType", "1"), ("formulaCatalogModelType", "4"),
                           ("systemProvidedStrategy", "0"), ("strategymall", "0"),
                           ("name", STRATEGY_NAME), ("type", "2"), ("simpleRun", "0")):
            entry.setAttribute(key, value)
        parent.appendChild(entry)
        changed = True
        print("已添加 catalog 条目: %s" % STRATEGY_NAME)

    # 2) strategyTrade item（抄 IOPV_LAB / CFQ_10110148_LITE 条目）
    section, items = _strategy_items(document)
    mine = [item for item in items if item.getAttribute("name") == STRATEGY_NAME]
    if mine:
        print("strategyTrade item 已存在 id=%s" % mine[0].getAttribute("id"))
        item = mine[0]
    else:
        template = None
        for item in items:
            if item.getAttribute("name") in ("IOPV_LAB", "CFQ_10110148_LITE"):
                template = item
                break
        if template is None:
            raise SystemExit("未找到 IOPV_LAB / CFQ_10110148_LITE 模板条目，请人工登记")
        item = template.cloneNode(deep=True)
        ids = [int(node.getAttribute("id")) for node in items if node.getAttribute("id").isdigit()]
        item.setAttribute("id", str(max(ids or [0]) + 1))
        section.appendChild(item)
        changed = True
        print("已添加 strategyTrade item id=%s（克隆自 %s）"
              % (item.getAttribute("id"), template.getAttribute("name")))

    attributes = {
        "name": STRATEGY_NAME,
        "startupAutorun": "1",
        "strategyRemark": REMARK,
        "FromulaExpandData": json.dumps(
            {"m_qsFormula": STRATEGY_NAME,
             "m_qsAccount": item.getAttribute("account") or "",
             "variableSize": "0"}, ensure_ascii=True),
    }
    for key, value in attributes.items():
        if item.getAttribute(key) != value:
            item.setAttribute(key, value)
            changed = True

    if not changed:
        print("登记已是最新，无需写入")
        return

    backup = root / "cfquant_managed" / "backups" / ("npsh_lab_%s.xml" % uuid.uuid4().hex[:12])
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_bytes(original)
    updated = document.toxml(encoding="utf-8")
    minidom.parseString(updated)  # 写回前 minidom 复验
    _atomic_write(config, updated)
    print("已写回 %s（备份: %s）" % (config, backup))


def status(qmt_root):
    from cfquant.qmt_strategy_deploy import qmt_is_running, qmt_root as normalize_root

    root = normalize_root(qmt_root, validate=True)
    config = root / "config" / "indexUserConfig.xml"
    document, _ = _read_document(config)
    running = qmt_is_running(root)
    print("QMT root: %s" % root)
    print("QMT 运行中: %s" % ("是" if running else "否"))

    parent = _catalog_parent(document)
    in_catalog = False
    if parent is not None:
        in_catalog = any(entry.getAttribute("name") == STRATEGY_NAME
                         for entry in parent.getElementsByTagName("catalog"))
    print("catalog 条目: %s" % ("已登记" if in_catalog else "未登记"))

    _, items = _strategy_items(document)
    mine = [item for item in items if item.getAttribute("name") == STRATEGY_NAME]
    if mine:
        item = mine[0]
        print("strategyTrade item: id=%s account=%s startupAutorun=%s"
              % (item.getAttribute("id"), item.getAttribute("account"),
                 item.getAttribute("startupAutorun")))
    else:
        print("strategyTrade item: 未登记")

    rzrk = root / "formulas" / (STRATEGY_NAME + ".rzrk")
    py = root / "python" / (STRATEGY_NAME + ".py")
    print("formulas/%s.rzrk: %s" % (STRATEGY_NAME, "存在" if rzrk.exists() else "不存在"))
    print("python/%s.py: %s（导入完成标志）" % (STRATEGY_NAME, "存在" if py.exists() else "不存在"))


def main(argv=None):
    parser = argparse.ArgumentParser(description="NPSH_LAB 单跳命名管道策略部署工具")
    parser.add_argument("--check", action="store_true", help="校验内嵌传输块同步")
    parser.add_argument("--sync", action="store_true", help="重新拼接内嵌传输块")
    parser.add_argument("--build", action="store_true", help="生成 .rzrk 包")
    parser.add_argument("--register", action="store_true", help="登记 indexUserConfig.xml")
    parser.add_argument("--redeploy", action="store_true", help="更新已导入策略（重建 rzrk + 触发重导入，QMT 须停止）")
    parser.add_argument("--status", action="store_true", help="查看登记状态")
    parser.add_argument("--qmt-root", default=None, help="QMT 安装根目录")
    parser.add_argument("--out", default=None, help="--build 的输出路径（默认 qmt_root/formulas）")
    args = parser.parse_args(argv)

    if not (args.check or args.sync or args.build or args.register or args.redeploy or args.status):
        parser.print_help()
        return 0

    if args.check:
        return 0 if verify_sync() else 1
    if args.sync:
        sync_embedded()
        verify_sync()
        return 0
    if args.build:
        if not args.out and not args.qmt_root:
            parser.error("--build 需要 --qmt-root 或 --out")
        build_rzrk(args.qmt_root, args.out)
        return 0
    if not args.qmt_root:
        parser.error("需要 --qmt-root")
    if args.redeploy:
        redeploy(args.qmt_root)
    if args.register:
        register(args.qmt_root)
    if args.status:
        status(args.qmt_root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
