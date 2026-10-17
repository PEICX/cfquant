"""Web account configuration integration with isolated state and deployment."""

import os

import pytest


@pytest.fixture
def web_config(tmp_path, monkeypatch):
    monkeypatch.setenv("CFQUANT_RUNTIME_DIR", str(tmp_path / "runtime"))
    import cfquant_web_server as web
    config = web.WebRuntimeConfig(str(tmp_path / "web.json"), str(tmp_path / "settings.db"))
    monkeypatch.setattr(web, "WEB_CONFIG", config)
    return web, config


def test_initialize_web_setup_can_skip_optional_admin_registration(web_config, monkeypatch, tmp_path):
    web, config = web_config
    monkeypatch.setattr(web, "auto_deploy_qmt_core_for_account", lambda *args, **kwargs: {
        "summary": {"ok": True},
        "results": [],
    })
    monkeypatch.setattr(web, "write_qmt_bridge_identity", lambda row: {"written": True, "path": "fake"})
    monkeypatch.setattr(web, "write_qmt_market_bridge_identities", lambda row: [])
    monkeypatch.setattr(web, "configure_account_qmt_strategies", lambda row, identity: {})
    monkeypatch.setattr(web, "qmt_auto_login_apply_for_account", lambda row, request=None, **kwargs: {
        "enabled": False,
    })
    monkeypatch.setattr(web, "ensure_account_runtime", lambda mode: {"mode": mode})

    data = web.initialize_web_setup({
        "account_id": "1000000001",
        "qmt_dir": str(tmp_path / "bin.x64"),
        "web_auth_enabled": False,
    })

    assert config.web_auth_enabled() is False
    assert data["server_access"]["web_auth_enabled"] is False
    assert data["web_auth"]["configured"] is False
    assert data["web_auth"]["username"] == ""
    assert data["setup"]["setup_required"] is False


def test_web_startup_starts_saved_qmt_bindings_once(web_config, monkeypatch, tmp_path):
    web, config = web_config
    qmt_dir = tmp_path / "qmt" / "bin.x64"
    config._data["account_configs"] = {
        "one": {
            "account_id": "1001",
            "account_type": "STOCK",
            "enabled": True,
            "qmt_dir": str(qmt_dir),
            "qmt_auto_login": {"enabled": True},
        },
        "two": {
            "account_id": "1002",
            "account_type": "STOCK",
            "enabled": True,
            "qmt_dir": str(qmt_dir),
            "qmt_auto_login": {"enabled": True},
        },
    }
    calls = []
    monkeypatch.setattr(web, "qmt_auto_login_apply_for_account", lambda row, request=None, **kwargs: calls.append((row["account_id"], kwargs)) or {"started": True})

    result = web.start_configured_qmt_on_web_startup()

    assert len(result) == 1
    assert calls == [("1001", {"restart": False, "reason": "web_startup"})]


def test_initialize_web_setup_persists_custom_python_environment(web_config, monkeypatch, tmp_path):
    web, config = web_config
    python_exe = tmp_path / "python.exe"
    python_exe.write_bytes(b"")
    monkeypatch.setattr(web, "auto_deploy_qmt_core_for_account", lambda *args, **kwargs: {"summary": {"ok": True}, "results": []})
    monkeypatch.setattr(web, "write_qmt_bridge_identity", lambda row: {"written": True})
    monkeypatch.setattr(web, "write_qmt_market_bridge_identities", lambda row: [])
    monkeypatch.setattr(web, "configure_account_qmt_strategies", lambda row, identity: {})
    monkeypatch.setattr(web, "qmt_auto_login_apply_for_account", lambda row, request=None, **kwargs: {"enabled": False})
    monkeypatch.setattr(web, "ensure_account_runtime", lambda mode: {"mode": mode})
    result = web.initialize_web_setup({
        "account_id": "1000000001",
        "python_environment": {"mode": "custom", "python_executable": str(python_exe)},
        "qmt_dir": str(tmp_path / "bin.x64"),
        "web_auth_enabled": False,
    })
    assert result["python_environment"]["mode"] == "custom"
    assert config.python_executable() == str(python_exe)
    assert web.PIPE_HUB._python_exe() == str(python_exe)


def test_web_reload_info_uses_target_port_and_preserves_previous_listener(web_config, monkeypatch):
    web, _ = web_config
    monkeypatch.setattr(web, "WEB_BOUND_HOST", "127.0.0.1")
    monkeypatch.setattr(web, "WEB_BOUND_PORT", 8765)
    monkeypatch.setattr(web, "server_access_info", lambda: {
        "configured_host": "0.0.0.0",
        "configured_port": 9876,
        "next_url": "http://127.0.0.1:9876/",
    })

    info = web.web_reload_info(reason="settings")

    assert info["host"] == "0.0.0.0"
    assert info["port"] == 9876
    assert info["previous_host"] == "127.0.0.1"
    assert info["previous_port"] == 8765


def test_spawn_reloaded_web_server_preserves_host_port_and_wait_env(web_config, monkeypatch, tmp_path):
    web, _ = web_config
    calls = []

    class FakePopen:
        pid = 4321

        def __init__(self, command, **kwargs):
            calls.append((command, kwargs))

    monkeypatch.setattr(web.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(web, "LOG_DIR", str(tmp_path))
    monkeypatch.setattr(web, "STATE_DIR", str(tmp_path))

    result = web.spawn_reloaded_web_server({
        "host": "0.0.0.0",
        "port": 9876,
        "next_url": "http://127.0.0.1:9876/",
    })

    assert result["pid"] == 4321
    command, kwargs = calls[0]
    assert command[0]
    if web.os.name == "nt" and os.path.isfile(os.path.join(web.BASE_DIR, "restart_cfquant.bat")):
        assert command[0].lower().endswith(("cmd.exe", "wscript.exe"))
        if command[0].lower().endswith("wscript.exe"):
            assert command[1].endswith("run_hidden_batch.vbs")
            assert command[2].endswith("restart_cfquant.bat")
        else:
            assert command[1:3] == ["/d", "/c"]
            assert command[3] == "call"
            assert command[4].endswith("restart_cfquant.bat")
    else:
        assert command[1].endswith("cfquant_web_server.py")
        assert command[command.index("--host") + 1] == "0.0.0.0"
        assert command[command.index("--port") + 1] == "9876"
    env = kwargs["env"]
    assert env["CFQUANT_WEB_HOST"] == "0.0.0.0"
    assert env["CFQUANT_WEB_PORT"] == "9876"
    assert env[web.WEB_RELOAD_WAIT_HOST_ENV] == "0.0.0.0"
    assert env[web.WEB_RELOAD_WAIT_PORT_ENV] == "9876"
    if web.os.name == "nt" and os.path.isfile(os.path.join(web.BASE_DIR, "restart_cfquant.bat")):
        assert env["CFQUANT_RESTART_NO_PAUSE"] == "1"
        assert env["CFQUANT_START_NO_PAUSE"] == "1"


def test_web_persists_strategy_flags_and_preserves_them_for_older_clients(web_config, tmp_path):
    web, config = web_config
    row = config.save_account_config("1000000001", qmt_dir=str(tmp_path), qmt_strategy={
        "enabled": True, "live": True, "autorun": False})
    saved = config.save_account_config("1000000001", qmt_dir=str(tmp_path), display_name="Updated")
    assert saved["qmt_strategy"] == row["qmt_strategy"]
    reloaded = web.WebRuntimeConfig(config.path, config.settings_db_path)
    assert reloaded.account_configs()[row["account_key"]]["qmt_strategy"] == row["qmt_strategy"]


def test_new_binding_defaults_strategy_to_live_mode(web_config, tmp_path):
    _, config = web_config
    row = config.save_account_config("1000000001", qmt_dir=str(tmp_path),
                                     qmt_strategy={"enabled": True})
    assert row["qmt_strategy"]["live"] is True


def test_new_binding_defaults_qmt_auto_login_to_enabled(web_config, tmp_path):
    _, config = web_config
    row = config.save_account_config("1000000001", qmt_dir=str(tmp_path))
    assert row["qmt_auto_login"] == {"enabled": True, "restart_times": ["08:00"]}


def test_binding_preserves_explicitly_disabled_qmt_auto_login(web_config, tmp_path):
    _, config = web_config
    config.save_account_config(
        "1000000001",
        qmt_dir=str(tmp_path),
        qmt_auto_login={"enabled": False},
    )
    row = config.save_account_config("1000000001", qmt_dir=str(tmp_path))
    assert row["qmt_auto_login"] == {"enabled": False, "restart_times": ["08:00"]}


def test_same_qmt_same_fund_has_one_mode_across_path_aliases_and_bindings(web_config, tmp_path):
    web, config = web_config
    first = config.save_account_config("1000000001", bridge_id="first", qmt_dir=str(tmp_path), data_provider=True)
    other = config.save_account_config("1000000002", bridge_id="other", qmt_dir=str(tmp_path))
    second = config.save_account_config("1000000001", bridge_id="second", qmt_dir=str(tmp_path / "bin.x64"), mode="lite")
    rows = config.account_configs()
    assert rows[first["account_key"]]["enabled"] is False
    assert rows[first["account_key"]]["data_provider"] is False
    assert config.account_pairs()[first["account_key"]]["enabled"] is False
    assert rows[second["account_key"]]["enabled"] is True
    assert rows[other["account_key"]]["enabled"] is True
    assert config.setup_info()["default_account_key"] != first["account_key"]


def test_set_data_provider_returns_complete_account_payload(web_config, tmp_path, monkeypatch):
    web, config = web_config
    first = config.save_account_config("1000000001", bridge_id="first", qmt_dir=str(tmp_path), data_provider=True)
    second = config.save_account_config("8885060548", bridge_id="default", qmt_dir=str(tmp_path / "guojin"))
    monkeypatch.setattr(web.ACCOUNT_CACHE, "prime_configured_accounts", lambda: None)
    monkeypatch.setattr(web.STATUS_MONITOR, "wake", lambda: None)
    monkeypatch.setattr(web.CALLBACKS, "refresh_channels", lambda channels: None)

    data = web.set_data_provider({"account_key": second["account_key"]})

    assert data["setup"]["data_provider_account_key"] == second["account_key"]
    assert data["account"]["account_id"] == "8885060548"
    assert data["account_configs"][first["account_key"]]["data_provider"] is False
    assert data["account_configs"][second["account_key"]]["data_provider"] is True
    assert data["bridges"]["default"]["python_dir"] == str(tmp_path / "guojin")


def test_disabled_account_cannot_be_set_as_data_provider(web_config, tmp_path, monkeypatch):
    web, config = web_config
    disabled = config.save_account_config("8885060548", qmt_dir=str(tmp_path), enabled=False)
    monkeypatch.setattr(web.ACCOUNT_CACHE, "prime_configured_accounts", lambda: None)
    monkeypatch.setattr(web.STATUS_MONITOR, "wake", lambda: None)
    monkeypatch.setattr(web.CALLBACKS, "refresh_channels", lambda channels: None)

    with pytest.raises(ValueError, match="disabled account"):
        web.set_data_provider({"account_key": disabled["account_key"]})


def test_advanced_mode_cannot_use_two_aliases_for_one_qmt(web_config, tmp_path):
    _, config = web_config
    with pytest.raises(ValueError, match="QMT"):
        config.save_account_config("1000000001", mode="lttx", qmt_dir=str(tmp_path),
                                   qmt_trade_dir=str(tmp_path / "bin.x64"))


def test_managed_accounts_in_one_qmt_get_distinct_channels(web_config, tmp_path):
    _, config = web_config
    first = config.save_account_config("1000000001", qmt_dir=str(tmp_path), qmt_strategy={"enabled": True})
    second = config.save_account_config("1000000002", qmt_dir=str(tmp_path), qmt_strategy={"enabled": True})
    assert first["bridge_id"] != second["bridge_id"]
    assert all(row["enabled"] for row in config.account_configs().values())


def test_managed_market_routes_require_directories_before_saving(web_config, tmp_path):
    _, config = web_config
    original = config.save_account_config("1000000001", qmt_dir=str(tmp_path),
                                          qmt_strategy={"enabled": True})
    with pytest.raises(ValueError, match="SH/SZ"):
        config.save_account_config("1000000001", qmt_dir=str(tmp_path),
                                   market_routing_enabled=True, market_bridges={},
                                   qmt_strategy={"enabled": True})
    assert config.account_configs()[original["account_key"]] == original
    with pytest.raises(ValueError, match="SZ"):
        config.save_account_config("1000000001", qmt_dir=str(tmp_path),
                                   market_routing_enabled=True,
                                   market_bridges={"SH": {"qmt_dir": str(tmp_path)}})
    assert config.account_configs()[original["account_key"]] == original


def test_market_routes_can_be_disabled_without_resending_route_details(web_config, tmp_path):
    _, config = web_config
    original = config.save_account_config("1000000001", qmt_dir=str(tmp_path),
        market_routing_enabled=True,
        market_bridges={market: {"qmt_dir": str(tmp_path)} for market in ("SH", "SZ")},
        qmt_strategy={"enabled": True})
    unchanged = config.save_account_config("1000000001", qmt_dir=str(tmp_path))
    assert unchanged["market_bridges"] == original["market_bridges"]
    assert unchanged["market_routing_enabled"] is True
    disabled = config.save_account_config("1000000001", qmt_dir=str(tmp_path), market_routing_enabled=False)
    assert disabled["market_routing_enabled"] is False
    assert disabled["market_bridges"] == {}
    assert disabled["qmt_strategy"]["enabled"] is True


def test_save_and_delete_call_deployment_manager(web_config, monkeypatch):
    web, config = web_config
    calls = []
    class Manager:
        def reconcile(self, accounts):
            calls.append(("reconcile", set(accounts)))
        def configure(self, row, identities, force=False, scripts_dir=None):
            assert force is True
            assert scripts_dir == os.path.join(web.BASE_DIR, "qmt_scripts")
            calls.append(("configure", row["qmt_strategy"], identities))
            return {"enabled": True, "targets": [{"state": "waiting_exit"}]}
    monkeypatch.setattr(web, "QMT_STRATEGIES", Manager())
    monkeypatch.setattr(web, "auto_deploy_qmt_core_for_account", lambda *args, **kwargs: {})
    monkeypatch.setattr(web, "write_qmt_bridge_identity", lambda row: {"written": True, "path": "fake"})
    monkeypatch.setattr(web, "write_qmt_market_bridge_identities", lambda row: [])
    monkeypatch.setattr(web, "ensure_account_runtime", lambda mode: {})
    monkeypatch.setattr(web.ACCOUNT_CACHE, "prime_configured_accounts", lambda: None)
    monkeypatch.setattr(web.STATUS_MONITOR, "wake", lambda: None)
    monkeypatch.setattr(web.CALLBACKS, "refresh_channels", lambda channels: None)
    data = web.save_account_runtime_config({"account_id": "1000000001", "qmt_dir": "D:/FAKE-QMT",
                                           "qmt_strategy": {"enabled": True, "live": False, "autorun": True}})
    assert data["qmt_strategy_deploy"]["targets"][0]["state"] == "waiting_exit"
    assert calls[1][0] == "configure"
    assert calls[1][1]["autorun"] is True
    web.delete_account_runtime_config({"account_key": data["account"]["account_key"]})
    assert calls[-1] == ("reconcile", set())


def test_save_can_start_qmt_auto_login_and_persist_restart_times(web_config, monkeypatch, tmp_path):
    web, config = web_config
    calls = []
    monkeypatch.setattr(web, "auto_deploy_qmt_core_for_account", lambda *args, **kwargs: {})
    monkeypatch.setattr(web, "write_qmt_bridge_identity", lambda row: {"written": True, "path": "fake"})
    monkeypatch.setattr(web, "write_qmt_market_bridge_identities", lambda row: [])
    monkeypatch.setattr(web, "configure_account_qmt_strategies", lambda row, identity: {})
    monkeypatch.setattr(web, "ensure_account_runtime", lambda mode: {})
    monkeypatch.setattr(web.ACCOUNT_CACHE, "prime_configured_accounts", lambda: None)
    monkeypatch.setattr(web.STATUS_MONITOR, "wake", lambda: None)
    monkeypatch.setattr(web.CALLBACKS, "refresh_channels", lambda channels: None)

    def apply(row, request=None, **kwargs):
        calls.append((row["account_id"], request, row["qmt_auto_login"]))
        return {"enabled": True, "started": True, "pid": 1234,
                "restart_times": row["qmt_auto_login"]["restart_times"]}

    monkeypatch.setattr(web, "qmt_auto_login_apply_for_account", apply)
    data = web.save_account_runtime_config({
        "account_id": "8885060548",
        "display_name": "国金证券",
        "qmt_dir": str(tmp_path / "QMT" / "bin.x64"),
        "qmt_auto_login": {"enabled": True, "restart_times": ["6:30", "06:30", "18:05"]},
    })
    assert data["qmt_auto_login"]["pid"] == 1234
    assert data["account"]["qmt_auto_login"] == {"enabled": True, "restart_times": ["06:30", "18:05"]}
    assert calls == [("8885060548", {"enabled": True, "restart_times": ["6:30", "06:30", "18:05"]},
                      {"enabled": True, "restart_times": ["06:30", "18:05"]})]


def test_complete_qmt_auto_login_restarts_saved_account(web_config, monkeypatch, tmp_path):
    web, config = web_config
    row = config.save_account_config(
        "8885060548",
        qmt_dir=str(tmp_path),
        display_name="国金证券",
        qmt_auto_login={"enabled": True, "restart_times": ["07:30"]},
    )
    calls = []
    monkeypatch.setattr(web, "qmt_auto_login_restart_for_account",
                        lambda account, request=None, reason="manual": calls.append((account["account_key"], request, reason)) or {"restarted": True})
    data = web.complete_qmt_auto_login({"account_key": row["account_key"]})
    assert data["restarted"] is True
    assert calls == [(row["account_key"], {"enabled": True, "restart_times": ["07:30"]}, "manual")]


def test_reset_web_auth_password_writes_file_and_revokes_old_session(web_config, monkeypatch, tmp_path):
    web, config = web_config
    monkeypatch.setattr(web, "BASE_DIR", str(tmp_path / "project"))
    config.set_server_access_settings(
        web_auth_enabled=True,
        web_auth_username="operator",
        web_auth_password="old-password",
    )
    token = web.issue_web_auth_token("operator", remember=True)
    assert config.verify_web_auth("operator", "old-password")
    assert web.web_auth_token_info(token)

    result = config.reset_web_auth_password()

    password_file = tmp_path / "project" / "cfquant_web_reset_password.txt"
    assert result["password_file"] == str(password_file)
    assert config.server_access_info()["web_auth_password_file"] == str(password_file)
    assert password_file.is_file()
    content = password_file.read_text(encoding="utf-8")
    new_password = content.split("新密码: ", 1)[1].splitlines()[0]
    assert len(new_password) == 18
    assert new_password != "old-password"
    assert not config.verify_web_auth("operator", "old-password")
    assert config.verify_web_auth("operator", new_password)
    assert web.web_auth_token_info(token) is None


def test_reset_web_auth_password_enables_auth_when_it_was_not_configured(web_config, monkeypatch, tmp_path):
    web, config = web_config
    monkeypatch.setattr(web, "BASE_DIR", str(tmp_path / "project"))
    result = config.reset_web_auth_password()
    password_file = tmp_path / "project" / "cfquant_web_reset_password.txt"
    new_password = password_file.read_text(encoding="utf-8").split("新密码: ", 1)[1].splitlines()[0]
    assert result["username"] == "admin"
    assert config.web_auth_enabled()
    assert config.verify_web_auth("admin", new_password)


def test_wait_processes_exit_polls_until_directory_is_quiet(web_config, monkeypatch):
    web, _ = web_config
    snapshots = [[{"pid": 1}], [{"pid": 1}], []]
    calls = {"n": 0}

    def fake_processes(path):
        index = min(calls["n"], len(snapshots) - 1)
        calls["n"] += 1
        return snapshots[index]

    monkeypatch.setattr(web, "_qmt_auto_login_processes", fake_processes)
    monkeypatch.setattr(web.time, "sleep", lambda seconds: None)
    remaining = web._qmt_auto_login_wait_processes_exit("bin.x64", timeout=5.0, interval=0.01)
    assert remaining == []
    assert calls["n"] == 3


def test_wait_processes_exit_returns_stragglers_on_timeout(web_config, monkeypatch):
    web, _ = web_config
    monkeypatch.setattr(web, "_qmt_auto_login_processes", lambda path: [{"pid": 9}])
    monkeypatch.setattr(web.time, "sleep", lambda seconds: None)
    # Deadline already elapsed -> return without spinning forever.
    monkeypatch.setattr(web.time, "monotonic", lambda: 1000.0)
    assert web._qmt_auto_login_wait_processes_exit("bin.x64", timeout=0.0) == [{"pid": 9}]


def test_scheduled_restart_waits_for_exit_then_relaunches(web_config, monkeypatch, tmp_path):
    web, config = web_config
    qmt_bin = tmp_path / "QMT" / "bin.x64"
    qmt_bin.mkdir(parents=True)
    (qmt_bin / "XtItClient.exe").write_bytes(b"")
    row = config.save_account_config(
        "8885060548",
        qmt_dir=str(qmt_bin.parent),
        qmt_auto_login={"enabled": True, "restart_times": ["08:00"]},
    )
    events = []
    monkeypatch.setattr(web, "_qmt_auto_login_stop_xtitclient",
                        lambda row, paths, session=None: events.append("stop") or {"requested_pids": [1]})
    monkeypatch.setattr(web, "_qmt_auto_login_wait_processes_exit",
                        lambda bin_dir, **kwargs: events.append("wait") or [])
    monkeypatch.setattr(web, "_qmt_auto_login_start_xtitclient",
                        lambda paths, use_link=False: events.append("launch") or {"pid": 4321, "bin_dir": paths["bin_dir"]})
    monkeypatch.setattr(web, "_qmt_auto_login_processes",
                        lambda path: [] if "launch" in events else [{"pid": 1, "name": "XtItClient.exe"}])
    monkeypatch.setattr(web, "_qmt_auto_login_start_update_guard",
                        lambda bin_dir, row=None: events.append("guard") or {"started": True, "key": bin_dir})
    result = web.qmt_auto_login_restart_for_account(row, reason="scheduled:08:00")
    assert events == ["stop", "wait", "launch", "guard"]
    assert result["restarted"] is True
    assert result["restart_pid"] == 4321


def test_qmt_process_snapshot_matches_directory_and_reports_pids(web_config, monkeypatch, tmp_path):
    web, _ = web_config
    qmt_bin = tmp_path / "QMT" / "bin.x64"
    qmt_bin.mkdir(parents=True)
    (qmt_bin / "XtItClient.exe").write_bytes(b"")
    monkeypatch.setattr(web, "_qmt_auto_login_processes", lambda path: [{
        "pid": 2718,
        "name": "XtItClient.exe",
        "executable_path": os.path.join(str(path), "XtItClient.exe"),
    }])
    result = web.qmt_process_snapshot(str(qmt_bin.parent))
    assert result["running"] is True
    assert result["pids"] == [2718]
    assert result["exe_path"].lower().endswith("bin.x64\\xtitclient.exe")


def test_qmt_process_preflight_rejects_running_strategy_before_mutation(web_config, monkeypatch, tmp_path):
    web, _ = web_config
    calls = []
    monkeypatch.setattr(web, "qmt_process_snapshots_for_request", lambda **kwargs: [{
        "qmt_dir": str(tmp_path), "running": True, "pids": [1234],
    }])
    with pytest.raises(RuntimeError, match="PID 1234"):
        web.qmt_process_preflight(body={
            "qmt_dir": str(tmp_path),
            "qmt_strategy": {"enabled": True},
        })
    assert calls == []


def test_qmt_process_snapshot_request_uses_saved_row_when_body_has_only_account_key(web_config, monkeypatch, tmp_path):
    web, _ = web_config
    captured = []
    monkeypatch.setattr(web, "qmt_process_snapshot", lambda path: captured.append(path) or {"running": False})
    web.qmt_process_snapshots_for_request(
        body={"account_key": "900010001595"},
        row={"qmt_dir": str(tmp_path / "QMT")},
    )
    assert captured == [web.normalize_optional_path(str(tmp_path / "QMT"))]


def test_update_processes_matches_xtupdate_by_name(web_config, monkeypatch):
    web, _ = web_config
    class FakeProcess:
        def __init__(self, pid, name, exe):
            self.info = {"pid": pid, "name": name}
            self._exe = exe

        def exe(self):
            return self._exe

    processes = [
        FakeProcess(1, "XtItClient.exe", r"C:\QMT\bin.x64\XtItClient.exe"),
        FakeProcess(2, "XtUpdate.exe", r"C:\QMT\bin.x64\XtUpdate.exe"),
        FakeProcess(3, "XtUpdate.exe", ""),
        FakeProcess(4, "explorer.exe", r"C:\Windows\explorer.exe"),
    ]
    monkeypatch.setattr(web.psutil, "process_iter", lambda *args, **kwargs: iter(processes))
    rows = web._qmt_auto_login_update_processes()
    assert [row["pid"] for row in rows] == [2, 3]


def test_update_kill_helper_force_closes_resident_updater(web_config, monkeypatch):
    web, _ = web_config
    killed = []
    def fake_run(command, **kwargs):
        killed.append(command)
        return web.subprocess.CompletedProcess(args=command, returncode=0, stdout="SUCCESS", stderr="")
    monkeypatch.setattr(web.subprocess, "run", fake_run)
    event = web._qmt_auto_login_kill_update_processes([{"pid": 7788, "name": "XtUpdate.exe"}])
    assert event["killed"] is True
    assert event["pids"] == [7788]
    assert "跳过" in event["message"] and "更新" in event["message"]
    assert killed == [["taskkill", "/PID", "7788", "/T", "/F"]]


def test_update_guard_does_not_touch_updater_before_delay(web_config, monkeypatch):
    web, _ = web_config
    # Every normal launch briefly spawns XtUpdate.exe and it exits on its own:
    # the guard must not scan or kill anything during the delay window.
    calls = {"n": 0}
    def fake_update_processes():
        calls["n"] += 1
        return []
    monkeypatch.setattr(web, "_qmt_auto_login_update_processes", fake_update_processes)
    monkeypatch.setattr(web.subprocess, "run",
                        lambda command, **kwargs: pytest.fail("must not kill before delay window"))
    monkeypatch.setattr(web, "_qmt_auto_login_record_update_event",
                        lambda *a, **k: pytest.fail("must not record event before delay window"))
    clock = {"now": 1000.0}
    monkeypatch.setattr(web.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(web.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + seconds))
    web._qmt_auto_login_guard_update(r"C:\QMT\bin.x64", {}, delay=2.0, wait=6.0)
    # No scan happens during the delay window; judging starts only after it.
    assert calls["n"] > 0


def test_update_guard_kills_updater_still_resident_after_delay(web_config, monkeypatch):
    web, _ = web_config
    snapshots = [[], [{"pid": 7799, "name": "XtUpdate.exe"}], [], []]
    calls = {"n": 0}
    def fake_update_processes():
        index = min(calls["n"], len(snapshots) - 1)
        calls["n"] += 1
        return snapshots[index]
    monkeypatch.setattr(web, "_qmt_auto_login_update_processes", fake_update_processes)
    killed = []
    monkeypatch.setattr(web, "_qmt_auto_login_kill_update_processes", lambda rows: (
        killed.append(rows)
        or {"detected": True, "auto_exited": False, "killed": True, "pids": [7799],
            "results": [], "message": "检测到 QMT 更新进程 XtUpdate.exe 阻塞自动登录，已强制结束；为保证流程不阻塞，本次更新已跳过，请及时手动更新 QMT"}
    ))
    recorded = []
    monkeypatch.setattr(web, "_qmt_auto_login_record_update_event",
                        lambda bin_dir, row, event: recorded.append(event))
    clock = {"now": 1000.0}
    monkeypatch.setattr(web.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(web.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + seconds))
    web._qmt_auto_login_guard_update(r"C:\QMT\bin.x64", {}, delay=2.0, wait=6.0)
    assert len(killed) >= 1
    assert recorded and recorded[0]["killed"] is True
    assert recorded[0]["pids"] == [7799]


def test_apply_for_account_starts_update_guard_and_records_event(web_config, monkeypatch, tmp_path):
    web, config = web_config
    qmt_bin = tmp_path / "QMT" / "bin.x64"
    qmt_bin.mkdir(parents=True)
    (qmt_bin / "XtItClient.exe").write_bytes(b"")
    row = config.save_account_config(
        "8885060548",
        qmt_dir=str(qmt_bin.parent),
        qmt_auto_login={"enabled": True, "restart_times": ["08:00"]},
    )
    monkeypatch.setattr(web, "_qmt_auto_login_stop_xtitclient",
                        lambda row, paths, session=None: {"requested_pids": [1]})
    monkeypatch.setattr(web, "_qmt_auto_login_wait_processes_exit", lambda bin_dir, **kwargs: [])
    monkeypatch.setattr(web, "_qmt_auto_login_start_xtitclient",
                        lambda paths, use_link=False: {"pid": 4321, "bin_dir": paths["bin_dir"]})
    monkeypatch.setattr(web, "_qmt_auto_login_processes",
                        lambda path: [{"pid": 4321, "name": "XtItClient.exe"}])
    monkeypatch.setattr(web, "_qmt_auto_login_start_update_guard",
                        lambda bin_dir, row=None: {"started": True, "key": bin_dir})
    monkeypatch.setattr(web, "QMT_UPDATE_EVENTS", {})
    result = web.qmt_auto_login_restart_for_account(row, reason="manual")
    assert result["restarted"] is True
    assert result["update_guard"]["started"] is True
    # The launch response is async now: the skipped-update event arrives from
    # the background guard through the recorded event/session path.
    event = {
        "detected": True, "auto_exited": False, "killed": True, "pids": [7788],
        "results": [{"pid": 7788, "returncode": 0}],
        "message": "检测到 QMT 更新进程 XtUpdate.exe 阻塞自动登录，已强制结束；为保证流程不阻塞，本次更新已跳过，请及时手动更新 QMT",
    }
    web._qmt_auto_login_record_update_event(str(qmt_bin), row, event)
    session = web._qmt_auto_login_saved_session(row)
    assert session.get("update_skipped") is True
    assert "跳过" in (session.get("update_message") or "")


def test_start_qmt_processes_for_request_reports_update_notice(web_config, monkeypatch, tmp_path):
    web, _ = web_config
    qmt_bin = tmp_path / "QMT" / "bin.x64"
    qmt_bin.mkdir(parents=True)
    (qmt_bin / "XtItClient.exe").write_bytes(b"")
    monkeypatch.setattr(web, "_qmt_auto_login_start_xtitclient",
                        lambda paths, use_link=False: {"pid": 4321, "bin_dir": paths["bin_dir"]})
    monkeypatch.setattr(web, "qmt_process_snapshot", lambda path: {
        "qmt_dir": str(qmt_bin.parent), "running": False, "pids": [], "processes": [],
    })
    monkeypatch.setattr(web, "_qmt_auto_login_start_update_guard",
                        lambda bin_dir, row=None: {"started": True, "key": bin_dir})
    data = web.start_qmt_processes_for_request({"targets": [{"qmt_dir": str(qmt_bin.parent)}]})
    target = data["targets"][0]
    assert target["update_guard"]["started"] is True
    # The skip decision is async (background guard); the start response does
    # not carry it yet.
    assert target["update_skipped"] is False


def test_start_update_guard_starts_once_and_dedups(web_config, monkeypatch):
    web, _ = web_config
    bin_dir = r"C:\QMT\bin.x64"
    started = []
    class FakeThread:
        alive = False
        def __init__(self, target, args, name=None, daemon=False):
            self._target = target
            self._args = args

        def is_alive(self):
            return FakeThread.alive

        def start(self):
            FakeThread.alive = True
            started.append(1)

    monkeypatch.setattr(web.threading, "Thread", FakeThread)
    first = web._qmt_auto_login_start_update_guard(bin_dir)
    assert first["started"] is True
    second = web._qmt_auto_login_start_update_guard(bin_dir)
    assert second["started"] is False
    assert second["reason"] == "already watching"
    assert len(started) == 1


def test_binding_status_snapshot_surfaces_update_skipped_event(web_config, monkeypatch, tmp_path):
    web, config = web_config
    qmt_bin = tmp_path / "QMT" / "bin.x64"
    qmt_bin.mkdir(parents=True)
    (qmt_bin / "XtItClient.exe").write_bytes(b"")
    config._data["account_configs"] = {
        "one": {
            "account_id": "1001",
            "account_type": "STOCK",
            "enabled": True,
            "qmt_dir": str(qmt_bin.parent),
        },
    }
    monkeypatch.setattr(web, "account_route_status", lambda *args, **kwargs: {"monitor": {"ready": True}})
    monkeypatch.setattr(web, "QMT_UPDATE_EVENTS", {
        os.path.normcase(os.path.abspath(str(qmt_bin))): {
            "update_skipped": True,
            "update_message": "检测到 QMT 更新进程 XtUpdate.exe 阻塞自动登录，已强制结束；为保证流程不阻塞，本次更新已跳过，请及时手动更新 QMT",
            "update_check": {"detected": True, "auto_exited": False, "killed": True, "pids": [7799], "message": "skip"},
        },
    })
    snapshot = web.binding_status_snapshot()
    row = snapshot["bindings"][0]
    assert row["qmt_auto_login"]["update_skipped"] is True
    assert "跳过" in row["qmt_auto_login"]["update_message"]
