#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Isolated filesystem/transaction tests; the manager is simulated, not Linux proof."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("user_service", Path(__file__).with_name("user-service.py"))
service = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(service)


class UpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.home = root / "home"
        self.home.mkdir()
        self.repo = root / "package"
        for folder in ("bin", "chains", "systemd"):
            (self.repo / folder).mkdir(parents=True)
        for name in service.FILES:
            folder = "systemd" if name in service.UNITS else "chains" if name in service.CHAINS else "bin"
            path = self.repo / folder / name
            path.write_text(json.dumps({"id": name}) if name in service.CHAINS else "old " + name + "\n")
            path.chmod(0o755 if folder == "bin" else 0o644)
        self.bins = self.repo / "bin"
        self.states = {unit: {"active": False, "enabled": "disabled"} for unit in service.UNITS}
        self.stops = 0
        self.drop_ins = ""
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {"HOME": str(self.home), "XDG_CONFIG_HOME": str(self.home / ".config")}).start()
        patch.object(service, "REPO", self.repo).start()
        patch.object(service.subprocess, "check_output", self.manager_show).start()
        patch.object(service.subprocess, "run", self.manager_run).start()
        patch.object(service, "health_check", self.health).start()
        service.install(self.bins)
        self.bundle, self.files, _ = service.layout()
        self.extra = self.bundle / "personal-note"
        self.extra.write_text("not installer-owned\n")
        self.state = self.home / ".local/state/smolhost/checkpoint"
        self.state.parent.mkdir(parents=True)
        self.state.write_bytes(b"private checkpoint, unchanged by installer")
        self.original = self.snapshot()
        for name in ("smolhost", "smolhostd"):
            (self.bins / name).write_text("new " + name + "\n")

    def manager_show(self, argv, **kwargs):
        if "--property=LoadState" in argv:
            return "not-found\nnot-found\n"
        unit = argv[3]
        state = self.states[unit]
        path = service.layout()[1][unit]
        active = "active" if state["active"] else "inactive"
        if unit == "smolhost.service" and getattr(self, "transition", False):
            active = "activating"
        return (f"ActiveState={active}\n"
                f"UnitFileState={state['enabled']}\nFragmentPath={path}\nDropInPaths={self.drop_ins}\n")

    def manager_run(self, argv, **kwargs):
        action = argv[2]
        units = [unit for unit in argv[3:] if unit in service.UNITS]
        if action == "stop":
            self.transition = False
            self.stops += 1
            for unit in units:
                self.states[unit]["active"] = False
        elif action == "start":
            for unit in units:
                self.states[unit]["active"] = True
                if unit == "smolhost.service":
                    self.states["smolhost.socket"]["active"] = True
        elif action == "disable":
            for unit in units:
                self.states[unit] = {"active": False, "enabled": "disabled"}
        return subprocess.CompletedProcess(argv, 0)

    def health(self):
        if self.states["smolhost.socket"]["active"]:
            self.states["smolhost.service"]["active"] = True

    def snapshot(self):
        return {name: (path.read_bytes(), path.stat().st_mode & 0o777) if path.exists() else None
                for name, path in service.targets().items()}

    def assert_preserved(self):
        self.assertEqual(self.extra.read_text(), "not installer-owned\n")
        self.assertEqual(self.state.read_bytes(), b"private checkpoint, unchanged by installer")

    def crash(self, operation):
        # Real abrupt process exit: no exception handler/finally can roll back.
        pid = os.fork()
        if pid == 0:
            try:
                operation()
            except BaseException:
                os._exit(82)
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        self.assertTrue(os.WIFEXITED(status))
        self.assertEqual(os.WEXITSTATUS(status), 73)

    def crash_after_first_replacement(self):
        original = service.replace_file

        def replace(directory, record, name, side):
            original(directory, record, name, side)
            if side == "after" and name == "smolhost":
                os._exit(73)

        with patch.object(service, "replace_file", replace):
            service.upgrade(self.bins)

    def test_upgrade_preserves_active_disabled_and_socket_only_policies(self):
        for active, socket, enabled in ((False, False, "disabled"), (True, True, "enabled"),
                                        (False, True, "enabled")):
            with self.subTest(active=active, socket=socket):
                self.states["smolhost.service"] = {"active": active, "enabled": enabled if active else "disabled"}
                self.states["smolhost.socket"] = {"active": socket, "enabled": enabled}
                before = copy.deepcopy(self.states)
                service.upgrade(self.bins)
                self.assertEqual(self.states, before)
                for name in ("smolhost", "smolhostd"):
                    self.assertEqual(self.files[name].read_bytes(), (self.bins / name).read_bytes())
                checksums = json.loads((self.bundle / service.RECEIPT).read_text())
                self.assertEqual(checksums, {name: service.digest(path) for name, path in self.files.items()})
                self.assertFalse(service.transaction_path().exists())
                self.assert_preserved()

    def test_modified_owned_file_is_preserved_without_stopping(self):
        self.files["smolhostd"].write_text("my local build")
        before = self.snapshot()
        with self.assertRaises(RuntimeError):
            service.upgrade(self.bins)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.stops, 0)
        self.assert_preserved()

    def test_symlinked_owned_file_does_not_overwrite_its_target(self):
        self.files["smolhostd"].unlink()
        self.files["smolhostd"].symlink_to(self.state)
        with self.assertRaises(RuntimeError):
            service.upgrade(self.bins)
        self.assertTrue(self.files["smolhostd"].is_symlink())
        self.assertEqual(self.stops, 0)
        self.assert_preserved()

    def test_custom_unit_drop_ins_are_preserved_before_stopping(self):
        self.drop_ins = "/custom/override.conf"
        with self.assertRaises(RuntimeError):
            service.upgrade(self.bins)
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(self.stops, 0)

    def test_new_file_collision_is_not_adopted_or_overwritten(self):
        receipt = self.bundle / service.RECEIPT
        checksums = json.loads(receipt.read_text())
        del checksums[service.CHAINS[-1]]
        receipt.write_text(json.dumps(checksums))
        before = self.snapshot()
        with self.assertRaises(RuntimeError):
            service.upgrade(self.bins)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.stops, 0)

    def test_health_failure_restores_exact_files_receipt_and_running_state(self):
        self.states = {unit: {"active": True, "enabled": "enabled"} for unit in service.UNITS}
        before = copy.deepcopy(self.states)

        def health():
            if self.files["smolhostd"].read_text().startswith("new"):
                self.transition = True
                raise RuntimeError("candidate cannot start")
            self.health()

        with patch.object(service, "health_check", health):
            with self.assertRaises(RuntimeError):
                service.upgrade(self.bins)
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(self.states, before)
        self.assertFalse(service.transaction_path().exists())
        self.assert_preserved()

    def test_abrupt_exit_between_replacements_is_recoverable(self):
        self.crash(self.crash_after_first_replacement)
        self.assertNotEqual(self.snapshot(), self.original)
        self.assertTrue(service.transaction_path().exists())
        service.recover()
        self.assertEqual(self.snapshot(), self.original)
        self.assertFalse(service.transaction_path().exists())
        self.assert_preserved()

    def test_recovery_refuses_changes_made_after_interruption(self):
        self.crash(self.crash_after_first_replacement)
        self.files["smolhostd"].write_text("user work after the crash")
        before = self.snapshot()
        with self.assertRaises(RuntimeError):
            service.recover()
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.stops, 0)
        self.assertTrue(service.transaction_path().exists())

    def test_corrupted_backup_cannot_replace_installed_files(self):
        self.crash(self.crash_after_first_replacement)
        (service.transaction_path() / "before/smolhostd").write_text("damaged backup")
        before = self.snapshot()
        with self.assertRaises(RuntimeError):
            service.recover()
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.stops, 0)

    def test_committed_upgrade_survives_interrupted_cleanup(self):
        def operation():
            def cleanup(directory, record):
                (directory / "before/smolhost").unlink()
                os._exit(73)
            with patch.object(service, "cleanup_transaction", cleanup):
                service.upgrade(self.bins)
        self.crash(operation)
        installed = self.snapshot()
        service.recover()
        self.assertEqual(self.snapshot(), installed)
        self.assertEqual(self.files["smolhost"].read_bytes(), (self.bins / "smolhost").read_bytes())
        self.assertFalse(service.transaction_path().exists())

    def test_rolled_back_upgrade_survives_interrupted_cleanup(self):
        def operation():
            def health():
                if self.files["smolhostd"].read_text().startswith("new"):
                    raise RuntimeError("candidate failed")
                self.health()
            def cleanup(directory, record):
                (directory / "before/smolhost").unlink()
                os._exit(73)
            self.states = {unit: {"active": True, "enabled": "disabled"} for unit in service.UNITS}
            with patch.object(service, "health_check", health), patch.object(service, "cleanup_transaction", cleanup):
                service.upgrade(self.bins)
        self.crash(operation)
        self.assertEqual(self.snapshot(), self.original)
        service.recover()
        self.assertEqual(self.snapshot(), self.original)
        self.assertFalse(service.transaction_path().exists())

    def test_uninstall_after_upgrade_preserves_unowned_files_and_state(self):
        service.upgrade(self.bins)
        service.uninstall()
        self.assertTrue(all(not path.exists() for path in service.targets().values()))
        self.assert_preserved()


if __name__ == "__main__":
    unittest.main()
