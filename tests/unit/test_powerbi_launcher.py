"""Keep the Power BI working project separate from its portable source."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import open_powerbi_chat as launcher


class LocalPowerBIProjectTests(unittest.TestCase):
    def setUp(self) -> None:
        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "repo"
        powerbi = root / "powerbi"
        powerbi.mkdir(parents=True)
        source_project = powerbi / f"{launcher.REPORT_NAME}.pbip"
        source_project.write_text('{"version":"1.0"}', encoding="utf-8")
        report = powerbi / f"{launcher.REPORT_NAME}.Report"
        (report / "CustomVisuals").mkdir(parents=True)
        (report / "CustomVisuals" / "chat.pbiviz").write_bytes(b"embedded visual")
        (report / "report.json").write_text("local layout", encoding="utf-8")
        (report / "definition.pbir").write_text(json.dumps({
            "datasetReference": {"byPath": {"path":
                f"../{launcher.REPORT_NAME}.SemanticModel"}}
        }), encoding="utf-8")
        model_dir = powerbi / f"{launcher.REPORT_NAME}.SemanticModel"
        (model_dir / ".pbi").mkdir(parents=True)
        (model_dir / ".pbi" / "cache.abf").write_bytes(b"imported mart data")
        (model_dir / "definition.pbism").write_text('{"version":"4.0"}', encoding="utf-8")
        source_model = model_dir / "model.bim"
        source_model.write_text(json.dumps({"model": {"expressions": [
            {"name": "CsvRootPath", "kind": "m", "expression":
             '"C:\\path\\to\\repo" meta [IsParameterQuery=true, Type="Text"]'},
            {"name": "Unrelated", "kind": "m", "expression": "1"},
        ], "tables": [{"name": "daily_trip_metrics"}]}}), encoding="utf-8")
        csv_directory = root / "data" / "gold" / "powerbi"
        csv_directory.mkdir(parents=True)
        local_directory = powerbi / ".local" / "p"
        for name, value in (
            ("REPORT_PATH", source_project),
            ("LOCAL_REPORT_DIRECTORY", local_directory),
            ("LOCAL_REPORT_PATH", local_directory / source_project.name),
            ("CSV_DIRECTORY", csv_directory),
        ):
            override = patch.object(launcher, name, value)
            override.start()
            self.addCleanup(override.stop)
        self.root = root
        self.source_project = source_project
        self.source_model = source_model
        self.csv_directory = csv_directory

    def local_model(self) -> Path:
        return (launcher.LOCAL_REPORT_DIRECTORY /
                f"{launcher.REPORT_NAME}.SemanticModel" / "model.bim")

    def test_first_launch_copies_full_project_and_changes_only_local_parameter(self) -> None:
        source_model_before = self.source_model.read_bytes()
        source_project_before = self.source_project.read_bytes()

        local_report = launcher._prepare_local_report()

        self.assertEqual(local_report, launcher.LOCAL_REPORT_PATH)
        self.assertEqual(local_report.read_bytes(), source_project_before)
        self.assertEqual(self.source_model.read_bytes(), source_model_before)
        local_root = launcher.LOCAL_REPORT_DIRECTORY
        report = local_root / f"{launcher.REPORT_NAME}.Report"
        model = local_root / f"{launcher.REPORT_NAME}.SemanticModel"
        self.assertEqual((report / "CustomVisuals" / "chat.pbiviz").read_bytes(),
                         b"embedded visual")
        self.assertEqual((model / ".pbi" / "cache.abf").read_bytes(), b"imported mart data")
        reference = json.loads((report / "definition.pbir").read_text(encoding="utf-8"))
        self.assertEqual(reference["datasetReference"]["byPath"]["path"],
                         f"../{launcher.REPORT_NAME}.SemanticModel")
        source_definition = json.loads(source_model_before)
        local_definition = json.loads(self.local_model().read_text(encoding="utf-8"))
        self.assertEqual(local_definition["model"]["tables"],
                         source_definition["model"]["tables"])
        self.assertEqual(local_definition["model"]["expressions"][1],
                         source_definition["model"]["expressions"][1])
        self.assertEqual(local_definition["model"]["expressions"][0]["expression"],
                         f'"{self.csv_directory}" meta [IsParameterQuery=true, Type="Text"]')

    def test_repeat_launch_preserves_local_edits_and_cache(self) -> None:
        launcher._prepare_local_report()
        root = launcher.LOCAL_REPORT_DIRECTORY
        report = root / f"{launcher.REPORT_NAME}.Report"
        model = root / f"{launcher.REPORT_NAME}.SemanticModel"
        (report / "report.json").write_text("my edited layout", encoding="utf-8")
        (report / "CustomVisuals" / "chat.pbiviz").write_bytes(b"my edited visual")
        (model / ".pbi" / "cache.abf").write_bytes(b"my refreshed cache")
        model_before = self.local_model().read_bytes()

        self.assertEqual(launcher._prepare_local_report(), launcher.LOCAL_REPORT_PATH)

        self.assertEqual((report / "report.json").read_text(encoding="utf-8"),
                         "my edited layout")
        self.assertEqual((report / "CustomVisuals" / "chat.pbiviz").read_bytes(),
                         b"my edited visual")
        self.assertEqual((model / ".pbi" / "cache.abf").read_bytes(), b"my refreshed cache")
        self.assertEqual(self.local_model().read_bytes(), model_before)

    def test_staging_is_direct_sibling_with_short_path(self) -> None:
        destinations: list[Path] = []
        copytree = launcher.shutil.copytree

        def record_copy(source: Path, destination: Path, *args: object,
                        **kwargs: object) -> Path:
            if isinstance(source, Path) and source.parent == self.source_project.parent:
                destinations.append(destination)
            return copytree(source, destination, *args, **kwargs)

        with patch.object(launcher.shutil, "copytree", side_effect=record_copy):
            launcher._prepare_local_report()

        self.assertEqual(len(destinations), 2)
        stage = destinations[0].parent
        self.assertTrue(all(destination.parent == stage for destination in destinations))
        self.assertEqual(stage.parent, launcher.LOCAL_REPORT_DIRECTORY.parent)
        self.assertTrue(stage.name.startswith("c"))
        self.assertLessEqual(len(stage.name), len("CityPulse"))
        self.assertFalse(stage.exists())
        self.assertTrue(launcher.LOCAL_REPORT_DIRECTORY.is_dir())

    def test_repo_move_updates_only_existing_local_csv_parameter(self) -> None:
        launcher._prepare_local_report()
        report = launcher.LOCAL_REPORT_DIRECTORY / f"{launcher.REPORT_NAME}.Report"
        (report / "report.json").write_text("my edited layout", encoding="utf-8")
        moved_csv_directory = self.root / "moved repo" / "data" / "gold" / "powerbi"
        with patch.object(launcher, "CSV_DIRECTORY", moved_csv_directory):
            source_before = self.source_model.read_bytes()
            launcher._prepare_local_report()

        self.assertEqual((report / "report.json").read_text(encoding="utf-8"),
                         "my edited layout")
        self.assertEqual(self.source_model.read_bytes(), source_before)
        expressions = json.loads(self.local_model().read_text(encoding="utf-8"))[
            "model"]["expressions"]
        self.assertTrue(expressions[0]["expression"].startswith(
            f'"{moved_csv_directory}" meta '))
        self.assertEqual(expressions[1]["expression"], "1")

    def test_linked_local_project_is_rejected_before_model_write(self) -> None:
        launcher._prepare_local_report()
        model_before = self.local_model().read_bytes()

        def linked(path: Path) -> bool:
            return path == launcher.LOCAL_REPORT_DIRECTORY

        with patch.object(launcher, "_is_link_or_reparse", side_effect=linked), \
             patch.object(launcher, "_set_local_csv_root") as setter:
            with self.assertRaisesRegex(launcher.LauncherError, "link or outside"):
                launcher._prepare_local_report()

        setter.assert_not_called()
        self.assertEqual(self.local_model().read_bytes(), model_before)

    def test_linked_local_model_is_rejected_before_model_write(self) -> None:
        launcher._prepare_local_report()
        model_before = self.local_model().read_bytes()

        def linked(path: Path) -> bool:
            return path == self.local_model().parent

        with patch.object(launcher, "_is_link_or_reparse", side_effect=linked), \
             patch.object(launcher, "_set_local_csv_root") as setter:
            with self.assertRaisesRegex(launcher.LauncherError, "linked file or folder"):
                launcher._prepare_local_report()

        setter.assert_not_called()
        self.assertEqual(self.local_model().read_bytes(), model_before)

    def test_failed_initial_copy_does_not_publish_partial_project(self) -> None:
        with patch.object(launcher, "_set_local_csv_root",
                          side_effect=launcher.LauncherError("bad model")):
            with self.assertRaisesRegex(launcher.LauncherError, "bad model"):
                launcher._prepare_local_report()
        self.assertFalse(launcher.LOCAL_REPORT_DIRECTORY.exists())
        self.assertEqual(list(launcher.LOCAL_REPORT_DIRECTORY.parent.glob(
            "c*")), [])
        self.assertTrue(self.source_model.is_file())

    def test_check_mode_does_not_create_local_project(self) -> None:
        with patch.object(sys, "argv", ["open_powerbi_chat.py", "--check"]), \
             patch.object(launcher, "_preflight") as preflight, \
             patch.object(launcher, "_prepare_local_report") as prepare:
            self.assertEqual(launcher.main(), 0)
        preflight.assert_called_once_with()
        prepare.assert_not_called()
        self.assertFalse(launcher.LOCAL_REPORT_DIRECTORY.exists())

    def test_failed_chat_thread_start_closes_server_socket(self) -> None:
        server = Mock()
        thread = Mock()
        thread.start.side_effect = RuntimeError("thread unavailable")
        thread.is_alive.return_value = False
        thread.ident = None
        with patch.object(sys, "argv", ["open_powerbi_chat.py"]), \
             patch.object(launcher, "_preflight"), \
             patch.object(launcher, "_prepare_local_report",
                          return_value=launcher.LOCAL_REPORT_PATH), \
             patch.object(launcher, "_detect_local_model", return_value=None), \
             patch.object(launcher, "_create_server", return_value=server), \
             patch.object(launcher.threading, "Thread", return_value=thread):
            self.assertEqual(launcher.main(), 1)

        server.server_close.assert_called_once_with()
        server.shutdown.assert_not_called()
        self.assertFalse(launcher.LOCAL_REPORT_DIRECTORY.exists())

    def test_preflight_rejects_any_open_city_pulse_window(self) -> None:
        with patch.object(launcher.os, "name", "nt"), \
             patch.object(launcher, "_report_windows", return_value=[123]):
            with self.assertRaisesRegex(launcher.LauncherError,
                                        "Close it, then reopen it"):
                launcher._preflight()


if __name__ == "__main__":
    unittest.main()
