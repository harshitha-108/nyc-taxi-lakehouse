"""Open City Pulse with its local chat service for one Power BI session."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import shutil
import socket
import stat
import tempfile
import threading
import time
import urllib.request
from ctypes import wintypes
from pathlib import Path

from chatbot.server import make_server

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
REPORT_NAME = "NYC Taxi - City Pulse Showcase"
REPORT_PATH = REPOSITORY_ROOT / "powerbi" / f"{REPORT_NAME}.pbip"
LOCAL_REPORT_DIRECTORY = REPOSITORY_ROOT / "powerbi" / ".local" / "p"
LOCAL_REPORT_PATH = LOCAL_REPORT_DIRECTORY / REPORT_PATH.name
CSV_DIRECTORY = REPOSITORY_ROOT / "data" / "gold" / "powerbi"
REQUIRED_EXPORTS = (
    "daily_trip_metrics.csv",
    "hourly_demand.csv",
    "payment_type_summary.csv",
    "pickup_zone_performance.csv",
    "takeaway_breakdown.csv",
)
CHAT_PORT = 8765  # The embedded Power BI visual calls this port.
STARTUP_TIMEOUT_SECONDS = 180
REPORT_CLOSE_GRACE_SECONDS = 10
LOCAL_MODEL = "qwen2.5:1.5b"


class LauncherError(RuntimeError):
    """Explain a local setup problem without a Python traceback."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def _detect_local_model() -> str | None:
    """Use only an installed model on Ollama's loopback API; never fetch a cloud model."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    try:
        with opener.open("http://127.0.0.1:11434/api/tags", timeout=1) as response:
            raw = response.read(65_537)
        if len(raw) > 65_536:
            return None
        payload = json.loads(raw)
        models = payload.get("models", []) if isinstance(payload, dict) else []
    except (OSError, TimeoutError, ValueError, TypeError):
        return None
    return LOCAL_MODEL if isinstance(models, list) and any(
        isinstance(model, dict) and model.get("name") == LOCAL_MODEL for model in models
    ) else None


def _report_windows() -> list[int]:
    """Return visible top-level Windows handles for this exact report title."""
    user32 = ctypes.WinDLL("user32", use_last_error=True)
    callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    user32.EnumWindows.argtypes = (callback_type, wintypes.LPARAM)
    user32.EnumWindows.restype = wintypes.BOOL
    user32.IsWindowVisible.argtypes = (wintypes.HWND,)
    user32.IsWindowVisible.restype = wintypes.BOOL
    user32.GetWindowTextLengthW.argtypes = (wintypes.HWND,)
    user32.GetWindowTextLengthW.restype = ctypes.c_int
    user32.GetWindowTextW.argtypes = (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
    user32.GetWindowTextW.restype = ctypes.c_int
    found: list[int] = []

    def visit(window: int, _unused: int) -> bool:
        if not user32.IsWindowVisible(window):
            return True
        length = user32.GetWindowTextLengthW(window)
        if not length:
            return True
        title = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(window, title, len(title))
        if title.value == REPORT_NAME:
            found.append(int(window))
        return True

    if not user32.EnumWindows(callback_type(visit), 0):
        raise LauncherError("Could not inspect Power BI report windows")
    return found


def _preflight() -> None:
    if os.name != "nt":
        raise LauncherError("The one-click report launcher is for Windows Power BI Desktop")
    if _report_windows():
        raise LauncherError(
            "City Pulse is already open. Close it, then reopen it with "
            "Open City Pulse.cmd so the local report copy and chat start together."
        )
    if not REPORT_PATH.is_file():
        raise LauncherError(f"Power BI project not found: {REPORT_PATH}")
    missing = [name for name in REQUIRED_EXPORTS if not (CSV_DIRECTORY / name).is_file()]
    if missing:
        raise LauncherError(
            "Export the Power BI marts before opening chat. Missing: " + ", ".join(missing)
        )
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(0.5)
        if connection.connect_ex(("127.0.0.1", CHAT_PORT)) == 0:
            raise LauncherError(
                "Port 8765 is already in use. Stop the existing chat service, then "
                "open City Pulse with this launcher so it can close the service safely."
            )


def _set_local_csv_root(model_path: Path) -> None:
    """Change the local model's CSV parameter without touching its other fields."""
    try:
        original = model_path.read_text(encoding="utf-8")
        definition = json.loads(original)
        expressions = definition["model"]["expressions"]
        parameters = [item for item in expressions
                      if isinstance(item, dict) and item.get("name") == "CsvRootPath"]
        if len(parameters) != 1:
            raise LauncherError("Local Power BI model must have one CsvRootPath parameter")
        parameter = parameters[0]
        current = parameter.get("expression")
        if not isinstance(current, str):
            raise LauncherError("Local CsvRootPath parameter is not a text expression")
        match = re.fullmatch(r'"(?:[^"]|"")*"(?P<meta>\s+meta\s+\[.*\])', current, re.DOTALL)
        if not match:
            raise LauncherError(
                "Local CsvRootPath is not a simple Power Query text parameter; "
                "its expression was left unchanged."
            )
        # In M strings, double quotes and # escape as "" and #(#), respectively.
        csv_path = str(CSV_DIRECTORY).replace("#", "#(#)").replace('"', '""')
        updated = f'"{csv_path}"{match.group("meta")}'
        if current == updated:
            return
        parameter["expression"] = updated
        contents = json.dumps(definition, ensure_ascii=False, indent=2)
        if original.endswith("\n"):
            contents += "\n"
        with tempfile.TemporaryDirectory(
            prefix=".model-bim-", dir=model_path.parent
        ) as temporary_directory:
            temporary_path = Path(temporary_directory) / "model.bim"
            temporary_path.write_text(contents, encoding="utf-8")
            os.replace(temporary_path, model_path)
    except (KeyError, TypeError, ValueError) as exc:
        raise LauncherError(f"Could not read the local Power BI model: {exc}") from exc


def _is_link_or_reparse(path: Path) -> bool:
    if not os.path.lexists(path):
        return False
    attributes = getattr(path.lstat(), "st_file_attributes", 0)
    return path.is_symlink() or bool(
        attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _check_local_path() -> None:
    """Keep local model writes inside this project's ordinary .local folder."""
    local_parent = LOCAL_REPORT_DIRECTORY.parent
    expected_parent = REPORT_PATH.parent.resolve() / ".local"
    if (_is_link_or_reparse(local_parent)
            or local_parent.resolve() != expected_parent
            or _is_link_or_reparse(LOCAL_REPORT_DIRECTORY)):
        raise LauncherError(
            f"Local City Pulse path is a link or outside this project: "
            f"{LOCAL_REPORT_DIRECTORY}. Review it before launching."
        )
    if (os.path.lexists(LOCAL_REPORT_DIRECTORY)
            and LOCAL_REPORT_DIRECTORY.resolve() !=
            expected_parent / LOCAL_REPORT_DIRECTORY.name):
        raise LauncherError(
            f"Local City Pulse path resolves outside its folder: "
            f"{LOCAL_REPORT_DIRECTORY}. Review it before launching."
        )


def _check_local_project_files(paths: tuple[Path, ...]) -> None:
    """Reject linked project files that could redirect a save into the source."""
    local_root = LOCAL_REPORT_DIRECTORY.resolve()
    for path in paths:
        if _is_link_or_reparse(path) or not path.resolve().is_relative_to(local_root):
            raise LauncherError(
                f"Local City Pulse project contains a linked file or folder: {path}. "
                "Review it before launching."
            )


def _prepare_local_report() -> Path:
    """Create the ignored working project once, retaining its cache and later edits."""
    _check_local_path()
    model_name = f"{REPORT_NAME}.SemanticModel"
    report_name = f"{REPORT_NAME}.Report"
    if not LOCAL_REPORT_DIRECTORY.exists():
        LOCAL_REPORT_DIRECTORY.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="c", dir=LOCAL_REPORT_DIRECTORY.parent
        ) as staging_directory:
            staged_project = Path(staging_directory)
            shutil.copy2(REPORT_PATH, staged_project / REPORT_PATH.name)
            for sibling in (report_name, model_name):
                shutil.copytree(REPORT_PATH.parent / sibling, staged_project / sibling)
            _set_local_csv_root(staged_project / model_name / "model.bim")
            try:
                staged_project.rename(LOCAL_REPORT_DIRECTORY)
            except OSError:
                # Another launcher may have completed the initial copy first.
                if not LOCAL_REPORT_DIRECTORY.is_dir():
                    raise
    required = (
        LOCAL_REPORT_PATH,
        LOCAL_REPORT_DIRECTORY / report_name / "definition.pbir",
        LOCAL_REPORT_DIRECTORY / model_name / "model.bim",
    )
    if not all(path.is_file() for path in required):
        raise LauncherError(
            f"Local City Pulse project is incomplete: {LOCAL_REPORT_DIRECTORY}. "
            "Review it before reopening the report."
        )
    _check_local_path()
    _check_local_project_files((
        LOCAL_REPORT_PATH,
        LOCAL_REPORT_DIRECTORY / report_name,
        required[1],
        LOCAL_REPORT_DIRECTORY / model_name,
        required[2],
    ))
    _set_local_csv_root(LOCAL_REPORT_DIRECTORY / model_name / "model.bim")
    return LOCAL_REPORT_PATH


def _create_server():
    overrides = {
        "CHAT_ALLOWED_ORIGINS": "https://ms-pbi.pbi.microsoft.com,null",
        "CHAT_AUTH_MODE": "local_public",
        "CHAT_DATA_SOURCE": "csv",
        "CHAT_CSV_DIR": str(CSV_DIRECTORY),
    }
    previous = {name: os.environ.get(name) for name in overrides}
    try:
        os.environ.update(overrides)
        return make_server(CHAT_PORT)
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _wait_for_report() -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if _report_windows():
            return
        time.sleep(1)
    raise LauncherError(
        "Power BI did not open the City Pulse report within three minutes. "
        "Check that Power BI Desktop is installed and the .pbip file association works."
    )


def _wait_until_report_closes() -> None:
    missing_since: float | None = None
    while True:
        if _report_windows():
            missing_since = None
        elif missing_since is None:
            missing_since = time.monotonic()
        elif time.monotonic() - missing_since >= REPORT_CLOSE_GRACE_SECONDS:
            return
        time.sleep(1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="validate setup without opening Power BI"
    )
    args = parser.parse_args()
    previous_model = os.environ.get("CHAT_OLLAMA_MODEL")
    try:
        _preflight()
        if args.check:
            print("City Pulse launcher is ready: CSVs and port available; report closed.")
            return 0
        local_report = _prepare_local_report()
        if not previous_model:
            local_model = _detect_local_model()
            if local_model:
                os.environ["CHAT_OLLAMA_MODEL"] = local_model
        print("Local AI question interpretation is " + (
            "available." if os.environ.get("CHAT_OLLAMA_MODEL") else
            "unavailable; using data-backed built-in answers."
        ), flush=True)
        server = _create_server()
        thread: threading.Thread | None = None
        try:
            thread = threading.Thread(
                target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True
            )
            try:
                thread.start()
            except RuntimeError as exc:
                raise LauncherError(f"Could not start the local chat thread: {exc}") from exc
            print("Starting City Pulse chat without an access code.", flush=True)
            os.startfile(local_report)
            _wait_for_report()
            print("Report is open. Use Ask City Pulse; this window closes with the report.",
                  flush=True)
            _wait_until_report_closes()
            print("Report closed. Stopping its local chat service.", flush=True)
        finally:
            try:
                if thread is not None and thread.is_alive():
                    server.shutdown()
            finally:
                server.server_close()
                if thread is not None and thread.ident is not None:
                    thread.join(timeout=3)
    except (LauncherError, OSError) as exc:
        print(f"City Pulse could not start: {exc}", flush=True)
        return 1
    except KeyboardInterrupt:
        print("City Pulse chat stopped.", flush=True)
    finally:
        if previous_model is None:
            os.environ.pop("CHAT_OLLAMA_MODEL", None)
        else:
            os.environ["CHAT_OLLAMA_MODEL"] = previous_model
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
