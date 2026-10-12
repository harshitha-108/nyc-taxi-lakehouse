"""Exercise the Power BI visual's actual loopback JSON contract and session IDs."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.request import ProxyHandler, Request, build_opener

from chatbot.evaluation.fixture import write_fixture
from chatbot.server import make_server


class PowerBIChatContextAPITests(unittest.TestCase):
    def test_visual_payload_retains_and_resets_context(self) -> None:
        scratch = Path(__file__).resolve().parents[2] / "data" / "state"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as name:
            write_fixture(Path(name))
            with patch.dict("os.environ", {
                "CHAT_AUTH_MODE": "local_public",
                "CHAT_DATA_SOURCE": "csv",
                "CHAT_CSV_DIR": name,
            }):
                server = make_server(0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            opener = build_opener(ProxyHandler({}))
            endpoint = f"http://127.0.0.1:{server.server_port}/chat"

            def ask(question: str, session_id: str | None = None) -> dict:
                body = json.dumps({"question": question, "month": "all",
                                   "borough": "all", "session_id": session_id}).encode()
                request = Request(endpoint, data=body, method="POST", headers={
                    "Content-Type": "application/json",
                    "Origin": "https://ms-pbi.pbi.microsoft.com",
                })
                with opener.open(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    return json.load(response)

            try:
                leader = ask("Which month had the most taxi trips?")
                tips = ask("What were the total tips for that month?",
                           leader["session_id"])
                compared = ask("How does that compare with June?", tips["session_id"])
                fresh = ask("What were the total tips for that month?")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(leader["session_id"], tips["session_id"])
            self.assertEqual(tips["session_id"], compared["session_id"])
            self.assertIn("$180.00", tips["answer"])
            self.assertIn("June 2024", compared["answer"])
            self.assertIn("$100.00", compared["answer"])
            self.assertIn("$180.00", compared["answer"])
            self.assertNotEqual(fresh["session_id"], leader["session_id"])
            self.assertIn("clarify", fresh["answer"].lower())


if __name__ == "__main__":
    unittest.main()
