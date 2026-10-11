"""Guard against shipping a chat visual source newer than the embedded PBIP copy."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUID = "cityPulseChat025EE4F52C524921A99C876C602C7504"
SOURCE = ROOT / "powerbi" / "cityPulseChat"
EMBEDDED = ROOT / "powerbi" / "NYC Taxi - City Pulse Showcase.Report" / "CustomVisuals" / GUID


class PowerBiChatBundleTests(unittest.TestCase):
    def test_visual_version_and_new_chat_contract_are_embedded(self) -> None:
        source = json.loads((SOURCE / "pbiviz.json").read_text(encoding="utf-8"))
        package = json.loads((EMBEDDED / "package.json").read_text(encoding="utf-8"))
        resource = json.loads((EMBEDDED / "resources" / f"{GUID}.pbiviz.json")
                              .read_text(encoding="utf-8"))
        version = source["visual"]["version"]
        self.assertEqual(source["version"], version)
        self.assertEqual(package["version"], version)
        self.assertEqual(resource["visual"]["version"], version)
        self.assertEqual(resource["visual"]["guid"], GUID)
        script = resource["content"]["js"]
        self.assertIn("session_id", script)
        self.assertIn("calculation", script)
        self.assertIn("reset", script)


if __name__ == "__main__":
    unittest.main()
