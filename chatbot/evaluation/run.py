"""CLI for the full, observed Ask City Pulse synthetic-question evaluation."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from chatbot.advanced_engine import AdvancedChatEngine
from chatbot.data import CsvSource
from chatbot.evaluation.fixture import write_fixture
from chatbot.evaluation.harness import evaluate_suite


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Save the observed JSON report here")
    parser.add_argument("--strict", action="store_true",
                        help="Exit nonzero if any case fails or needs manual review")
    parser.add_argument("--fail-on-failure", action="store_true",
                        help="Exit nonzero on failed cases; report manual review separately")
    parser.add_argument("--use-local-model", action="store_true",
                        help="Permit installed Ollama to interpret unsupported wording")
    args = parser.parse_args()
    scratch = Path(__file__).resolve().parents[2] / "data" / "state"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as name:
        write_fixture(Path(name))
        engine = AdvancedChatEngine(CsvSource(Path(name)))
        if args.use_local_model:
            report = evaluate_suite(engine)
        else:
            # Repeatable offline mode: classification cannot silently depend on a
            # separately installed model or a running network/localhost service.
            with patch("chatbot.advanced_engine.propose_analytic_sql", return_value=None), \
                    patch("chatbot.engine.plan_question", return_value=None):
                report = evaluate_suite(engine)
    report["model_mode"] = "local_model" if args.use_local_model else "offline_deterministic"
    report["dataset_note"] = "Invented synthetic mart rows; no production TLC scores."
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(f"Saved observed results to {args.output}")
        print(f"Counts: {report['counts']}")
    else:
        # Windows consoles may default to cp1252; ASCII escapes keep the CLI usable.
        print(json.dumps(report, indent=2, ensure_ascii=True))
    return int((args.strict and (report["counts"]["fail"] > 0
                                 or report["counts"]["needs_review"] > 0))
               or (args.fail_on_failure and report["counts"]["fail"] > 0))


if __name__ == "__main__":
    raise SystemExit(main())
