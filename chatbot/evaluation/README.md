# Ask City Pulse evaluation

This is an offline, reproducible **synthetic-data** evaluation, not a claim about
the production TLC dataset or a hosted model. `cases.json` contains 60 natural-
language cases across totals, report filters, rankings, comparisons, trends and
anomalies, multi-step analysis, follow-ups, and unsupported or hostile requests.
The invented rows in `fixture.py` reconcile across all five Gold-like marts.

Run the full observed suite from the repository root:

```powershell
python -m chatbot.evaluation.run --output data/state/chat-evaluation.json
```

The default run disables local-model calls to keep it deterministic. Use
`--use-local-model` only to separately assess an installed Ollama model. Use
`--strict` to return a nonzero process exit when any case fails or needs manual
review. `--fail-on-failure` returns nonzero only for failed checks and leaves
manual-review cases visibly pending; CI runs this full deterministic mode.
The small core/safety subset in `tests/unit/test_chatbot_evaluation.py` is a
separate unit-test gate.

Each result records:

- `validity`: nonempty, relevant answer and a valid `answer`/`evidence` contract;
- `execution`: whether the engine completed without an error;
- `numeric_correctness`: expected values in the **visible answer**, with an
  explicit per-case tolerance when rounding is reasonable;
- `safety`: unsupported or injection prompts must decline without data evidence;
- `latency_ms`: measured wall-clock time for all turns in the case.

The summary gives observed pass/fail/manual-review counts and p50/p95/max
latencies. A `needs_review` case (currently qualitative anomaly wording) is not
counted as a pass. The fixture deliberately includes just two days per month;
month-to-month **absolute differences** are checkable, but percentage changes
for incomplete calendar months must be withheld with an explicit caveat. The
fixture also omits March and April 2024, so those months must not be invented.

This suite does not prove PostgreSQL permissions, AST validation, Power BI
rendering, or Ollama availability. Those require their separate security,
integration, and visual checks.
