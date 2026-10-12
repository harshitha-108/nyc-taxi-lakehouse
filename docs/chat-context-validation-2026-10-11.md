# Ask City Pulse conversational context validation

## Finding

The user's clarification after asking about unusually low-volume days was **correct**: that answer identified February 13 and January 7, so “that month” did not identify a unique month. The genuine defect was a different follow-up: after a clear May 2024 answer and a May tips answer, “How does that compare with April?” could revert to trip/amount metrics rather than compare recorded tips. Some daily answers also left an older monthly referent active, and a borough follow-up could drop its borough scope.

## Changes

- Session state now records typed, answer-backed referents: metric, month(s), dates, date range(s), borough, zone, payment type, and comparison group. It does not infer context by matching the last month word in an answer.
- A singular “that month” resolves only when the preceding analytical answer has exactly one month at monthly grain. Multi-month and daily answers require clarification; a stated month can resolve a pending clarification.
- Clear follow-ups can change metrics within a period or change periods while retaining the metric. A comparison uses the prior focused month as the subject and the newly named month as the baseline.
- Follow-ups retain the exact date ranges, borough scope, and saved pickup-location ID where those published marts support the metric. A change of zone, multiple candidate zones/months, or a daily-grain filter unavailable at zone grain produces clarification rather than a citywide or whole-month answer. A zone label that maps to multiple location IDs is kept as multiple candidates, not collapsed to one ID.
- Metric parsing prioritizes specific measures (for example, average trip distance) over the generic word “trip”; peer-median anomaly questions remain supported. Unrelated answers replace stale referents. Unsupported metrics are not silently treated as trips. The existing validated calculations, read-only API contract, and report visuals remain unchanged.
- `New chat` starts a new visual session and clears the displayed conversation. A new or unknown session ID creates independent server state.

## Verification

- Chatbot unit and loopback API suite: **105 tests passed** (`python -m unittest discover -s tests/unit -p 'test_chatbot*.py'`). This includes exact-range, Manhattan/zone-scope, ambiguous-zone/month, metric-priority, context-reset, and HTTP session tests. The HTTP test used the Power BI visual's request fields and verified continuity and reset with a new session ID.
- Packaged visual test: **1 passed** (`python -m unittest tests.unit.test_powerbi_chat_bundle -q`).
- Evaluation suite: **63 pass, 0 fail, 1 needs_review** (`python -m chatbot.evaluation.run --fail-on-failure`). The remaining item is a manual anomaly-review case, not a failed assertion.
- In Power BI Desktop with the local CSV chat service, the May → May tips → April comparison returned the expected metric-specific figures. The two-date anomaly answer followed by “that month” asked whether January or February was intended; naming February then returned February tips.
- The saved local working copy was backed up to ignored `data/state/city-pulse-working-pre-v10-20261011.zip`, updated from chat visual 1.0.0.9 to 1.0.0.10, saved, and reopened. `New chat` visibly cleared the conversation; a subsequent pronoun-only tips question asked for clarification instead of reusing May. A byte comparison with the backup confirmed `report.json` and `model.bim` were unchanged. Only the custom visual package and Power BI's local settings changed.

## Three-question demo

1. **Which month had the most taxi trips?** → May 2024, **3,663,653 trips**.
2. **What were the total tips for that month?** → May 2024, **$12,366,400.12** in recorded tips.
3. **How does that compare with April?** → April **$11,347,717.85**; May was **$1,018,682.27 higher (+9.0% versus April)**.

The figures are validated against the report's exported aggregates; the local AI model may interpret unfamiliar phrasing but does not invent numeric answers. Start the local service through `powerbi/Open City Pulse.cmd` before using the chat in Power BI Desktop.
