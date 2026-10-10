# Phase 1 issue validation — 2026-10-10

This checkpoint covers the four repository issues requested before the Ask City Pulse upgrade. It
does not claim a fresh full-data pipeline run, a Power BI Desktop UI automation pass, or a hosted CI
run for unpushed changes.

| Issue | Root cause and change | Evidence and result | Remaining limit |
| --- | --- | --- | --- |
| dbt claim | No dbt project, dependency, model, or CI step exists. The tracked README and status explicitly identify dbt as unimplemented. The public GitHub About description was changed to list the implemented stack without dbt. | Repository search and dependency/workflow audit completed; the new About text was verified on the signed-in repository page. | GitHub topics were not listed; no topic change was needed. |
| Status file | `PROJECT_STATUS.md` stopped at the Phase 14 checkpoint, before the 13-chart Superset dashboard, Power BI report, CSV export, and local chat. It now distinguishes historical phase evidence from the current architecture and limitations. | Checked against dashboard bootstrap/smoke, report files, chat API, and serving code. | The historical Phase 14 benchmark and 155-test result remain historical, not new measurements. |
| Chatbot and CI | Partial/malformed aggregate CSV exports could raise an uncaught exception; the custom visual's direct TypeScript pin conflicted with transitive Node types and lacked a CI check. CSV mart/row validation now produces a source error/503 and non-ready health, the visual pins a compatible TypeScript version, and CI checks its type/lint contract. | Real loopback health/chat/degraded tests, 47 focused local chatbot tests, local Ollama `qwen2.5:1.5b` interpretation and CSV-backed answer, Power BI visual typecheck and lint passed. | Power BI in-report interaction was not automated here. PostgreSQL unavailability is regression-tested with a stub; protected-mode live Power BI access was not exercised. The normal code-free mode is CSV-only and unsuitable for sensitive data. |
| Top Pickup Zones | ECharts queried only ten ranked rows, so Superset's row-limit indicator correctly warned that the result might be truncated. The chart now fetches all current zone groups and displays a ranked ten-row table with in-cell bars. | Current live bootstrap and 13-chart API smoke passed; Top Pickup Zones returned 261 groups. Four dashboard integration tests passed, including two month/borough rankings independently compared with PostgreSQL aggregates. | Signed-in browser visual regression remains manual. The 1,000-group cap exceeds the current 265-zone lookup; revisit it if the source geography changes materially. |

## Validation gate

- `docker compose --profile airflow --profile dashboard config --quiet`: pass.
- Ruff on `src tests airflow scripts chatbot`: pass.
- Service-independent Docker pytest run: 205 passed, 1 skipped because the pipeline image lacks
  Airflow, and 13 deselected (12 service/heavy tests plus one Git-dependent hygiene test). The
  unfiltered run had 204 passes and one environment-only failure because this lean image does not
  contain the `git` executable required by that test. `python scripts/check_repository_hygiene.py`
  on the Windows host, where Git exists, passed. The test itself remains unchanged for hosted CI.
- Focused dashboard and chatbot unit tests: 52 passed. Live Superset integration tests: 4 passed.
- January–June source Parquet metadata matched the committed 19-field Yellow Taxi schema contract
  for each month. The check wrote ignored runtime audit records only.
- January–June Gold/PostgreSQL serving reconciliation: all five marts matched each source month;
  total valid Silver trips were 20,015,099. Read-only example queries succeeded.
- Superset bootstrap updated the existing dashboard in place and retained its saved chart-row order;
  a follow-up API smoke returned data from all 13 charts. The filtered zone-rank test matched
  independent PostgreSQL `SUM(trip_count)` results.
- `pnpm exec tsc --noEmit`, `pnpm exec eslint src`, `git diff --check`, and the host repository
  hygiene check passed. A new GitHub Actions job will run visual typecheck/lint on future pushes or
  pull requests; it has not run remotely for these local changes.

No historical taxi data, Docker volumes, or the Power BI report were deleted or rebuilt for this
checkpoint. No GitHub push was performed.
