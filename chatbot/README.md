# Power BI report chat service

This service answers questions inside the Power BI report's chat visual. It reads only published, aggregated yellow taxi marts, not trip-level records. The normal one-click mode uses the same local CSV exports as the report. A separate, **opt-in protected PostgreSQL mode** permits a local model to *propose* a narrow analytical SQL query; the service validates and recompiles it before any read-only execution. No model-generated numeric answer is trusted.

## Run on this machine

After running `scripts/export_powerbi_marts.py` and refreshing Power BI, double-click `powerbi/Open City Pulse.cmd` on Windows. It starts this service, opens the report if needed, and stops the service it started after the report closes. A small status window stays open automatically; no command or access code needs to be entered. If port 8765 is already in use, the launcher stops with an explanation rather than touching another process.

To start only the service manually, run this from the repository root:

```powershell
& .\scripts\start_powerbi_chat.ps1
```

For optional local AI interpretation on Windows, install [Ollama from its official Windows download](https://ollama.com/download/windows), then run `ollama pull qwen2.5:1.5b` in PowerShell (about 1 GB). Leave Ollama running. Both launchers detect the installed model automatically; if it is unavailable, the built-in question matcher still works. No OpenAI account, API key, or paid API is needed.

The service listens only on `http://127.0.0.1:8765`. Both launch methods enable **code-free local mode** and force the data source to this project's exported, aggregate CSVs. There is no access code to copy into Power BI. Keep the manual terminal open while using the chat and stop it when finished; the one-click launcher handles its own shutdown. Because Power BI Desktop's sandboxed visual sends an opaque `Origin: null`, another sandboxed page on the same PC could query these public taxi aggregates while the code-free service runs. This mode is unsuitable for private or sensitive data. Requests are limited to 30 per minute, and the service cannot use PostgreSQL in this mode. `http://127.0.0.1:8765/health` reports whether the service is ready. The export script writes the report's four marts plus `takeaway_breakdown.csv`, which the chat needs for borough-scoped hour and payment questions. If Power BI is showing an older refresh than the CSV files on disk, refresh the report before comparing results. The service reads CSVs again for each answer, so new exports are visible without restarting it. Missing or malformed mart exports make health non-ready and chat return HTTP 503; rerun the export before retrying.

Chat questions only append answers inside the popup; they do not change the report's charts or slicers. On page 1, the chat can read a single selected source month as context. The borough slicer is on another page and is not connected to the chat visual, so name a borough in your question when you want a borough-specific answer. Naming another month or borough in a question affects that answer only, not the visible report selection. Closing the popup currently clears its visible conversation.

The popup shows the answer and an optional **How this was calculated** explanation; evidence remains in the API response but is not rendered as a distracting source list. `total_revenue` means TLC `total_amount` summed across trips, not fare-only amount or company accounting revenue. Month selection uses the mart's publication month (`_source_year` and `_source_month`). The assistant supports month and ISO date-range comparisons, zone rankings and changes, payment-share differences in percentage points, weekday/weekend patterns, cautious same-weekday anomaly flags, and bounded multi-step questions. A session ID retains only validated, structured referents: metric, answer-backed month candidates, exact dates/date ranges, zone IDs, borough/payment scope, and comparison group. One answer month resolves “that month”; two answer months trigger clarification. A follow-up can switch metrics while keeping a clear period, or compare the current metric with a named month. A day/date-range answer is not silently promoted to an entire month. **New chat** starts a fresh session, and separate popups do not share context. Questions never change report charts or filters. See the [context validation record](../docs/chat-context-validation-2026-10-11.md) for the reproduced issue and test results.

Some combinations do not exist at the published grain: daily borough/zone/payment intersections, zone-level average fares, passenger identities, routes, and causal explanations. A partially observed date range is identified as such, and percentage change is withheld when a comparison period is incomplete or has a zero baseline. Fare averages from Gold are trip-weighted from rounded daily values; see [metric definitions](../chatbot/metrics.py) for exact formulas and limitations. The assistant does not forecast.

### Optional protected SQL mode

This is **not** the normal code-free CSV launcher. It is for local users who want validated model-proposed grouping queries and are willing to set up a dedicated database reader and enter a per-run access code. First publish the six approved marts, install `requirements.txt`, and put a unique `CHAT_READONLY_DB_USER` and `CHAT_READONLY_DB_PASSWORD` in the ignored `.env` (see `.env.example`). Do not reuse the analytics owner or admin credentials. Provision the dedicated role with:

```powershell
docker compose --profile airflow run --rm --no-deps analytics-db-init python scripts/provision_chat_reader.py
```

The script preflights the six marts and role safety, then grants only analytics-schema usage and SELECT on those marts in one transaction. It does not change data, PUBLIC grants, or other roles. For a Windows-host service, the normal Compose hostname is not reachable; start PostgreSQL with the **optional loopback-only** port override below, then set `CHAT_READONLY_DB_HOST=127.0.0.1` and `CHAT_READONLY_DB_PORT=5433` in the ignored `.env`:

```powershell
docker compose -f docker-compose.yml -f docker-compose.chat-postgres.yml --profile airflow up -d --wait airflow-postgres
& .\scripts\start_powerbi_chat_postgres.ps1
```

Start the portable/local Power BI report separately: `Open City Pulse.cmd` intentionally starts its own CSV service and refuses an already occupied port. The protected server prints a fresh access code for the chat popup; the normal CSV launcher never asks for a code. Use only on your own machine, keep the port bound to loopback, and never put credentials in the report, custom visual, committed files, or a public screenshot. The SQL path is disabled unless `CHAT_ENABLE_DYNAMIC_SQL=1`, `CHAT_AUTH_MODE=code`, and `CHAT_DATA_SOURCE=postgres` are set together. If PostgreSQL or the local model is unavailable, the service returns a clear limitation/error and does not fabricate a number.

In normal CSV mode, Ollama can suggest a supported intent, after which fixed code retrieves and calculates the answer. In protected SQL mode, Ollama may additionally propose a SQL SELECT when a supported question needs a new grouping. SQLGlot parses it; the compiler permits one approved analytics mart, a small projection/grouping/predicate grammar, bounded complexity, bind parameters, a yellow-taxi scope, and a 100-row default/200-row hard result cap. It rejects joins, subqueries, CTEs, stars, writes, administrative commands, system tables, unsafe functions, and unrequested filters. The executor uses only the dedicated reader, a read-only transaction, a 3-second statement timeout, and a 1-second lock timeout. The model neither executes SQL nor supplies the final numeric value. This is intentionally narrower than arbitrary conversational SQL; unsupported proposals are refused. `CHAT_OLLAMA_MODEL` selects an already installed local model (default launcher detects `qwen2.5:1.5b`); `CHAT_OLLAMA_URL` must stay loopback and `:cloud` models are refused.

Power BI Desktop's sandboxed visual sends `Origin: null`. The start script allows its browser preflight and chat requests to the code-free, CSV-only service. The server's default protected mode (when run directly) allows only `https://ms-pbi.pbi.microsoft.com`; `CHAT_ALLOWED_ORIGINS` accepts an exact comma-separated list of trusted origins. In protected mode, requests without a browser `Origin` header, such as local command-line checks, still need the access code for `/chat`.

## API

`GET /health` returns `{"status":"ready","source":"csv","auth_required":false,...}` when the normal local service is ready. `POST /chat` accepts JSON without a code in that mode; protected mode requires the per-run code in the `X-City-Pulse-Code` header:

```json
{"question":"Which pickup zone had the most trips?","month":"2024-03","borough":"Manhattan","session_id":"optional"}
```

`month` can be `1`–`12`, `YYYY-MM`, or a month name. `borough` can be Bronx, Brooklyn, Manhattan, Queens, Staten Island, EWR, Unknown, or N/A. Explicit names in the question take precedence over report selection. The response includes `answer`, `evidence`, `calculation`, `session_id`, and `ai_used`; clients may send `{"reset":true}` to start a fresh session. Only a small metric/period/entity context is stored in memory for 30 minutes; the server does not retain full transcripts. A missing or incorrect protected-mode code returns HTTP 401; code-free mode rate-limits with HTTP 429. A missing mart returns HTTP 503, and malformed inputs return HTTP 400. The API binds only to loopback and is not a remote deployment.

Try this connected conversation in one popup after selecting **New chat**: “Which month had the most taxi trips?” → “What were the total tips for that month?” → “How does that compare with April?” The month and then the recorded-tips metric should carry across turns; a question that mentions multiple candidate months can instead trigger a clarification. Other supported starting questions include “Which pickup zone had the most trips?”, “What was the busiest hour?”, and “Which payment method was most common?”

Run focused tests with `python -m unittest discover -s tests/unit -p 'test_chatbot*.py'`. The offline evaluation currently has **64** synthetic cases with independently specified expectations: `python -m chatbot.evaluation.run --fail-on-failure --output data/state/chat-evaluation.json`. The latest context check recorded **105 passing chatbot tests** and **63 pass, 0 fail, 1 needs_review** in that evaluation. The one anomaly case is deliberately marked for manual interpretation; `--strict` also fails on that pending review. See [evaluation guidance](evaluation/README.md), the [context-validation record](../docs/chat-context-validation-2026-10-11.md), and the earlier [Phase 2 validation record](../docs/phase2-validation-2026-10-11.md). Live Ollama and PostgreSQL results are separate from these deterministic scores.
