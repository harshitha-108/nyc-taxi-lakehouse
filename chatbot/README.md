# Power BI report chat service

This service answers questions inside the Power BI report's chat visual. It reads only the project's published, aggregated yellow taxi marts. The default source is the same local CSV export folder used by the Power BI report, so its numbers match a freshly refreshed report. It has no access to trip-level records and does not let a model write SQL.

## Run on this machine

After running `scripts/export_powerbi_marts.py` and refreshing Power BI, double-click `powerbi/Open City Pulse.cmd` on Windows. It starts this service, opens the report if needed, and stops the service it started after the report closes. A small status window stays open automatically; no command or access code needs to be entered. If port 8765 is already in use, the launcher stops with an explanation rather than touching another process.

To start only the service manually, run this from the repository root:

```powershell
& .\scripts\start_powerbi_chat.ps1
```

For optional local AI interpretation on Windows, install [Ollama from its official Windows download](https://ollama.com/download/windows), then run `ollama pull qwen2.5:1.5b` in PowerShell (about 1 GB). Leave Ollama running. Both launchers detect the installed model automatically; if it is unavailable, the built-in question matcher still works. No OpenAI account, API key, or paid API is needed.

The service listens only on `http://127.0.0.1:8765`. Both launch methods enable **code-free local mode** and force the data source to this project's exported, aggregate CSVs. There is no access code to copy into Power BI. Keep the manual terminal open while using the chat and stop it when finished; the one-click launcher handles its own shutdown. Because Power BI Desktop's sandboxed visual sends an opaque `Origin: null`, another sandboxed page on the same PC could query these public taxi aggregates while the code-free service runs. This mode is unsuitable for private or sensitive data. Requests are limited to 30 per minute, and the service cannot use PostgreSQL in this mode. `http://127.0.0.1:8765/health` reports whether the service is ready. The export script writes the report's four marts plus `takeaway_breakdown.csv`, which the chat needs for borough-scoped hour and payment questions. If Power BI is showing an older refresh than the CSV files on disk, refresh the report before comparing results. The service reads CSVs again for each answer, so new exports are visible without restarting it.

Chat questions only append answers inside the popup; they do not change the report's charts or slicers. On page 1, the chat can read a single selected source month as context. The borough slicer is on another page and is not connected to the chat visual, so name a borough in your question when you want a borough-specific answer. Naming another month or borough in a question affects that answer only, not the visible report selection. Closing the popup currently clears its visible conversation.

The popup displays only the conversational answer. The local API still returns evidence identifying the mart and filter context for verification; it is not shown in the chat. `total_revenue` means TLC `total_amount` summed across trips, not accounting revenue. Month selection uses the mart's publication month (`_source_year` and `_source_month`), matching the report's month slicer. Some detailed combinations are unavailable at the published grain: for example, the daily mart cannot identify a busiest day *within a borough*. The chat also declines causes, forecasts, relative dates, and pickup-to-dropoff routes rather than guessing.

For a direct read of the PostgreSQL serving marts instead, use the protected mode by starting `python -m chatbot.server` directly. Install the project's `psycopg2-binary` dependency, set the existing `ANALYTICS_DB_*` environment variables for a host-accessible, read-only database account, set `CHAT_DATA_SOURCE=postgres`, and set `CHAT_ALLOWED_ORIGINS=https://ms-pbi.pbi.microsoft.com,null` if using Power BI Desktop. Protected mode is the default when the server is started directly; its terminal prints a fresh access code that the chat popup requests. The Docker Compose hostname `airflow-postgres` is not reachable from Power BI Desktop on the Windows host; use a host-mapped loopback PostgreSQL port if choosing this mode. Do not put passwords in the report, visual, source files, or Git.

When built-in matching cannot identify a question type, local Ollama can suggest one of the supported types at `http://127.0.0.1:11434/api/generate`. Advanced users can set `CHAT_OLLAMA_MODEL` to another installed local model and `CHAT_OLLAMA_URL` to another loopback endpoint; `:cloud` models are refused. The service validates the suggestion, then fixed code applies filters, calculates, and words answers from published mart values; the model cannot supply facts or SQL. Unsupported questions or details absent from these aggregate marts remain unavailable. `ai_used` shows whether a validated local interpretation was used.

Power BI Desktop's sandboxed visual sends `Origin: null`. The start script allows its browser preflight and chat requests to the code-free, CSV-only service. The server's default protected mode (when run directly) allows only `https://ms-pbi.pbi.microsoft.com`; `CHAT_ALLOWED_ORIGINS` accepts an exact comma-separated list of trusted origins. In protected mode, requests without a browser `Origin` header, such as local command-line checks, still need the access code for `/chat`.

## API

`GET /health` returns `{"status":"ready","source":"csv","auth_required":false,...}` when the normal local service is ready. `POST /chat` accepts JSON without a code in that mode; protected mode requires the per-run code in the `X-City-Pulse-Code` header:

```json
{"question":"Which pickup zone had the most trips?","month":"2024-03","borough":"Manhattan","session_id":"optional"}
```

`month` can be `1`–`12`, `YYYY-MM`, or a month name. `borough` can be Bronx, Brooklyn, Manhattan, Queens, Staten Island, EWR, Unknown, or N/A. Explicit month or borough names in the question take precedence over the report selection. The response includes `answer`, an `evidence` array of metric, value, unit, source, and filters, an echoed or generated `session_id`, and `ai_used`. The session ID does not currently retain conversation history. A missing or incorrect code in protected mode returns HTTP 401; the code-free mode limits requests with HTTP 429. A missing mart returns HTTP 503, and malformed inputs return HTTP 400. Browser preflight does not reveal mart data. The API permits configured Power BI Desktop visual origins via CORS and binds only to loopback. It is intended for local Desktop use, not remote publishing.

Try: “How many trips were there?”, “What was TLC total amount?”, “Which pickup zone had the most trips?”, “What was the busiest hour?”, “Which payment method was most common?”, and “Compare January and February.”

Run its focused tests with `python -m unittest discover -s tests/unit -p test_chatbot.py`.
