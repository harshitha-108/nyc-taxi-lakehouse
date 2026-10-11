# Power BI City Pulse

The [City Pulse Showcase project](NYC%20Taxi%20-%20City%20Pulse%20Showcase.pbip) is the portable, three-page Power BI Desktop source. Keep the `.pbip`, `.Report`, and `.SemanticModel` items together: the project and report definitions use relative references between these siblings. On Windows, open it through [Open City Pulse.cmd](Open%20City%20Pulse.cmd). The launcher makes a separate, Git-ignored working copy under `powerbi/.local/p/`, sets its CSV path for your PC, and opens that copy. It never overwrites an existing working report or its imported-data cache.

The report uses four aggregated PostgreSQL serving marts exported as local CSVs: `daily_trip_metrics`, `hourly_demand`, `payment_type_summary`, and `pickup_zone_performance`. The export also includes `takeaway_breakdown.csv` for the local chat assistant's borough-scoped hour and payment answers. It does not contain the underlying trip-level TLC files. The three page screenshots and chat example in the root README show the locally loaded January–June 2024 snapshot; values can change after a refresh with different data.

## Open and refresh locally

1. Install Power BI Desktop, then follow the repository's [local pipeline and serving steps](../README.md#run-locally) through PostgreSQL serving publication, including the separate `nyc_taxi_lakehouse.serving.takeaways` command, for the months you want to view.
2. From the repository root, export the four report marts and chat breakdown to the ignored `data/gold/powerbi/` folder:

   ```powershell
   docker compose --profile airflow run --rm --no-deps pipeline python scripts/export_powerbi_marts.py
   ```

3. Double-click [Open City Pulse.cmd](Open%20City%20Pulse.cmd). The **Ask City Pulse** visual is already embedded on page 1; do not import a `.pbiviz` from `cityPulseChat/dist` again. If an import file picker is open, cancel it. The tracked `CsvRootPath` is intentionally a placeholder. The launcher sets the real path **only in the ignored working copy**.
4. On a fresh clone, select **Refresh** in Power BI Desktop after the CSVs have been exported; the source project has no checked-in data cache, so charts may initially be blank. Save the working copy when the refresh finishes. Subsequent launches reuse that same copy and its cache. If the repository moves to another folder, the launcher updates only the local copy's CSV path.

## Use the local report chat

The one-click Windows launcher checks that the five aggregate CSVs and port 8765 are available, starts the code-free local service, and opens the private working `.pbip`. Close any already-open City Pulse report before launching; it will not guess which copy is open. Leave its automatically opened status window alone while using the report; the launcher stops its own chat service when that report closes. If port 8765 is already in use, it exits with an explanation rather than stopping another process. Python must be available on the host.

If you prefer to start only the service yourself, the manual fallback from the repository root is:

```powershell
& .\scripts\start_powerbi_chat.ps1
```

Then open the **working copy** at `powerbi/.local/p/NYC Taxi - City Pulse Showcase.pbip`, not the tracked source. The working copy must already have been created by the launcher at least once.

With either normal launch method, select **Ask City Pulse** in the report. No access code is needed: the service serves only exported, aggregate CSVs on `127.0.0.1`. The chat can compare months or ISO date ranges, explain calculations on demand, and retain limited session context for follow-ups. **New chat** resets the conversation. Asking a question changes only the popup; it does not alter charts or filters. Report slicers still work normally; the chat reads a single selected month on page 1 as context, while boroughs should be named in the question. The bundled custom visual needs Power BI Desktop to permit custom visuals, modal dialogs, and web access to `http://127.0.0.1:8765`; organization policy can block these capabilities. Code-free mode is for public taxi aggregates only: another sandboxed page on the same PC could query those aggregates while the service runs. Stop the manual terminal when finished; the one-click launcher stops itself.

For optional free, on-device question interpretation, install [Ollama for Windows](https://ollama.com/download/windows), run `ollama pull qwen2.5:1.5b` (about 1 GB), and leave Ollama running. Both launchers detect this local model automatically; built-in matching remains available without it. No paid OpenAI API is needed. Answers include mart evidence and remain limited to supported questions over the published aggregates. See the [chat service guide](../chatbot/README.md) for data scope and troubleshooting.

`cityPulseChat/dist` is ignored generated build output and is not needed to open this report. The final visual is already embedded in the `.Report` folder. If you later change its source, build a new `.pbiviz` with `pnpm run package` from `cityPulseChat`; do not re-import it into the current report unless you intend to update the visual.

The tracked portable report bundles Ask City Pulse visual version **1.0.0.10**, including the new calculation disclosure, reset, and clearer errors. The one-click launcher preserves an **existing** ignored working copy, including any edits or cache; it does not silently overwrite its older custom visual. To inspect the new visual locally, open the portable `.pbip` in Power BI Desktop or explicitly update the visual in a disposable copy first, then promote only reviewed changes. Do not delete an edited `.local` report to force an update. Static bundle/version tests and TypeScript lint/typecheck run in CI, but Power BI Desktop rendering and dialog interaction still require a manual check.

For the optional protected, model-proposed PostgreSQL SQL path, see the [chat service guide](../chatbot/README.md#optional-protected-sql-mode). It uses a separate reader login and a temporary per-run access code; the standard one-click CSV experience stays code-free.

The CSV exports, `powerbi/.local/`, `.pbi/cache.abf`, `.pbi/localSettings.json`, credentials, and source datasets must remain outside Git. Edits made later in the working copy do **not** automatically change the portable source; if you want to publish new visual changes, review and promote the report definitions deliberately, and keep personal paths out of `model.bim`. This local Power BI report is not published to the Power BI service.
