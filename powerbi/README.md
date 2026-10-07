# Power BI City Pulse

The [City Pulse Showcase project](NYC%20Taxi%20-%20City%20Pulse%20Showcase.pbip) is an editable, three-page Power BI Desktop report. Keep the `.pbip`, `.Report`, and `.SemanticModel` items together: the project and report definitions use relative references between these siblings.

The report uses four aggregated PostgreSQL serving marts exported as local CSVs: `daily_trip_metrics`, `hourly_demand`, `payment_type_summary`, and `pickup_zone_performance`. It does not contain the underlying trip-level TLC files. The three page screenshots in the root README show the locally loaded January–June 2024 snapshot; values can change after a refresh with different data.

## Open and refresh locally

1. Install Power BI Desktop, then follow the repository's [local pipeline and serving steps](../README.md#run-locally) through PostgreSQL serving publication for the months you want to view.
2. From the repository root, export the four serving marts to the ignored `data/gold/powerbi/` folder:

   ```powershell
   docker compose --profile airflow run --rm --no-deps pipeline python scripts/export_powerbi_marts.py
   ```

3. Open `NYC Taxi - City Pulse Showcase.pbip` in Power BI Desktop. In Power Query's **Manage Parameters**, set `CsvRootPath` to the absolute path of your own `<repo>\data\gold\powerbi` folder, without a trailing slash. The checked-in value is intentionally a placeholder, not a personal file path.
4. Select **Close & Apply** and refresh the report. The first load from a fresh clone has no cached data, so charts are blank until the CSVs are exported and refreshed. Changing the `CsvRootPath` parameter for your machine will modify your local project file; review that change before any later commit.

The CSV exports, `.pbi/cache.abf`, `.pbi/localSettings.json`, credentials, and source datasets must remain outside Git. This local Power BI report is not published to the Power BI service or validated by the repository's CI workflow.
