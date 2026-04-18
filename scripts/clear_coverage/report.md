# Clear-coverage report for `copy_deduplicate`

_Generated 2026-04-18 13:05 UTC._

Source checkouts:
- `bigquery-etl` @ [`0fcd46c`](https://github.com/mozilla/bigquery-etl/commit/0fcd46c61264b283d030d3f93aa3a9df032e6ded)
- `private-bigquery-etl` @ [`df4acc0`](https://github.com/mozilla/private-bigquery-etl/commit/df4acc08ee2991e440877f3bf2db81c069baa097)
- `telemetry-airflow` @ [`6bf3065`](https://github.com/mozilla/telemetry-airflow/commit/6bf30656d0b3e8a4151b6cc378867366f653c5ed)

_Filtered out 43 paused DAG id(s) listed in `paused_dags.txt`._

- Total DAGs analysed (after filter): **204**
- Transitively impacted by copy_deduplicate (via sensors): **105**
- Reachable by recursive clear (marker+sensor matched): **93**
- **Gap (impacted − reachable): 12**

## Gap DAGs (logically depend on copy_deduplicate but clear won't reach them)

| DAG | Schedule | Break upstream | Break downstream | Reason | Detail |
|---|---|---|---|---|---|
| `bqetl_mozilla_vpn_site_metrics` | daily (`0 15 * * *`) | `bqetl_google_analytics_derived_ga4` | `bqetl_mozilla_vpn_site_metrics` | missing_marker_on_upstream | bqetl_google_analytics_derived_ga4 has no ExternalTaskMarker targeting bqetl_mozilla_vpn_site_metrics |
| `probe_scraper` | daily (`0 0 * * *`) | `bqetl_monitoring` | `probe_scraper` | missing_marker_on_upstream | bqetl_monitoring has no ExternalTaskMarker targeting probe_scraper |
| `search_forecasting` | monthly (`30 5 7 * *`) | `bqetl_search_dashboard` | `search_forecasting` | missing_marker_on_upstream | bqetl_search_dashboard has no ExternalTaskMarker targeting search_forecasting |
| `adm_dma_export` | daily (`0 8 * * *`) | `bqetl_search_terms_daily` | `adm_dma_export` | missing_marker_on_upstream | bqetl_search_terms_daily has no ExternalTaskMarker targeting adm_dma_export |
| `adm_export` | daily (`0 8 * * *`) | `bqetl_search_terms_daily` | `adm_export` | missing_marker_on_upstream | bqetl_search_terms_daily has no ExternalTaskMarker targeting adm_export |
| `bqetl_glam_refresh_aggregates` | daily (`0 8 * * *`) | `copy_deduplicate` | `glam_fenix` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting glam_fenix |
| `bqetl_glam_refresh_aggregates_release` | weekly (`0 18 * * 6`) | `copy_deduplicate` | `glam_fenix` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting glam_fenix |
| `firefox_public_data_report` | weekly (`0 1 * * MON`) | `copy_deduplicate` | `firefox_public_data_report` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_main_ping'] but firefox_public_data_report sensor task_ids (against copy_deduplicate)=['wait_for_main_ping'] |
| `glam_fenix` | daily (`0 2 * * *`) | `copy_deduplicate` | `glam_fenix` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting glam_fenix |
| `glam_fenix_release` | weekly (`0 10 * * 6`) | `copy_deduplicate` | `glam_fenix` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting glam_fenix |
| `glam_fog` | daily (`0 2 * * *`) | `copy_deduplicate` | `glam_fog` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting glam_fog |
| `glam_fog_release` | weekly (`0 10 * * 6`) | `copy_deduplicate` | `glam_fog` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting glam_fog |

## Direct sensors on copy_deduplicate that are NOT in its marker sets

These DAGs have an `ExternalTaskSensor` pointing at `copy_deduplicate`, but `copy_deduplicate.py` has no corresponding `ExternalTaskMarker` for them. Fix by adding the DAG to the appropriate `downstream_dependencies` set in `telemetry-airflow/dags/copy_deduplicate.py`.

| DAG | File | Schedule | Sensor task_id | Upstream task_id |
|---|---|---|---|---|
| `firefox_public_data_report` | `telemetry-airflow/dags/firefox_public_data_report.py` | `0 1 * * MON` | `wait_for_main_ping` | `copy_deduplicate_main_ping` |
| `glam_fenix` | `telemetry-airflow/dags/glam_fenix.py` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `glam_fog` | `telemetry-airflow/dags/glam_fog.py` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |

## Task-level sensor gaps (reachable DAGs with uncovered sensors)

These are sensor tasks *inside* reachable DAGs whose upstream DAG does not publish a matching `ExternalTaskMarker`. Recursive clear will not clear these sensor tasks, so anything downstream of them **within the DAG** that doesn't also depend on a cleared sensor will retain stale state. For a full backfill, each of these needs to be cleared by hand (or fixed by adding a marker upstream).

| Downstream DAG | Schedule | Sensor task_id | Upstream DAG | Upstream task_id | DAG in top-level gap? |
|---|---|---|---|---|---|
| `bqetl_mozilla_vpn_site_metrics` | `0 15 * * *` | `wait_for_wait_for_wmo_events_table` | `bqetl_google_analytics_derived_ga4` | `wait_for_wmo_events_table` | **YES** |
| `firefox_public_data_report` | `0 1 * * MON` | `wait_for_clients_last_seen` | `bqetl_main_summary` | `telemetry_derived__clients_last_seen__v1` | **YES** |
| `probe_scraper` | `0 0 * * *` | `wait_for_table_partition_expirations` | `bqetl_monitoring` | `monitoring_derived__table_partition_expirations__v1` | **YES** |
| `catalyst` | `0 4 * * *` | `wait_for_search_clients_daily` | `bqetl_search` | `search_derived__search_clients_daily__v8` | no |
| `search_forecasting` | `30 5 7 * *` | `wait_for_search_dashboard` | `bqetl_search_dashboard` | `search_derived__search_revenue_levers_daily__v1` | **YES** |
| `adm_dma_export` | `0 8 * * *` | `wait_for_adm_daily_dma_aggregates` | `bqetl_search_terms_daily` | `search_terms_derived__adm_daily_dma_aggregates__v1` | **YES** |
| `adm_export` | `0 8 * * *` | `wait_for_adm_daily_aggregates` | `bqetl_search_terms_daily` | `search_terms_derived__adm_daily_aggregates__v1` | **YES** |
| `firefox_public_data_report` | `0 1 * * MON` | `wait_for_main_ping` | `copy_deduplicate` | `copy_deduplicate_main_ping` | **YES** |
| `glam_fenix` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `glam_fog` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |

## Non-daily DAGs in the impacted set

copy_deduplicate runs daily. Downstream DAGs on non-daily schedules (hourly, weekly, `@once`, etc.) are where marker generation is trickiest — flag for manual review regardless of whether they appear in the gap.

| DAG | Schedule | Class | In gap? |
|---|---|---|---|
| `bqetl_cohort_daily_churn` | `50 14 * * 7` | weekly | no |
| `bqetl_desktop_mobile_search_monthly` | `0 5 2 * *` | monthly | no |
| `bqetl_glam_refresh_aggregates_release` | `0 18 * * 6` | weekly | **YES** |
| `bqetl_shredder_impact_measurement` | `40 12 * * 7` | weekly | no |
| `firefox_public_data_report` | `0 1 * * MON` | weekly | **YES** |
| `glam_fenix_release` | `0 10 * * 6` | weekly | **YES** |
| `glam_fog_release` | `0 10 * * 6` | weekly | **YES** |
| `private_bqetl_ads_monthly` | `0 5 1 * *` | monthly | no |
| `private_bqetl_ads_quarterly` | `0 5 1 1,4,7,10 *` | non-daily (0 5 1 1,4,7,10 *) | no |
| `search_forecasting` | `30 5 7 * *` | monthly | **YES** |

## Caveats

- **Table-level data dependencies are invisible here.** A DAG that reads a `*_stable` table populated by copy_deduplicate without any `ExternalTaskSensor` will not appear in either `impacted` or `reachable`. Catching those requires parsing bqetl SQL under `bigquery-etl/sql/` and is out of scope for this pass.
- `ExternalTaskSensor` with unusual `execution_delta` (e.g. `timedelta(days=-6)` for weekly DAGs) is counted as a sensor here; whether clearing will actually align timestamps correctly is a separate concern.
