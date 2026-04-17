# Clear-coverage report for `copy_deduplicate`

_Filtered out 43 paused DAG id(s) listed in `paused_dags.txt`._

- Total DAGs analysed (after filter): **204**
- Transitively impacted by copy_deduplicate (via sensors): **106**
- Reachable by recursive clear (marker+sensor matched): **81**
- **Gap (impacted − reachable): 25**

## Gap DAGs (logically depend on copy_deduplicate but clear won't reach them)

| DAG | Schedule | Break upstream | Break downstream | Reason | Detail |
|---|---|---|---|---|---|
| `bqetl_mozilla_vpn_site_metrics` | daily (`0 15 * * *`) | `bqetl_google_analytics_derived_ga4` | `bqetl_mozilla_vpn_site_metrics` | missing_marker_on_upstream | bqetl_google_analytics_derived_ga4 has no ExternalTaskMarker targeting bqetl_mozilla_vpn_site_metrics |
| `probe_scraper` | daily (`0 0 * * *`) | `bqetl_monitoring` | `probe_scraper` | missing_marker_on_upstream | bqetl_monitoring has no ExternalTaskMarker targeting probe_scraper |
| `search_forecasting` | monthly (`30 5 7 * *`) | `bqetl_search_dashboard` | `search_forecasting` | missing_marker_on_upstream | bqetl_search_dashboard has no ExternalTaskMarker targeting search_forecasting |
| `adm_dma_export` | daily (`0 8 * * *`) | `bqetl_search_terms_daily` | `adm_dma_export` | missing_marker_on_upstream | bqetl_search_terms_daily has no ExternalTaskMarker targeting adm_dma_export |
| `adm_export` | daily (`0 8 * * *`) | `bqetl_search_terms_daily` | `adm_export` | missing_marker_on_upstream | bqetl_search_terms_daily has no ExternalTaskMarker targeting adm_export |
| `bhr_collection` | daily (`0 5 * * *`) | `copy_deduplicate` | `bhr_collection` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but bhr_collection sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |
| `bqetl_accounts_derived` | daily (`30 2 * * *`) | `copy_deduplicate` | `bqetl_accounts_derived` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_accounts_derived |
| `bqetl_ads_hourly` | @hourly (`@hourly`) | `copy_deduplicate` | `bqetl_ads_hourly` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_ads_hourly |
| `bqetl_broken_reports_agg` | daily (`0 6 * * *`) | `copy_deduplicate` | `bqetl_broken_reports_agg` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_broken_reports_agg |
| `bqetl_crashes` | daily (`0 4 * * *`) | `copy_deduplicate` | `bqetl_crashes` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_crashes |
| `bqetl_desktop_installs_v1` | daily (`55 23 * * *`) | `copy_deduplicate` | `bqetl_desktop_installs_v1` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_desktop_installs_v1 |
| `bqetl_firefox_installer_aggregates` | daily (`0 15 * * *`) | `copy_deduplicate` | `bqetl_firefox_installer_aggregates` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_firefox_installer_aggregates |
| `bqetl_glam_refresh_aggregates` | daily (`0 8 * * *`) | `copy_deduplicate` | `glam_fenix` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but glam_fenix sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |
| `bqetl_glam_refresh_aggregates_release` | weekly (`0 18 * * 6`) | `copy_deduplicate` | `glam_fenix` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but glam_fenix sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |
| `bqetl_pageload_v1` | daily (`@daily`) | `copy_deduplicate` | `bqetl_pageload_v1` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_pageload_v1 |
| `bqetl_pocket` | daily (`0 12 * * *`) | `copy_deduplicate` | `bqetl_pocket` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_pocket |
| `bqetl_rust_component_metrics` | daily (`0 3 * * *`) | `copy_deduplicate` | `bqetl_rust_component_metrics` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_rust_component_metrics |
| `bqetl_serp` | daily (`@daily`) | `copy_deduplicate` | `bqetl_serp` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_serp |
| `bqetl_usage_reporting` | daily (`0 4 * * *`) | `copy_deduplicate` | `bqetl_usage_reporting` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_usage_reporting |
| `bqetl_use_counter_analysis` | daily (`0 8 * * *`) | `copy_deduplicate` | `bqetl_use_counter_analysis` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting bqetl_use_counter_analysis |
| `firefox_public_data_report` | weekly (`0 1 * * MON`) | `copy_deduplicate` | `firefox_public_data_report` | missing_marker_on_upstream | copy_deduplicate has no ExternalTaskMarker targeting firefox_public_data_report |
| `glam_fenix` | daily (`0 2 * * *`) | `copy_deduplicate` | `glam_fenix` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but glam_fenix sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |
| `glam_fenix_release` | weekly (`0 10 * * 6`) | `copy_deduplicate` | `glam_fenix` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but glam_fenix sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |
| `glam_fog` | daily (`0 2 * * *`) | `copy_deduplicate` | `glam_fog` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but glam_fog sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |
| `glam_fog_release` | weekly (`0 10 * * 6`) | `copy_deduplicate` | `glam_fog` | sensor_task_id_mismatch | copy_deduplicate marker external_task_ids=['wait_for_copy_deduplicate_all'] but glam_fog sensor task_ids (against copy_deduplicate)=['wait_for_copy_deduplicate'] |

## Direct sensors on copy_deduplicate that are NOT in its marker sets

These DAGs have an `ExternalTaskSensor` pointing at `copy_deduplicate`, but `copy_deduplicate.py` has no corresponding `ExternalTaskMarker` for them. Fix by adding the DAG to the appropriate `downstream_dependencies` set in `telemetry-airflow/dags/copy_deduplicate.py`.

| DAG | File | Schedule | Sensor task_id | Upstream task_id |
|---|---|---|---|---|
| `bhr_collection` | `telemetry-airflow/dags/bhr_collection.py` | `0 5 * * *` | `wait_for_copy_deduplicate` | `copy_deduplicate_all` |
| `bqetl_accounts_derived` | `bigquery-etl/dags/bqetl_accounts_derived.py` | `30 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_ads_hourly` | `bigquery-etl/dags/bqetl_ads_hourly.py` | `@hourly` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_analytics_aggregations` | `bigquery-etl/dags/bqetl_analytics_aggregations.py` | `15 4 * * *` | `wait_for_bq_main_events` | `bq_main_events` |
| `bqetl_analytics_aggregations` | `bigquery-etl/dags/bqetl_analytics_aggregations.py` | `15 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_analytics_aggregations` | `bigquery-etl/dags/bqetl_analytics_aggregations.py` | `15 4 * * *` | `wait_for_event_events` | `event_events` |
| `bqetl_analytics_tables` | `bigquery-etl/dags/bqetl_analytics_tables.py` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_broken_reports_agg` | `bigquery-etl/dags/bqetl_broken_reports_agg.py` | `0 6 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_client_attributes` | `bigquery-etl/dags/bqetl_client_attributes.py` | `40 19 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_crashes` | `bigquery-etl/dags/bqetl_crashes.py` | `0 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_default_browser_aggregates` | `bigquery-etl/dags/bqetl_default_browser_aggregates.py` | `0 22 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_desktop_engagement_model` | `bigquery-etl/dags/bqetl_desktop_engagement_model.py` | `0 5 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_desktop_installs_v1` | `bigquery-etl/dags/bqetl_desktop_installs_v1.py` | `55 23 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_ech_adoption_rate` | `bigquery-etl/dags/bqetl_ech_adoption_rate.py` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_firefox_installer_aggregates` | `bigquery-etl/dags/bqetl_firefox_installer_aggregates.py` | `0 15 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_fx_cert_error_privacy_dashboard` | `bigquery-etl/dags/bqetl_fx_cert_error_privacy_dashboard.py` | `40 16 * * *` | `wait_for_bq_main_events` | `bq_main_events` |
| `bqetl_fx_cert_error_privacy_dashboard` | `bigquery-etl/dags/bqetl_fx_cert_error_privacy_dashboard.py` | `40 16 * * *` | `wait_for_event_events` | `event_events` |
| `bqetl_fx_health_ind_dashboard` | `bigquery-etl/dags/bqetl_fx_health_ind_dashboard.py` | `0 16 * * *` | `wait_for_bq_main_events` | `bq_main_events` |
| `bqetl_fx_health_ind_dashboard` | `bigquery-etl/dags/bqetl_fx_health_ind_dashboard.py` | `0 16 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_fx_health_ind_dashboard` | `bigquery-etl/dags/bqetl_fx_health_ind_dashboard.py` | `0 16 * * *` | `wait_for_copy_deduplicate_main_ping` | `copy_deduplicate_main_ping` |
| `bqetl_fx_health_ind_dashboard` | `bigquery-etl/dags/bqetl_fx_health_ind_dashboard.py` | `0 16 * * *` | `wait_for_event_events` | `event_events` |
| `bqetl_generated_funnels` | `bigquery-etl/dags/bqetl_generated_funnels.py` | `0 5 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_glean_usage` | `bigquery-etl/dags/bqetl_glean_usage.py` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_glean_usage` | `bigquery-etl/dags/bqetl_glean_usage.py` | `0 2 * * *` | `wait_for_telemetry_derived__core_clients_first_seen__v1` | `telemetry_derived__core_clients_first_seen__v1` |
| `bqetl_ltv` | `bigquery-etl/dags/bqetl_ltv.py` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_marketing_analysis` | `bigquery-etl/dags/bqetl_marketing_analysis.py` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_mobile_feature_usage` | `bigquery-etl/dags/bqetl_mobile_feature_usage.py` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_mobile_kpi_metrics` | `bigquery-etl/dags/bqetl_mobile_kpi_metrics.py` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_nimbus_feature_monitoring` | `bigquery-etl/dags/bqetl_nimbus_feature_monitoring.py` | `0 3 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_pageload_v1` | `bigquery-etl/dags/bqetl_pageload_v1.py` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_pocket` | `bigquery-etl/dags/bqetl_pocket.py` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_review_checker` | `bigquery-etl/dags/bqetl_review_checker.py` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_rust_component_metrics` | `bigquery-etl/dags/bqetl_rust_component_metrics.py` | `0 3 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_search` | `bigquery-etl/dags/bqetl_search.py` | `0 3 * * *` | `wait_for_copy_deduplicate_main_ping` | `copy_deduplicate_main_ping` |
| `bqetl_search_dashboard` | `bigquery-etl/dags/bqetl_search_dashboard.py` | `30 5 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_serp` | `bigquery-etl/dags/bqetl_serp.py` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_terms_of_use` | `bigquery-etl/dags/bqetl_terms_of_use.py` | `0 6 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_usage_reporting` | `bigquery-etl/dags/bqetl_usage_reporting.py` | `0 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `bqetl_use_counter_analysis` | `bigquery-etl/dags/bqetl_use_counter_analysis.py` | `0 8 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |
| `catalyst` | `telemetry-airflow/dags/catalyst.py` | `0 4 * * *` | `wait_for_bq_main_events` | `bq_main_events` |
| `catalyst` | `telemetry-airflow/dags/catalyst.py` | `0 4 * * *` | `wait_for_event_events` | `event_events` |
| `firefox_public_data_report` | `telemetry-airflow/dags/firefox_public_data_report.py` | `0 1 * * MON` | `wait_for_main_ping` | `copy_deduplicate_main_ping` |
| `glam_fenix` | `telemetry-airflow/dags/glam_fenix.py` | `0 2 * * *` | `wait_for_copy_deduplicate` | `copy_deduplicate_all` |
| `glam_fog` | `telemetry-airflow/dags/glam_fog.py` | `0 2 * * *` | `wait_for_copy_deduplicate` | `copy_deduplicate_all` |
| `private_bqetl_ads` | `private-bigquery-etl/dags/private_bqetl_ads.py` | `0 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate_all` |

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
| `bhr_collection` | `0 5 * * *` | `wait_for_copy_deduplicate` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_accounts_derived` | `30 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_ads_hourly` | `@hourly` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_analytics_aggregations` | `15 4 * * *` | `wait_for_bq_main_events` | `copy_deduplicate` | `bq_main_events` | no |
| `bqetl_analytics_aggregations` | `15 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_analytics_aggregations` | `15 4 * * *` | `wait_for_event_events` | `copy_deduplicate` | `event_events` | no |
| `bqetl_analytics_tables` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_broken_reports_agg` | `0 6 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_client_attributes` | `40 19 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_crashes` | `0 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_default_browser_aggregates` | `0 22 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_desktop_engagement_model` | `0 5 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_desktop_installs_v1` | `55 23 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_ech_adoption_rate` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_firefox_installer_aggregates` | `0 15 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_fx_cert_error_privacy_dashboard` | `40 16 * * *` | `wait_for_bq_main_events` | `copy_deduplicate` | `bq_main_events` | no |
| `bqetl_fx_cert_error_privacy_dashboard` | `40 16 * * *` | `wait_for_event_events` | `copy_deduplicate` | `event_events` | no |
| `bqetl_fx_health_ind_dashboard` | `0 16 * * *` | `wait_for_bq_main_events` | `copy_deduplicate` | `bq_main_events` | no |
| `bqetl_fx_health_ind_dashboard` | `0 16 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_fx_health_ind_dashboard` | `0 16 * * *` | `wait_for_copy_deduplicate_main_ping` | `copy_deduplicate` | `copy_deduplicate_main_ping` | no |
| `bqetl_fx_health_ind_dashboard` | `0 16 * * *` | `wait_for_event_events` | `copy_deduplicate` | `event_events` | no |
| `bqetl_generated_funnels` | `0 5 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_glean_usage` | `0 2 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_glean_usage` | `0 2 * * *` | `wait_for_telemetry_derived__core_clients_first_seen__v1` | `copy_deduplicate` | `telemetry_derived__core_clients_first_seen__v1` | no |
| `bqetl_ltv` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_marketing_analysis` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_mobile_feature_usage` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_mobile_kpi_metrics` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_nimbus_feature_monitoring` | `0 3 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_pageload_v1` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_pocket` | `0 12 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_review_checker` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_rust_component_metrics` | `0 3 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_search` | `0 3 * * *` | `wait_for_copy_deduplicate_main_ping` | `copy_deduplicate` | `copy_deduplicate_main_ping` | no |
| `bqetl_search_dashboard` | `30 5 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_serp` | `@daily` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_terms_of_use` | `0 6 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |
| `bqetl_usage_reporting` | `0 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `bqetl_use_counter_analysis` | `0 8 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `catalyst` | `0 4 * * *` | `wait_for_bq_main_events` | `copy_deduplicate` | `bq_main_events` | no |
| `catalyst` | `0 4 * * *` | `wait_for_event_events` | `copy_deduplicate` | `event_events` | no |
| `firefox_public_data_report` | `0 1 * * MON` | `wait_for_main_ping` | `copy_deduplicate` | `copy_deduplicate_main_ping` | **YES** |
| `glam_fenix` | `0 2 * * *` | `wait_for_copy_deduplicate` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `glam_fog` | `0 2 * * *` | `wait_for_copy_deduplicate` | `copy_deduplicate` | `copy_deduplicate_all` | **YES** |
| `private_bqetl_ads` | `0 4 * * *` | `wait_for_copy_deduplicate_all` | `copy_deduplicate` | `copy_deduplicate_all` | no |

## Non-daily DAGs in the impacted set

copy_deduplicate runs daily. Downstream DAGs on non-daily schedules (hourly, weekly, `@once`, etc.) are where marker generation is trickiest — flag for manual review regardless of whether they appear in the gap.

| DAG | Schedule | Class | In gap? |
|---|---|---|---|
| `bqetl_ads_hourly` | `@hourly` | @hourly | **YES** |
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
