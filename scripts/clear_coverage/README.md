# clear_coverage

Static analysis over every DAG in `bigquery-etl/dags/`, `private-bigquery-etl/dags/`,
and `telemetry-airflow/dags/` to answer:

> If we recursively clear `copy_deduplicate`, which DAGs — and which specific sensor
> tasks — will NOT be cleared, and therefore need manual intervention during a backfill?

Airflow's "clear downstream → recursive" only follows cross-DAG dependencies expressed
as `ExternalTaskMarker` (upstream side) + `ExternalTaskSensor` (downstream side) with
matching `(external_dag_id, external_task_id ↔ task_id)` on both ends. Anything else
(time-based waits, TriggerDagRunOperator, table-level data dependencies without a
sensor, mismatched task ids) will not propagate.

## Usage

```
python3 scripts/clear_coverage/parse_dags.py
python3 scripts/clear_coverage/analyze_clear_coverage.py
```

The first script produces `dags.json` (one entry per parsed DAG file). The second
reads that and produces `report.md`. Both output files are gitignored.

## Filtering out paused DAGs

Paused-DAG state is runtime metadata (not in the DAG files), so provide it
yourself. Create `paused_dags.txt` next to the scripts, one `dag_id` per line
(blank lines and `#`-comments ignored), then re-run the analyzer. The easiest
source is the Airflow UI's filter, or:

```
airflow dags list-paused -o plain | awk 'NR>1 {print $1}' > scripts/clear_coverage/paused_dags.txt
```

When the file is present, the analyzer drops those DAGs from the entries *and*
prunes sensor/marker edges that point at them, so a paused DAG can't drag a
live DAG into the impacted set (or vice versa). The file is gitignored.

## What's in the report

- **Gap DAGs** — DAGs that are transitively impacted by `copy_deduplicate` via sensors
  but that the recursive-clear wavefront won't reach. Each row names the upstream
  DAG where the chain breaks and classifies the reason (`missing_marker_on_upstream`,
  `sensor_task_id_mismatch`, `upstream_not_cleared`).
- **Direct sensors on copy_deduplicate that are NOT in its marker sets** — fix by
  adding the DAG id to the appropriate `downstream_dependencies` set in
  `telemetry-airflow/dags/copy_deduplicate.py`.
- **Task-level sensor gaps** — sensor tasks inside *reachable* DAGs whose upstream
  doesn't publish a matching marker. These tasks (and anything downstream of them
  within the DAG that doesn't also depend on a cleared sensor) will retain stale
  state after a backfill unless cleared by hand.
- **Non-daily DAGs in the impacted set** — weekly/hourly/monthly DAGs where marker
  generation relative to a daily upstream is tricky; flag for manual review.

## Caveats

- Table-level data dependencies (a DAG reads a `*_stable` table with no sensor) are
  invisible to this analysis. Catching those requires parsing bqetl SQL.
- The parser unrolls simple `for x in <literal-iterable>:` loops (used in
  `copy_deduplicate.py` to generate marker groups) but it is not a full Python
  interpreter — complex runtime logic inside a DAG may be missed.
