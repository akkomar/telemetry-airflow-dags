"""
Given the parsed DAG data from parse_dags.py, compute which DAGs recursive
"clear downstream" from `copy_deduplicate` would actually cover vs. which ones
logically depend on copy_deduplicate but would be missed.

Two graphs are built from the parsed data:
  - marker_edges[upstream_dag] = set of (downstream_dag, downstream_sensor_task_id)
      — derived from ExternalTaskMarker calls in the upstream DAG.
  - sensor_edges[downstream_dag] = set of (upstream_dag, upstream_task_id, sensor_task_id)
      — derived from ExternalTaskSensor calls in the downstream DAG.

REACHABLE (what clear actually does): BFS from copy_deduplicate along marker_edges.
An edge (D_up -> (D_down, marker_ext_task_id)) is "real" only if D_down actually
has a sensor whose task_id == marker_ext_task_id AND whose external_dag_id == D_up.
Any mismatch truncates the wavefront.

IMPACTED (what clear should cover): BFS from copy_deduplicate along the inverted
sensor graph. Any DAG with a sensor pointing into the impacted set is itself impacted.

GAP = IMPACTED \\ REACHABLE. These are the DAGs that need manual clearing.

Output: report.md next to this script.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DAGS_JSON = ROOT / "dags.json"
REPORT_PATH = ROOT / "report.md"
START_DAG = "copy_deduplicate"


def load() -> list[dict[str, Any]]:
    data = json.loads(DAGS_JSON.read_text())
    # Drop entries with no dag_id (shouldn't happen given current parser but defend anyway).
    return [e for e in data["entries"] if e["dag_id"]]


def build_graphs(entries: list[dict[str, Any]]):
    """Return (marker_edges, sensor_edges, sensors_by_dag, dag_by_id)."""
    marker_edges: dict[str, set[tuple[str, str]]] = defaultdict(set)
    sensor_edges: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    dag_by_id: dict[str, dict[str, Any]] = {}

    for e in entries:
        dag_by_id[e["dag_id"]] = e
        for m in e["markers"]:
            if m["external_dag_id"] and m["external_task_id"]:
                marker_edges[e["dag_id"]].add((m["external_dag_id"], m["external_task_id"]))
        for s in e["sensors"]:
            if s["external_dag_id"] and s["external_task_id"] and s["task_id"]:
                sensor_edges[e["dag_id"]].add(
                    (s["external_dag_id"], s["external_task_id"], s["task_id"])
                )
    return marker_edges, sensor_edges, dag_by_id


def has_matching_sensor(
    downstream_dag: str,
    upstream_dag: str,
    marker_ext_task_id: str,
    sensor_edges: dict[str, set[tuple[str, str, str]]],
) -> bool:
    """Does downstream_dag have a sensor whose task_id == marker_ext_task_id
    AND whose external_dag_id == upstream_dag?

    That's the real Airflow contract: ExternalTaskMarker(external_task_id=X) in
    upstream_dag clears a task named X in downstream_dag, and that task should
    itself be an ExternalTaskSensor pointing back at upstream_dag so the clear
    propagates through the sensor's own downstream.
    """
    for (s_up_dag, _s_up_task, s_task_id) in sensor_edges.get(downstream_dag, ()):
        if s_task_id == marker_ext_task_id and s_up_dag == upstream_dag:
            return True
    return False


def compute_reachable(
    marker_edges: dict[str, set[tuple[str, str]]],
    sensor_edges: dict[str, set[tuple[str, str, str]]],
    start: str,
) -> tuple[set[str], dict[str, tuple[str, str]]]:
    """BFS along marker→sensor edges. Returns (reachable_dag_ids, predecessor_map).

    predecessor_map[d] = (upstream_dag, marker_ext_task_id) — the edge that first
    put d into the reachable set, so we can pretty-print the chain.
    """
    reachable = {start}
    predecessors: dict[str, tuple[str, str]] = {}
    q = deque([start])
    while q:
        up = q.popleft()
        for (down, marker_task_id) in marker_edges.get(up, ()):
            if down in reachable:
                continue
            if has_matching_sensor(down, up, marker_task_id, sensor_edges):
                reachable.add(down)
                predecessors[down] = (up, marker_task_id)
                q.append(down)
    return reachable, predecessors


def compute_impacted(
    sensor_edges: dict[str, set[tuple[str, str, str]]],
    start: str,
) -> tuple[set[str], dict[str, tuple[str, str, str]]]:
    """BFS along the inverted sensor graph. Returns (impacted_dag_ids, predecessor_map).

    predecessor_map[d] = (upstream_dag, upstream_task_id, sensor_task_id) — the first
    sensor edge that pulled d into the impacted set.
    """
    # Build inverted sensor index: upstream_dag -> set of (downstream_dag, upstream_task_id, sensor_task_id)
    inverted: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for down, edges in sensor_edges.items():
        for (up_dag, up_task, sensor_task) in edges:
            inverted[up_dag].add((down, up_task, sensor_task))

    impacted = {start}
    predecessors: dict[str, tuple[str, str, str]] = {}
    q = deque([start])
    while q:
        up = q.popleft()
        for (down, up_task, sensor_task) in inverted.get(up, ()):
            if down in impacted:
                continue
            impacted.add(down)
            predecessors[down] = (up, up_task, sensor_task)
            q.append(down)
    return impacted, predecessors


def classify_gap(
    dag: str,
    sensor_pred: dict[str, tuple[str, str, str]],
    marker_edges: dict[str, set[tuple[str, str]]],
    sensor_edges: dict[str, set[tuple[str, str, str]]],
    reachable: set[str],
) -> dict[str, str]:
    """For a DAG in the gap, return a dict describing *why* clear doesn't reach it.

    We walk up the sensor-predecessor chain until we find the first hop where the
    chain breaks. The break may happen above this DAG (an ancestor was itself
    unreachable), so we iterate until we hit the first edge whose upstream IS in
    reachable. That edge is the actionable fix point.
    """
    # Walk up the chain to find the first upstream that IS reachable;
    # the edge from that upstream to its downstream (current) is the break.
    current = dag
    while current in sensor_pred:
        up_dag, up_task, sensor_task_id = sensor_pred[current]
        if up_dag in reachable:
            # The break is here: upstream is in reachable, downstream (current) is not.
            break_up = up_dag
            break_down = current
            # Figure out the reason:
            upstream_markers = marker_edges.get(break_up, set())
            marker_matches = [
                (d, mt) for (d, mt) in upstream_markers
                if d == break_down
            ]
            if not marker_matches:
                reason = "missing_marker_on_upstream"
                detail = f"{break_up} has no ExternalTaskMarker targeting {break_down}"
            else:
                # There is at least one marker, but sensor match failed.
                matching_tasks = {mt for (_, mt) in marker_matches}
                sensor_task_ids = {
                    s_task_id for (s_up_dag, _s_up_task, s_task_id)
                    in sensor_edges.get(break_down, set())
                    if s_up_dag == break_up
                }
                if sensor_task_ids & matching_tasks:
                    # Coverage by a matching marker exists but we ended up here anyway,
                    # which means the upstream was unreachable at traversal time —
                    # unusual, fall through to next ancestor.
                    current = up_dag
                    continue
                reason = "sensor_task_id_mismatch"
                detail = (
                    f"{break_up} marker external_task_ids={sorted(matching_tasks)} "
                    f"but {break_down} sensor task_ids (against {break_up})="
                    f"{sorted(sensor_task_ids)}"
                )
            return {
                "break_upstream": break_up,
                "break_downstream": break_down,
                "break_sensor_task_id": sensor_task_id,
                "break_upstream_task_id": up_task,
                "reason": reason,
                "detail": detail,
            }
        current = up_dag
    # If we never found a reachable upstream on the chain, the DAG is simply
    # disconnected from the wavefront.
    return {
        "break_upstream": "",
        "break_downstream": dag,
        "break_sensor_task_id": "",
        "break_upstream_task_id": "",
        "reason": "upstream_not_cleared",
        "detail": "no reachable upstream on the sensor chain",
    }


def _schedule_classification(schedule: Any) -> str:
    if schedule is None:
        return "none/manual"
    s = str(schedule).strip()
    if s == "@daily":
        return "daily"
    if s in {"@once", "@hourly", "@weekly", "@monthly"}:
        return s
    parts = s.split()
    if len(parts) == 5:
        minute, hour, dom, month, dow = parts
        # Daily: day-of-month, month, day-of-week all wildcard; minute and hour are simple.
        if dom == "*" and month == "*" and dow == "*" \
                and "/" not in minute and "/" not in hour \
                and "," not in minute and "," not in hour:
            return "daily"
        # Weekly: day-of-month and month are wildcard, day-of-week is a specific value.
        if dom == "*" and month == "*" and dow != "*":
            return "weekly"
        # Monthly: day-of-month is a specific value, month is wildcard.
        if dom != "*" and month == "*":
            return "monthly"
    return f"non-daily ({s})"


def main() -> int:
    entries = load()
    marker_edges, sensor_edges, dag_by_id = build_graphs(entries)

    if START_DAG not in dag_by_id:
        raise SystemExit(f"{START_DAG} not found in parsed entries")

    reachable, reach_pred = compute_reachable(marker_edges, sensor_edges, START_DAG)
    impacted, sensor_pred = compute_impacted(sensor_edges, START_DAG)
    gap = sorted(impacted - reachable)

    # Direct sensors on copy_deduplicate that are NOT matched by a marker entry.
    direct_unmarked: list[dict[str, Any]] = []
    copy_dedup_markers = marker_edges.get(START_DAG, set())
    for e in entries:
        for s in e["sensors"]:
            if s["external_dag_id"] != START_DAG:
                continue
            marker_task = s["task_id"]
            if (e["dag_id"], marker_task) not in copy_dedup_markers:
                direct_unmarked.append({
                    "dag_id": e["dag_id"],
                    "file": e["file"],
                    "schedule": e["schedule"],
                    "sensor_task_id": s["task_id"],
                    "sensor_external_task_id": s["external_task_id"],
                })

    # Every sensor edge inside the impacted set whose upstream IS reachable, but
    # which itself is uncovered (no matching marker). These are the task-level
    # holes: within a DAG that IS cleared via some other sensor, these specific
    # sensor tasks will NOT be cleared, so any downstream task that depends
    # exclusively on them will still hold stale state after the backfill.
    task_level_gaps: list[dict[str, Any]] = []
    for e in entries:
        if e["dag_id"] == START_DAG:
            continue
        for s in e["sensors"]:
            up_dag = s["external_dag_id"]
            up_task = s["external_task_id"]
            sensor_task_id = s["task_id"]
            if not up_dag or not up_task or not sensor_task_id:
                continue
            if up_dag not in reachable:
                # Upstream isn't cleared at all; the gap-DAG section already covers this.
                continue
            marker_pairs = marker_edges.get(up_dag, set())
            if (e["dag_id"], sensor_task_id) not in marker_pairs:
                task_level_gaps.append({
                    "dag_id": e["dag_id"],
                    "file": e["file"],
                    "schedule": e["schedule"],
                    "sensor_task_id": sensor_task_id,
                    "upstream_dag": up_dag,
                    "upstream_task_id": up_task,
                    "dag_reachable": e["dag_id"] in reachable,
                })

    # Non-daily impacted DAGs — interesting for manual review per srose's note.
    non_daily_impacted: list[dict[str, Any]] = []
    for d in sorted(impacted):
        info = dag_by_id.get(d, {})
        cls = _schedule_classification(info.get("schedule"))
        if cls not in ("daily",) and d != START_DAG:
            non_daily_impacted.append({
                "dag_id": d,
                "file": info.get("file", ""),
                "schedule": info.get("schedule"),
                "class": cls,
                "in_gap": d in set(gap),
            })

    # Classify each gap DAG.
    gap_rows: list[dict[str, Any]] = []
    for d in gap:
        info = dag_by_id.get(d, {})
        classification = classify_gap(d, sensor_pred, marker_edges, sensor_edges, reachable)
        gap_rows.append({
            "dag_id": d,
            "file": info.get("file", ""),
            "schedule": info.get("schedule"),
            "schedule_class": _schedule_classification(info.get("schedule")),
            **classification,
        })

    # Sort gap rows: break_upstream first (to cluster fixes), then dag_id.
    gap_rows.sort(key=lambda r: (r["break_upstream"], r["dag_id"]))

    # -------------------- Render report.md --------------------
    lines: list[str] = []
    lines.append(f"# Clear-coverage report for `{START_DAG}`")
    lines.append("")
    lines.append(f"- Total DAGs parsed: **{len(entries)}**")
    lines.append(f"- Transitively impacted by {START_DAG} (via sensors): **{len(impacted)}**")
    lines.append(f"- Reachable by recursive clear (marker+sensor matched): **{len(reachable)}**")
    lines.append(f"- **Gap (impacted − reachable): {len(gap)}**")
    lines.append("")
    lines.append("## Gap DAGs (logically depend on copy_deduplicate but clear won't reach them)")
    lines.append("")
    if not gap_rows:
        lines.append("_None — every impacted DAG is reachable. :)_")
    else:
        lines.append("| DAG | Schedule | Break upstream | Break downstream | Reason | Detail |")
        lines.append("|---|---|---|---|---|---|")
        for r in gap_rows:
            lines.append(
                f"| `{r['dag_id']}` | {r['schedule_class']} (`{r['schedule']}`) | "
                f"`{r['break_upstream']}` | `{r['break_downstream']}` | "
                f"{r['reason']} | {r['detail']} |"
            )
    lines.append("")

    lines.append("## Direct sensors on copy_deduplicate that are NOT in its marker sets")
    lines.append("")
    lines.append(
        "These DAGs have an `ExternalTaskSensor` pointing at `copy_deduplicate`, "
        "but `copy_deduplicate.py` has no corresponding `ExternalTaskMarker` for them. "
        "Fix by adding the DAG to the appropriate `downstream_dependencies` set in "
        "`telemetry-airflow/dags/copy_deduplicate.py`."
    )
    lines.append("")
    if not direct_unmarked:
        lines.append("_None._")
    else:
        lines.append("| DAG | File | Schedule | Sensor task_id | Upstream task_id |")
        lines.append("|---|---|---|---|---|")
        for r in sorted(direct_unmarked, key=lambda x: (x["dag_id"], x["sensor_task_id"])):
            lines.append(
                f"| `{r['dag_id']}` | `{r['file']}` | `{r['schedule']}` | "
                f"`{r['sensor_task_id']}` | `{r['sensor_external_task_id']}` |"
            )
    lines.append("")

    lines.append("## Task-level sensor gaps (reachable DAGs with uncovered sensors)")
    lines.append("")
    lines.append(
        "These are sensor tasks *inside* reachable DAGs whose upstream DAG does not "
        "publish a matching `ExternalTaskMarker`. Recursive clear will not clear these "
        "sensor tasks, so anything downstream of them **within the DAG** that doesn't "
        "also depend on a cleared sensor will retain stale state. For a full backfill, "
        "each of these needs to be cleared by hand (or fixed by adding a marker upstream)."
    )
    lines.append("")
    if not task_level_gaps:
        lines.append("_None._")
    else:
        lines.append("| Downstream DAG | Schedule | Sensor task_id | Upstream DAG | Upstream task_id | DAG in top-level gap? |")
        lines.append("|---|---|---|---|---|---|")
        for r in sorted(task_level_gaps, key=lambda x: (x["upstream_dag"], x["dag_id"], x["sensor_task_id"])):
            lines.append(
                f"| `{r['dag_id']}` | `{r['schedule']}` | `{r['sensor_task_id']}` | "
                f"`{r['upstream_dag']}` | `{r['upstream_task_id']}` | "
                f"{'no' if r['dag_reachable'] else '**YES**'} |"
            )
    lines.append("")

    lines.append("## Non-daily DAGs in the impacted set")
    lines.append("")
    lines.append(
        "copy_deduplicate runs daily. Downstream DAGs on non-daily schedules (hourly, "
        "weekly, `@once`, etc.) are where marker generation is trickiest — flag for "
        "manual review regardless of whether they appear in the gap."
    )
    lines.append("")
    if not non_daily_impacted:
        lines.append("_None._")
    else:
        lines.append("| DAG | Schedule | Class | In gap? |")
        lines.append("|---|---|---|---|")
        for r in non_daily_impacted:
            lines.append(
                f"| `{r['dag_id']}` | `{r['schedule']}` | {r['class']} | "
                f"{'**YES**' if r['in_gap'] else 'no'} |"
            )
    lines.append("")

    lines.append("## Caveats")
    lines.append("")
    lines.append(
        "- **Table-level data dependencies are invisible here.** A DAG that reads a "
        "`*_stable` table populated by copy_deduplicate without any `ExternalTaskSensor` "
        "will not appear in either `impacted` or `reachable`. Catching those requires "
        "parsing bqetl SQL under `bigquery-etl/sql/` and is out of scope for this pass."
    )
    lines.append(
        "- `ExternalTaskSensor` with unusual `execution_delta` (e.g. `timedelta(days=-6)` "
        "for weekly DAGs) is counted as a sensor here; whether clearing will actually "
        "align timestamps correctly is a separate concern."
    )
    lines.append("")

    REPORT_PATH.write_text("\n".join(lines))
    print(f"Wrote {REPORT_PATH}")
    print(f"  impacted={len(impacted)} reachable={len(reachable)} gap={len(gap)}")
    print(f"  direct_unmarked={len(direct_unmarked)} task_level_gaps={len(task_level_gaps)}")
    print(f"  non_daily_impacted={len(non_daily_impacted)}")
    return 0


if __name__ == "__main__":
    main()
