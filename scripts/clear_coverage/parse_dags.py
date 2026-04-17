"""
Statically parse every DAG .py file under the three DAG directories and extract:
  - dag_id and schedule
  - every ExternalTaskSensor call (external_dag_id, external_task_id, sensor task_id)
  - every ExternalTaskMarker call (external_dag_id, external_task_id)

No Airflow import is needed — we walk the AST and, for the `copy_deduplicate`-style
pattern of generating markers inside `for dep in {(..., ...), ...}` loops,
statically unroll the loop with ast.literal_eval.

Output: dags.json, one entry per parseable DAG file.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_DIRS = [
    REPO_ROOT / "bigquery-etl" / "dags",
    REPO_ROOT / "private-bigquery-etl" / "dags",
    REPO_ROOT / "telemetry-airflow" / "dags",
]
OUTPUT_PATH = Path(__file__).resolve().parent / "dags.json"


def _literal(node: ast.AST) -> Any:
    """Best-effort literal evaluation. Returns a sentinel for unresolvable nodes."""
    try:
        return ast.literal_eval(node)
    except Exception:
        return _UNRESOLVED


_UNRESOLVED = object()


def _kw(call: ast.Call, name: str) -> ast.AST | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _positional(call: ast.Call, index: int) -> ast.AST | None:
    if len(call.args) > index:
        return call.args[index]
    return None


def _func_name(call: ast.Call) -> str:
    """Return the simple name of the callable, e.g. 'DAG', 'models.DAG', 'ExternalTaskSensor'."""
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


class ModuleScope:
    """Collected module-level constants usable for resolving names in AST nodes."""

    def __init__(self, tree: ast.Module):
        self.names: dict[str, Any] = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                tgt = node.targets[0]
                if isinstance(tgt, ast.Name):
                    value = _literal(node.value)
                    if value is not _UNRESOLVED:
                        self.names[tgt.id] = value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                value = _literal(node.value)
                if value is not _UNRESOLVED:
                    self.names[node.target.id] = value

    def resolve(self, node: ast.AST) -> Any:
        """Resolve a node to a concrete value if possible using literals + module names."""
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return self.names.get(node.id, _UNRESOLVED)
        return _literal(node)


def _resolve_with_loop_var(
    node: ast.AST,
    scope: ModuleScope,
    loop_var: str | None,
    loop_value: Any,
    locals_: dict[str, Any] | None = None,
) -> Any:
    """Resolve an AST node to a concrete value, substituting loop_var -> loop_value where applicable.

    Resolution order: loop_var binding > locals_ (nearest-enclosing block scope) > module scope.

    Handles:
      - ast.Constant
      - ast.Name referring to either loop_var, a local, or a module-scope constant
      - ast.Subscript of loop_var (e.g. loop_var[0])
      - ast.JoinedStr (f-string) built from the above
      - ast.Set / ast.List / ast.Tuple / ast.Dict literals whose elements are resolvable
    """
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if loop_var is not None and node.id == loop_var:
            return loop_value
        if locals_ is not None and node.id in locals_:
            return locals_[node.id]
        return scope.names.get(node.id, _UNRESOLVED)
    if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
        items = []
        for elt in node.elts:
            v = _resolve_with_loop_var(elt, scope, loop_var, loop_value, locals_)
            if v is _UNRESOLVED:
                return _UNRESOLVED
            items.append(v)
        if isinstance(node, ast.Set):
            try:
                return set(items)
            except TypeError:
                return _UNRESOLVED
        if isinstance(node, ast.Tuple):
            return tuple(items)
        return items
    if isinstance(node, ast.Dict):
        out: dict[Any, Any] = {}
        for k_node, v_node in zip(node.keys, node.values):
            if k_node is None:
                return _UNRESOLVED
            k = _resolve_with_loop_var(k_node, scope, loop_var, loop_value, locals_)
            v = _resolve_with_loop_var(v_node, scope, loop_var, loop_value, locals_)
            if k is _UNRESOLVED or v is _UNRESOLVED:
                return _UNRESOLVED
            out[k] = v
        return out
    if isinstance(node, ast.Subscript):
        target = _resolve_with_loop_var(node.value, scope, loop_var, loop_value, locals_)
        if target is _UNRESOLVED:
            return _UNRESOLVED
        slc = node.slice
        if isinstance(slc, ast.Index):  # pragma: no cover (Py<3.9)
            slc = slc.value  # type: ignore[attr-defined]
        idx = _resolve_with_loop_var(slc, scope, loop_var, loop_value, locals_)
        if idx is _UNRESOLVED:
            return _UNRESOLVED
        try:
            return target[idx]
        except Exception:
            return _UNRESOLVED
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                resolved = _resolve_with_loop_var(v.value, scope, loop_var, loop_value, locals_)
                if resolved is _UNRESOLVED:
                    return _UNRESOLVED
                parts.append(str(resolved))
            else:
                return _UNRESOLVED
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolve_with_loop_var(node.left, scope, loop_var, loop_value, locals_)
        right = _resolve_with_loop_var(node.right, scope, loop_var, loop_value, locals_)
        if left is _UNRESOLVED or right is _UNRESOLVED:
            return _UNRESOLVED
        try:
            return left + right
        except Exception:
            return _UNRESOLVED
    return _literal(node)


def _extract_dag(tree: ast.Module, scope: ModuleScope) -> dict[str, Any] | None:
    """Find the first DAG / models.DAG constructor call or @dag(...) decorator and
    extract dag_id + schedule."""
    def _from_call(call: ast.Call) -> dict[str, Any] | None:
        dag_id_node = _kw(call, "dag_id") or _positional(call, 0)
        sched_node = _kw(call, "schedule_interval") or _kw(call, "schedule")
        dag_id = scope.resolve(dag_id_node) if dag_id_node is not None else _UNRESOLVED
        schedule = scope.resolve(sched_node) if sched_node is not None else None
        if dag_id is _UNRESOLVED:
            return None
        return {
            "dag_id": dag_id,
            "schedule": schedule if schedule is not _UNRESOLVED else None,
        }

    for node in ast.walk(tree):
        # Standard DAG(...) / models.DAG(...) constructor.
        if isinstance(node, ast.Call) and _func_name(node) == "DAG":
            result = _from_call(node)
            if result is not None:
                return result
        # @dag(...) TaskFlow decorator.
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and _func_name(dec) == "dag":
                    result = _from_call(dec)
                    if result is not None:
                        return result
    return None


def _extract_external_calls_from_call(
    call: ast.Call,
    scope: ModuleScope,
    loop_var: str | None = None,
    loop_value: Any = None,
    locals_: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]] | None:
    """If `call` is ExternalTaskSensor / ExternalTaskMarker, return (kind, fields)."""
    name = _func_name(call)
    if name not in ("ExternalTaskSensor", "ExternalTaskMarker"):
        return None

    def resolve(kwname: str) -> Any:
        n = _kw(call, kwname)
        if n is None:
            return None
        return _resolve_with_loop_var(n, scope, loop_var, loop_value, locals_)

    external_dag_id = resolve("external_dag_id")
    external_task_id = resolve("external_task_id")
    task_id = resolve("task_id")

    return name, {
        "external_dag_id": external_dag_id if external_dag_id is not _UNRESOLVED else None,
        "external_task_id": external_task_id if external_task_id is not _UNRESOLVED else None,
        "task_id": task_id if task_id is not _UNRESOLVED else None,
    }


def _walk_for_external_calls(
    body: list[ast.stmt],
    scope: ModuleScope,
    sensors: list[dict[str, Any]],
    markers: list[dict[str, Any]],
    loop_var: str | None = None,
    loop_value: Any = None,
    locals_: dict[str, Any] | None = None,
) -> None:
    """Walk a block of statements, descending into control flow and unrolling
    eligible `for` loops whose iterable is a literal of tuples/strings.

    `locals_` is a block-local name → value map. Assign statements we can resolve
    are recorded there so that subsequent statements (e.g. `for x in var:`) can
    look them up. We always *copy* locals_ before descending into child blocks so
    a reassignment inside a `with`/`for`/`if` does not leak back out.
    """
    current_locals: dict[str, Any] = dict(locals_) if locals_ else {}

    for stmt in body:
        # Record simple single-target assignments to the local scope when resolvable —
        # this is what lets `for dep in downstream_dependencies:` reach the set literal.
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            value = _resolve_with_loop_var(stmt.value, scope, loop_var, loop_value, current_locals)
            if value is not _UNRESOLVED:
                current_locals[stmt.targets[0].id] = value
            # Fall through to the generic call-finding path below: `x = ExternalTaskSensor(...)`
            # is common in static DAGs.

        if isinstance(stmt, ast.For):
            iterable = _resolve_with_loop_var(stmt.iter, scope, loop_var, loop_value, current_locals)
            if iterable is not _UNRESOLVED and isinstance(stmt.target, ast.Name):
                try:
                    iter_values = list(iterable)
                except Exception:
                    iter_values = None
                if iter_values is not None:
                    for elem in iter_values:
                        _walk_for_external_calls(
                            stmt.body, scope, sensors, markers,
                            loop_var=stmt.target.id, loop_value=elem,
                            locals_=current_locals,
                        )
                    continue
            _walk_for_external_calls(stmt.body, scope, sensors, markers, loop_var, loop_value, current_locals)
        elif isinstance(stmt, (ast.With, ast.AsyncWith)):
            _walk_for_external_calls(stmt.body, scope, sensors, markers, loop_var, loop_value, current_locals)
        elif isinstance(stmt, ast.If):
            _walk_for_external_calls(stmt.body, scope, sensors, markers, loop_var, loop_value, current_locals)
            _walk_for_external_calls(stmt.orelse, scope, sensors, markers, loop_var, loop_value, current_locals)
        elif isinstance(stmt, ast.Try):
            _walk_for_external_calls(stmt.body, scope, sensors, markers, loop_var, loop_value, current_locals)
            for h in stmt.handlers:
                _walk_for_external_calls(h.body, scope, sensors, markers, loop_var, loop_value, current_locals)
            _walk_for_external_calls(stmt.orelse, scope, sensors, markers, loop_var, loop_value, current_locals)
            _walk_for_external_calls(stmt.finalbody, scope, sensors, markers, loop_var, loop_value, current_locals)
        else:
            for call in _find_calls(stmt):
                result = _extract_external_calls_from_call(call, scope, loop_var, loop_value, current_locals)
                if result is None:
                    continue
                kind, fields = result
                if kind == "ExternalTaskSensor":
                    sensors.append(fields)
                else:
                    markers.append(fields)


def _find_calls(node: ast.AST) -> list[ast.Call]:
    return [n for n in ast.walk(node) if isinstance(n, ast.Call)]


def parse_file(path: Path) -> dict[str, Any]:
    source = path.read_text()
    tree = ast.parse(source, filename=str(path))
    scope = ModuleScope(tree)

    dag = _extract_dag(tree, scope)
    sensors: list[dict[str, Any]] = []
    markers: list[dict[str, Any]] = []
    _walk_for_external_calls(tree.body, scope, sensors, markers)

    rel = path.relative_to(REPO_ROOT)
    return {
        "file": str(rel),
        "dag_id": dag["dag_id"] if dag else None,
        "schedule": dag["schedule"] if dag else None,
        "sensors": sensors,
        "markers": markers,
    }


def main() -> int:
    entries: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for d in DAG_DIRS:
        if not d.exists():
            errors.append({"file": str(d), "error": "directory missing"})
            continue
        for path in sorted(d.glob("*.py")):
            if path.name == "__init__.py":
                continue
            try:
                entries.append(parse_file(path))
            except Exception as e:  # noqa: BLE001 - we want best-effort
                errors.append({"file": str(path.relative_to(REPO_ROOT)), "error": f"{type(e).__name__}: {e}"})

    output = {
        "entries": entries,
        "errors": errors,
        "counts": {
            "total_files": len(entries),
            "files_with_dag_id": sum(1 for e in entries if e["dag_id"]),
            "total_sensors": sum(len(e["sensors"]) for e in entries),
            "total_markers": sum(len(e["markers"]) for e in entries),
            "parse_errors": len(errors),
        },
    }
    OUTPUT_PATH.write_text(json.dumps(output, indent=2, default=str))
    print(f"Wrote {OUTPUT_PATH}")
    print(f"  entries={len(entries)} errors={len(errors)}")
    print(f"  with_dag_id={output['counts']['files_with_dag_id']}")
    print(f"  sensors={output['counts']['total_sensors']} markers={output['counts']['total_markers']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
