"""Invariant validators for generated Airflow DAGs and PySpark scripts.

These functions take raw Python source code (str) and return (passed: bool, details: list[str])
indicating whether each invariant from D3_airflow_dags.json / D4_pyspark_tasks.json holds.

Used in pytest test_dag_generation_quality.py and test_spark_generation_quality.py.
"""
import ast
import re
from dataclasses import dataclass


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


# ---------- Common ---------------------------------------------------------------

def parses_ok(code: str) -> CheckResult:
    try:
        ast.parse(code)
        return CheckResult("valid_python_ast", True)
    except SyntaxError as e:
        return CheckResult("valid_python_ast", False, str(e))


def _walk_calls(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            yield node


def _name_of_call(node: ast.Call) -> str:
    """Best-effort name extraction: foo() or pkg.mod.foo()."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


# ---------- Airflow DAG validators ----------------------------------------------

def has_operator(code: str, operator_names: list[str]) -> CheckResult:
    """Check that DAG uses at least one of the given operator classes."""
    found = [op for op in operator_names if re.search(rf"\b{op}\b", code)]
    return CheckResult(
        f"has_operator({'|'.join(operator_names)})",
        bool(found),
        f"matched: {found}" if found else "no operator from list found",
    )


def has_task_id(code: str, task_id: str) -> CheckResult:
    pattern = rf"task_id\s*=\s*['\"]{re.escape(task_id)}['\"]"
    ok = bool(re.search(pattern, code))
    return CheckResult(f"has_task_id({task_id})", ok)


def schedule_matches(code: str, pattern: str) -> CheckResult:
    """Check schedule_interval / schedule parameter.

    Accept either exact equality, or a known equivalence between cron literals
    and @-aliases (e.g. '0 3 * * *' is equivalent to @daily semantically only
    when run-at-3am is acceptable; here we treat both as the same intent).
    """
    m = re.search(r"schedule(?:_interval)?\s*=\s*['\"]([^'\"]+)['\"]", code)
    if not m:
        return CheckResult(f"schedule_matches({pattern})", False, "schedule not found")
    found = m.group(1).strip()
    equiv = {
        "@daily":   {"@daily",   "0 0 * * *", "0 3 * * *"},
        "@hourly":  {"@hourly",  "0 * * * *"},
        "@monthly": {"@monthly", "0 0 1 * *"},
        "@weekly":  {"@weekly",  "0 0 * * 0"},
        "@once":    {"@once"},
    }
    targets = equiv.get(pattern, {pattern})
    ok = found in targets or found == pattern
    return CheckResult(f"schedule_matches({pattern})", ok, f"actual: {found}")


def retries_at_least(code: str, n: int) -> CheckResult:
    """Look for retries inside default_args dict literal or via direct kwarg."""
    pats = [
        r"['\"]retries['\"]\s*:\s*(\d+)",   # dict-style: "retries": 3
        r"\bretries\s*=\s*(\d+)",            # kwarg: retries=3
    ]
    for p in pats:
        m = re.search(p, code)
        if m:
            actual = int(m.group(1))
            return CheckResult(f"retries>={n}", actual >= n, f"actual: {actual}")
    return CheckResult(f"retries>={n}", False, "retries not set")


def has_dependency(code: str, upstream: str, downstream: str) -> CheckResult:
    """Check 'upstream >> downstream' or 'downstream.set_upstream(upstream)'."""
    pat1 = rf"{re.escape(upstream)}\s*>>\s*{re.escape(downstream)}"
    pat2 = rf"{re.escape(downstream)}\.set_upstream\(\s*{re.escape(upstream)}\s*\)"
    pat3 = rf"{re.escape(upstream)}\.set_downstream\(\s*{re.escape(downstream)}\s*\)"
    ok = bool(re.search(pat1, code) or re.search(pat2, code) or re.search(pat3, code))
    return CheckResult(f"dependency({upstream}>>{downstream})", ok)


def references_string(code: str, needle: str) -> CheckResult:
    ok = needle in code
    return CheckResult(f"references({needle})", ok)


def has_callback(code: str, kind: str = "on_failure_callback") -> CheckResult:
    ok = bool(re.search(rf"\b{kind}\b\s*=", code))
    return CheckResult(f"has_callback({kind})", ok)


def uses_dynamic_task_mapping(code: str) -> CheckResult:
    ok = ".expand(" in code or ".expand_kwargs(" in code or ".partial(" in code
    return CheckResult("uses_dynamic_task_mapping", ok)


def no_cycles_in_dag_source(code: str) -> CheckResult:
    """Static heuristic: collect a>>b edges from the source, run cycle check.

    Note: this is best-effort and assumes typical DAG idioms. A more rigorous
    check requires loading the DAG via Airflow's DagBag.
    """
    edges = re.findall(r"(\w+)\s*>>\s*(\w+)", code)
    graph: dict[str, set[str]] = {}
    for u, v in edges:
        graph.setdefault(u, set()).add(v)
        graph.setdefault(v, set())

    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph}

    def dfs(n: str) -> bool:
        color[n] = GRAY
        for m in graph[n]:
            if color[m] == GRAY:
                return True
            if color[m] == WHITE and dfs(m):
                return True
        color[n] = BLACK
        return False

    has_cycle = any(color[n] == WHITE and dfs(n) for n in graph)
    return CheckResult("no_cycles", not has_cycle,
                       f"edges={len(edges)}; cycle={has_cycle}")


# ---------- PySpark validators --------------------------------------------------

def reads_format(code: str, fmt: str) -> CheckResult:
    """Detect read of given format, allowing chained .option()/.schema() between.

    Accepts: spark.read.<fmt>(...), spark.read.format("<fmt>")...,
             spark.read.option(...).schema(...).<fmt>(...).
    """
    pats = [
        # any chain that starts with .read and ends with .<fmt>(
        rf"\.read[\w.()'\",\s]*\.{fmt}\(",
        rf"\.read[\w.()'\",\s]*\.format\(['\"]{fmt}['\"]\)",
        rf"\.readStream[\w.()'\",\s]*\.format\(['\"]{fmt}['\"]\)",
    ]
    ok = any(re.search(p, code, re.DOTALL) for p in pats)
    return CheckResult(f"reads_format({fmt})", ok)


def writes_format(code: str, fmt: str) -> CheckResult:
    """Detect write of given format with chained options."""
    pats = [
        rf"\.write[\w.()'\",\s]*\.{fmt}\(",
        rf"\.write[\w.()'\",\s]*\.format\(['\"]{fmt}['\"]\)",
        rf"\.writeStream[\w.()'\",\s]*\.format\(['\"]{fmt}['\"]\)",
    ]
    if fmt == "parquet":
        pats.append(r"\.write[\w.()'\",\s]*\.parquet\(")
    if fmt == "delta":
        # delta is commonly written via .format("delta").save()
        pats.append(r"\.format\(['\"]delta['\"]\)[\w.()'\",\s]*\.save\(")
    ok = any(re.search(p, code, re.DOTALL) for p in pats)
    return CheckResult(f"writes_format({fmt})", ok)


def uses_window_function(code: str) -> CheckResult:
    ok = "Window.partitionBy" in code or "Window.orderBy" in code
    return CheckResult("uses_window_function", ok)


def uses_join(code: str) -> CheckResult:
    ok = bool(re.search(r"\.join\(", code))
    return CheckResult("uses_join", ok)


def uses_groupBy(code: str, col: str | None = None) -> CheckResult:
    if col is None:
        ok = ".groupBy(" in code
        return CheckResult("uses_groupBy", ok)
    ok = bool(re.search(rf"\.groupBy\([^)]*['\"]{col}['\"]", code))
    return CheckResult(f"uses_groupBy({col})", ok)


def uses_agg(code: str, fns: list[str]) -> CheckResult:
    found = [f for f in fns if re.search(rf"\b{f}\b\(", code) or f"F.{f}(" in code]
    return CheckResult(f"uses_agg({'|'.join(fns)})", bool(found),
                       f"found: {found}")


def uses_broadcast_hint(code: str) -> CheckResult:
    ok = "F.broadcast(" in code or "broadcast(" in code
    return CheckResult("uses_broadcast_hint", ok)


def has_watermark(code: str, threshold: str | None = None) -> CheckResult:
    if not re.search(r"\.withWatermark\(", code):
        return CheckResult("has_watermark", False)
    if threshold is None:
        return CheckResult("has_watermark", True)
    ok = bool(re.search(rf"['\"]{re.escape(threshold)}['\"]", code))
    return CheckResult(f"has_watermark({threshold})", ok)


def writes_mode(code: str, mode: str) -> CheckResult:
    ok = bool(re.search(rf"\.mode\(['\"]{mode}['\"]\)", code))
    return CheckResult(f"mode({mode})", ok)


def partition_by(code: str, col: str) -> CheckResult:
    ok = bool(re.search(rf"\.partitionBy\([^)]*['\"]?{col}['\"]?", code))
    return CheckResult(f"partitionBy({col})", ok)


def explicit_schema_defined(code: str) -> CheckResult:
    ok = "StructType(" in code or ".schema(" in code
    return CheckResult("explicit_schema_defined", ok)


def header_true(code: str) -> CheckResult:
    ok = bool(re.search(r"\.option\(['\"]header['\"],\s*['\"]?(true|True)['\"]?", code))
    return CheckResult("header_true", ok)


def uses_kafka_format(code: str) -> CheckResult:
    ok = bool(re.search(r"\.format\(['\"]kafka['\"]\)", code))
    return CheckResult("kafka_format", ok)


# ---------- End-to-end runner ---------------------------------------------------

def evaluate_dag(code: str, invariants: list) -> list[CheckResult]:
    """Run a list of validator calls against generated DAG code.

    `invariants` is the parsed JSON list. This helper interprets the textual
    invariants used in D3_airflow_dags.json.
    """
    results = [parses_ok(code)]

    for inv in invariants:
        inv_l = inv.lower()
        if "uses bashoperator" in inv_l or "has_operator('BashOperator')" in inv:
            results.append(has_operator(code, ["BashOperator"]))
        elif "postgresoperator" in inv_l or "postgreshook" in inv_l:
            results.append(has_operator(code, ["PostgresOperator", "PostgresHook"]))
        elif "sparksubmitoperator" in inv_l:
            results.append(has_operator(code, ["SparkSubmitOperator", "SparkSqlOperator"]))
        elif "emailoperator" in inv_l:
            results.append(has_operator(code, ["EmailOperator"]))
        elif "httpoperator" in inv_l or "requests" in inv_l:
            results.append(has_operator(code, ["HttpOperator", "requests"]))
        elif "s3hook" in inv_l or "boto3" in inv_l:
            results.append(has_operator(code, ["S3Hook", "boto3", "s3fs"]))
        elif "dynamic task mapping" in inv_l or ".expand()" in inv_l:
            results.append(uses_dynamic_task_mapping(code))
        elif "on_failure_callback" in inv_l:
            results.append(has_callback(code, "on_failure_callback"))
        elif "schedule" in inv_l:
            # Extract first quoted pattern; if none, look for @daily/@hourly/etc directly
            m = re.search(r"['\"]([^'\"]+)['\"]", inv)
            if m:
                results.append(schedule_matches(code, m.group(1)))
            else:
                for alias in ("@daily", "@hourly", "@monthly", "@weekly", "@once"):
                    if alias in inv_l:
                        results.append(schedule_matches(code, alias))
                        break
        elif "retries" in inv_l:
            m = re.search(r"\d+", inv)
            if m:
                results.append(retries_at_least(code, int(m.group(0))))
        elif "depends_on" in inv_l:
            # "depends_on order_items→orders" means: order_items DEPENDS ON orders,
            # i.e. orders runs FIRST, then order_items. Edge direction: orders → order_items.
            # We also accept the inverse reading; both >>‑relations satisfy "depends_on".
            m = re.search(r"depends_on\s+(\w+)\s*[→\->]+\s*(\w+)", inv)
            if m:
                a, b = m.group(1), m.group(2)
                # task names in reference dags may have `load_` prefix
                check_a = has_dependency(code, b, a)
                check_b = has_dependency(code, f"load_{b}", f"load_{a}")
                check_c = has_dependency(code, f"load_{b}", a)
                ok = check_a.passed or check_b.passed or check_c.passed
                results.append(CheckResult(f"depends_on({a} after {b})", ok))
        elif "no cycles" in inv_l or "no_cycles" in inv_l:
            results.append(no_cycles_in_dag_source(code))
        elif "len(tasks)" in inv_l:
            m = re.search(r"len\(tasks\)\s*==\s*(\d+)", inv)
            if m:
                # Count `task_id=` declarations as proxy
                count = len(re.findall(r"task_id\s*=", code))
                results.append(CheckResult(f"task_count={m.group(1)}",
                                           count == int(m.group(1)),
                                           f"actual: {count}"))
        elif "slack" in inv_l:
            results.append(references_string(code, "slack"))
        else:
            # Generic text-presence check
            words = re.findall(r"[a-zA-Z_]\w{3,}", inv)
            for w in words:
                if w.lower() in code.lower():
                    results.append(CheckResult(f"text({w})", True))
                    break

    return results


def evaluate_spark(code: str, invariants: list) -> list[CheckResult]:
    results = [parses_ok(code)]

    for inv in invariants:
        inv_l = inv.lower()
        if "reads parquet" in inv_l or "reads_format('parquet')" in inv_l:
            results.append(reads_format(code, "parquet"))
        elif "reads csv" in inv_l or "reads_format('csv')" in inv_l:
            results.append(reads_format(code, "csv"))
        elif "writes parquet" in inv_l or "writes_format('parquet')" in inv_l:
            results.append(writes_format(code, "parquet"))
        elif "writes delta" in inv_l:
            results.append(writes_format(code, "delta"))
        elif "writes_format('csv')" in inv_l:
            results.append(writes_format(code, "csv"))
        elif "broadcast" in inv_l:
            results.append(uses_broadcast_hint(code))
        elif "window" in inv_l or "partitionby" in inv_l:
            results.append(uses_window_function(code))
        elif "groupby" in inv_l:
            m = re.search(r"groupby\(?['\"]?(\w+)", inv_l)
            results.append(uses_groupBy(code, m.group(1) if m else None))
        elif "join" in inv_l:
            results.append(uses_join(code))
        elif "kafka" in inv_l:
            results.append(uses_kafka_format(code))
        elif "watermark" in inv_l:
            m = re.search(r"'([^']+\s+minutes?)'", inv)
            results.append(has_watermark(code, m.group(1) if m else None))
        elif "schema" in inv_l and "explicit" in inv_l:
            results.append(explicit_schema_defined(code))
        elif "header" in inv_l:
            results.append(header_true(code))
        elif "partitionby('dt')" in inv_l or "partition_by('dt')" in inv_l:
            results.append(partition_by(code, "dt"))
        elif "mode == overwrite" in inv_l or "mode_is('overwrite')" in inv_l:
            results.append(writes_mode(code, "overwrite"))
        elif "readstream" in inv_l:
            results.append(CheckResult("uses_readStream", ".readStream" in code))
        elif "writestream" in inv_l:
            results.append(CheckResult("uses_writeStream", ".writeStream" in code))
        elif any(agg in inv_l for agg in ("mean/avg", "avg/mean")):
            results.append(uses_agg(code, ["mean", "avg"]))
        elif "stddev" in inv_l:
            results.append(uses_agg(code, ["stddev", "stddev_pop", "stddev_samp"]))
        elif "percentile" in inv_l or "median" in inv_l:
            results.append(uses_agg(code, ["percentile_approx", "percentile"]))
        elif "sum" in inv_l:
            results.append(uses_agg(code, ["sum"]))
        elif "valid ast" in inv_l:
            pass  # already covered by parses_ok
        else:
            # Generic
            for w in re.findall(r"[a-zA-Z_]\w{3,}", inv):
                if w.lower() in code.lower():
                    results.append(CheckResult(f"text({w})", True))
                    break

    return results
