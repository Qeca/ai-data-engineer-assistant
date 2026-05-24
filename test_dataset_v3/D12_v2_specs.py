"""Извлечь NL-спецификации и инварианты из real-world DAG скачанных из apache/airflow.

Каждый сценарий → словарь:
  id, source_file, source_sha, license, spec (NL), invariants (list)
Спека формируется из:
  1) module docstring (если есть)
  2) первого comment-блока
  3) имени файла
  4) перечисления используемых операторов и количества задач
"""

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DAGS = ROOT / "D12_real_dags"
SHA = "ea7481d7d59b0eb129f8b39c848a24aa111e7ca3"


def extract_docstring(tree: ast.AST) -> str | None:
    if isinstance(tree, ast.Module) and tree.body and isinstance(tree.body[0], ast.Expr) \
            and isinstance(tree.body[0].value, ast.Constant) and isinstance(tree.body[0].value.value, str):
        return tree.body[0].value.value.strip()
    return None


def find_used_operators(source: str) -> list[str]:
    """Найти классы операторов, используемые в коде."""
    ops = re.findall(r"\b([A-Z][A-Za-z]+Operator)\b", source)
    return sorted(set(ops))


def find_used_decorators(source: str) -> list[str]:
    decs = re.findall(r"@(task|dag|task_group|setup|teardown|asset|sla_miss_callback)\b", source)
    return sorted(set(decs))


def find_schedule(source: str) -> str | None:
    m = re.search(r"schedule\s*=\s*['\"]?([^'\",\)\n]+)['\"]?", source)
    return m.group(1).strip() if m else None


def count_tasks(source: str) -> int:
    # Грубая оценка: каждое создание Operator-инстанса или @task функция
    op_inst = len(re.findall(r"=\s*[A-Z][A-Za-z]+Operator\(", source))
    task_decs = len(re.findall(r"@task\b", source))
    return op_inst + task_decs


def derive_spec(path: Path) -> dict:
    src = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = None
    doc = extract_docstring(tree) if tree else None
    ops = find_used_operators(src)
    decs = find_used_decorators(src)
    schedule = find_schedule(src)
    n_tasks = count_tasks(src)

    # Нормализуем docstring (берём первый абзац)
    doc_short = ""
    if doc:
        # Удаляем Apache license header если он попал
        cleaned = "\n".join(
            ln for ln in doc.splitlines()
            if "Licensed to the Apache" not in ln
            and "ASF" not in ln
            and "License is distributed" not in ln
            and "License at" not in ln
            and "License." not in ln
            and ".. note::" not in ln
            and not ln.strip().startswith("http")
        ).strip()
        doc_short = cleaned.split("\n\n")[0].strip()[:600]

    # NL spec: соединяем docstring + структурные подсказки
    nl_parts = []
    if doc_short:
        nl_parts.append(doc_short)
    if ops:
        nl_parts.append(f"Используй операторы: {', '.join(ops[:6])}.")
    if decs and not ops:
        nl_parts.append(f"Используй декораторы Airflow: {', '.join('@' + d for d in decs)}.")
    if n_tasks > 1:
        nl_parts.append(f"DAG должен содержать {n_tasks} задач(и).")
    if schedule:
        nl_parts.append(f"Расписание: {schedule}.")
    nl_parts.append("Сгенерируй валидный Airflow DAG и проверь его в sandbox.")
    spec = " ".join(nl_parts)

    # Invariants for invariants.py-style validation
    invariants = ["valid_python_ast"]
    if ops:
        invariants.append({"has_operator": ops[:4]})
    if "task" in decs:
        invariants.append("uses_taskflow_task")
    if "dag" in decs:
        invariants.append("uses_dag_decorator")
    if "task_group" in decs:
        invariants.append("uses_taskgroup")
    if schedule and schedule not in ("None", "null"):
        invariants.append({"schedule_equals": schedule})

    return {
        "id": path.stem,
        "source_file": f"D12_real_dags/{path.name}",
        "source_repo": "apache/airflow",
        "source_sha": SHA,
        "source_license": "Apache-2.0",
        "operators_used": ops,
        "decorators_used": decs,
        "schedule": schedule,
        "n_tasks_observed": n_tasks,
        "spec": spec,
        "invariants": invariants,
        "must_artifact_type": "dag",
    }


def main() -> None:
    specs: list[dict] = []
    for f in sorted(DAGS.glob("*.py")):
        s = derive_spec(f)
        specs.append(s)
    out = ROOT / "D12_v2_pipeline_specs.json"
    out.write_text(json.dumps(specs, ensure_ascii=False, indent=2))
    print(f"Generated {len(specs)} specs → {out.name}")
    # Sample
    print("\nSample [first]:")
    print(json.dumps(specs[0], ensure_ascii=False, indent=2)[:500])
    print("\nSample [most complex by operator count]:")
    most_ops = max(specs, key=lambda s: len(s["operators_used"]))
    print(json.dumps(most_ops, ensure_ascii=False, indent=2)[:500])


if __name__ == "__main__":
    main()
