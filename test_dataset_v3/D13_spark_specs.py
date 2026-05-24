"""Извлечь NL-спецификации и инварианты из real PySpark scripts (apache/spark)."""

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT / "D13_real_spark_scripts"
SHA = "b2c2a8d68dcbbaca715adc74c0dd543582c9ff02"


def docstring(tree):
    if isinstance(tree, ast.Module) and tree.body and isinstance(tree.body[0], ast.Expr) \
            and isinstance(tree.body[0].value, ast.Constant) \
            and isinstance(tree.body[0].value.value, str):
        return tree.body[0].value.value.strip()
    return None


def find_classes(src: str) -> list[str]:
    """Найти Spark классы и функции которые упоминаются."""
    sparks = []
    patterns = [
        r"\bSparkSession\b", r"\bSparkContext\b", r"\bStreamingContext\b",
        r"\bDataFrame\b", r"\.createDataFrame\b", r"\.read\.(parquet|json|csv|jdbc|avro|orc|format)",
        r"\.write\.(parquet|json|csv|jdbc|saveAsTable|format)",
        r"\bGroupedData\b", r"\.agg\(", r"\.groupBy\(", r"\.window\(",
        r"\bml\.classification\b", r"\bml\.regression\b", r"\bml\.clustering\b",
        r"\bml\.feature\b", r"\bml\.recommendation\b", r"\bml\.evaluation\b",
        r"\bml\.tuning\b", r"\bPipeline\(", r"\bCrossValidator\(",
        r"\breadStream\b", r"\bwriteStream\b",
        r"\bkafka\b", r"\bsocket\b",
        r"\bsql\(", r"\.collect\(",
    ]
    for p in patterns:
        if re.search(p, src):
            sparks.append(p.strip("\\b().*?"))
    return list(set(sparks))


def find_ml_imports(src: str) -> list[str]:
    return list(set(re.findall(r"from pyspark\.ml\.(\w+)", src)))


def find_top_classes_used(src: str) -> list[str]:
    """Топ-используемые pyspark классы."""
    return list(set(re.findall(r"\b([A-Z][A-Za-z]+)\(", src)))[:8]


def topic_keyword(name: str, doc: str | None) -> str:
    """Высокоуровневая категория: streaming/ml/sql/core."""
    n = name.lower()
    if "streaming" in n or "stream" in (doc or "").lower():
        return "streaming"
    if "ml" in n or "_example" in n:
        return "ml"
    if any(x in n for x in ("sql", "basic", "datasource", "hive", "jdbc", "arrow")):
        return "sql"
    return "core"


def derive(path: Path) -> dict:
    src = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = None
    doc = docstring(tree) if tree else None
    # Удалим лицензионный header из docstring
    if doc:
        doc = "\n".join(ln for ln in doc.splitlines()
                        if "Apache License" not in ln
                        and "Licensed to the Apache" not in ln
                        and "WITHOUT WARRANTIES" not in ln
                        and "implied" not in ln
                        and "License at" not in ln
                        and "License." not in ln
                        and "http://www.apache.org" not in ln
                        and "may not use this file" not in ln
                        and "Unless required" not in ln).strip()
        # Берём первый абзац
        doc = doc.split("\n\n")[0].strip()[:500]
    ml_mods = find_ml_imports(src)
    classes = find_top_classes_used(src)
    topic = topic_keyword(path.name, doc)

    nl_parts = []
    if doc:
        nl_parts.append(f"Описание задачи: {doc}")
    else:
        nl_parts.append(f"Сгенерируй PySpark-скрипт по образцу {path.stem.split('_', 1)[-1]}.")
    if ml_mods:
        nl_parts.append(f"Используй модули PySpark ML: {', '.join('pyspark.ml.' + m for m in ml_mods[:3])}.")
    if topic == "streaming":
        nl_parts.append("Используй Structured Streaming API (readStream/writeStream).")
    elif topic == "sql":
        nl_parts.append("Используй SparkSession и DataFrame API.")
    elif topic == "ml":
        nl_parts.append("Используй pyspark.ml для построения модели.")
    if classes:
        nl_parts.append(f"Ключевые классы: {', '.join(classes[:6])}.")
    nl_parts.append("Скрипт должен импортировать pyspark и быть готовым к spark-submit.")
    spec = " ".join(nl_parts)

    inv = ["valid_python_ast", "imports_pyspark"]
    if topic == "streaming":
        inv.append("uses_streaming")
    if topic == "ml" or ml_mods:
        inv.append("uses_pyspark_ml")
    if topic == "sql":
        inv.append("uses_spark_session")
    if classes:
        inv.append({"has_class_or_call": classes[:4]})

    return {
        "id": path.stem,
        "source_file": f"D13_real_spark_scripts/{path.name}",
        "source_repo": "apache/spark",
        "source_sha": SHA,
        "source_license": "Apache-2.0",
        "topic": topic,
        "ml_modules": ml_mods,
        "classes_used": classes,
        "spec": spec,
        "invariants": inv,
        "must_artifact_type": "spark",
    }


def main() -> None:
    specs = [derive(f) for f in sorted(SCRIPTS.glob("*.py"))]
    out = ROOT / "D13_spark_specs.json"
    out.write_text(json.dumps(specs, ensure_ascii=False, indent=2))
    print(f"Generated {len(specs)} specs → {out.name}")
    print("\nTopic distribution:")
    from collections import Counter
    print(Counter(s["topic"] for s in specs))
    print("\nSample spec [first]:")
    print(json.dumps(specs[0], ensure_ascii=False, indent=2)[:600])


if __name__ == "__main__":
    main()
