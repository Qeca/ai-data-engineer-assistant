"""Скачать 30+ реальных PySpark скриптов из apache/spark @ pinned SHA."""

import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "D13_real_spark_scripts"
OUT.mkdir(exist_ok=True)

# pinned apache/spark commit (same as D4)
SHA = "b2c2a8d68dcbbaca715adc74c0dd543582c9ff02"
BASE = f"https://raw.githubusercontent.com/apache/spark/{SHA}/"

# 33 разнообразных PySpark скриптов: core SQL/streaming/ML/batch
SCRIPTS = [
    # ---- Core examples ----
    "examples/src/main/python/wordcount.py",
    "examples/src/main/python/sort.py",
    "examples/src/main/python/pi.py",
    "examples/src/main/python/kmeans.py",
    "examples/src/main/python/logistic_regression.py",
    "examples/src/main/python/pagerank.py",
    "examples/src/main/python/transitive_closure.py",
    "examples/src/main/python/avro_inputformat.py",
    "examples/src/main/python/parquet_inputformat.py",
    "examples/src/main/python/status_api_demo.py",
    # ---- SQL ----
    "examples/src/main/python/sql/basic.py",
    "examples/src/main/python/sql/arrow.py",
    "examples/src/main/python/sql/datasource.py",
    "examples/src/main/python/sql/hive.py",
    "examples/src/main/python/sql/jdbc.py",
    # ---- Streaming ----
    "examples/src/main/python/sql/streaming/structured_kafka_wordcount.py",
    "examples/src/main/python/sql/streaming/structured_network_wordcount.py",
    "examples/src/main/python/sql/streaming/structured_network_wordcount_session_window.py",
    "examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py",
    "examples/src/main/python/sql/streaming/structured_sessionization.py",
    # ---- ML (выбираю представительные) ----
    "examples/src/main/python/ml/als_example.py",
    "examples/src/main/python/ml/kmeans_example.py",
    "examples/src/main/python/ml/linear_regression_with_elastic_net.py",
    "examples/src/main/python/ml/decision_tree_classification_example.py",
    "examples/src/main/python/ml/random_forest_classifier_example.py",
    "examples/src/main/python/ml/gradient_boosted_tree_classifier_example.py",
    "examples/src/main/python/ml/naive_bayes_example.py",
    "examples/src/main/python/ml/pipeline_example.py",
    "examples/src/main/python/ml/cross_validator.py",
    "examples/src/main/python/ml/tf_idf_example.py",
    "examples/src/main/python/ml/word2vec_example.py",
    "examples/src/main/python/ml/standard_scaler_example.py",
    "examples/src/main/python/ml/pca_example.py",
]


def fetch(path: str) -> tuple[str, str]:
    name = Path(path).name
    idx = len(list(OUT.glob("*.py"))) + 1
    out_path = OUT / f"{idx:02d}_{name}"
    url = BASE + path
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = r.read().decode("utf-8", "replace")
        out_path.write_text(data, encoding="utf-8")
        return name, str(out_path.relative_to(ROOT))
    except Exception as e:
        return name, f"ERROR: {e}"


def main() -> None:
    log = []
    for path in SCRIPTS:
        name, result = fetch(path)
        log.append({"source_path": path, "local": result})
        print(f"  {name:55s} → {result}")
    (OUT / "manifest.json").write_text(json.dumps({"sha": SHA, "files": log},
                                                  ensure_ascii=False, indent=2))
    ok = sum(1 for x in log if not str(x["local"]).startswith("ERROR"))
    print(f"\nFetched {ok}/{len(SCRIPTS)} → {OUT}")


if __name__ == "__main__":
    main()
