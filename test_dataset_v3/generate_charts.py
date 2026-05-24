"""Generate charts and statistics artifacts for diploma chapter 5.

Produces PNG + SVG figures into ./figures/ and a stats summary in stats.json.
Run from the test_dataset_v3 directory:

    cd test_dataset_v3 && python3 generate_charts.py
"""

import csv
import json
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

# Consistent style for diploma figures
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.dpi": 140,
    "savefig.dpi": 140,
    "savefig.bbox": "tight",
})

PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8",
]


def save(fig: plt.Figure, name: str) -> None:
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"{name}.{ext}")
    plt.close(fig)


def load_csv(name: str) -> list[dict]:
    with open(ROOT / name, newline="") as f:
        return list(csv.DictReader(f))


def load_json(name: str) -> list:
    with open(ROOT / name) as f:
        return json.load(f)


# ---------------- Dataset composition ----------------
CATEGORIES = [
    ("D1", "Intent classifier", 43, "synthesis"),
    ("D2.1", "Text-to-SQL BIRD financial", 106, "open dataset"),
    ("D2.2", "Text-to-SQL Chinook", 20, "open dataset"),
    ("D2.3", "Text-to-SQL Sakila", 15, "open dataset"),
    ("D2.4", "Text-to-SQL demo PG", 7, "custom"),
    ("D2.5", "Text-to-SQL demo MySQL", 6, "custom"),
    ("D2.6", "Text-to-SQL demo ClickHouse", 7, "custom"),
    ("D2.7", "Text-to-Query Mongo", 6, "custom"),
    ("D3", "Airflow DAG specs", 10, "apache/airflow"),
    ("D4", "PySpark task specs", 10, "TPC-H + open"),
    ("D5", "MCP discovery", 10, "custom"),
    ("D6", "Sandbox corpus", 20, "mutation testing"),
    ("D7", "Prompt injection", 15, "OWASP LLM01"),
    ("D8", "Session continuity", 10, "custom"),
    ("D9", "Edge cases", 15, "adversarial"),
    ("D10", "Connection management", 30, "custom"),
    ("D11", "Bash sandbox + Git", 40, "OWASP + custom"),
]


def chart_dataset_composition() -> None:
    labels = [f"{c[0]} ({c[2]})" for c in CATEGORIES]
    sizes = [c[2] for c in CATEGORIES]
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = (PALETTE * 2)[: len(labels)]
    wedges, _ = ax.pie(
        sizes,
        startangle=90,
        colors=colors,
        wedgeprops=dict(width=0.55, edgecolor="white", linewidth=1.2),
    )
    ax.legend(
        wedges,
        labels,
        title=f"Категории датасета (всего {sum(sizes)} кейсов в репрезентативной выдержке)",
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
        fontsize=9,
    )
    ax.set_title("Рис. 5.1 — Состав тестового датасета по категориям")
    save(fig, "fig_5_1_dataset_composition")


def chart_sample_sizes() -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    ids = [c[0] for c in CATEGORIES]
    sizes = [c[2] for c in CATEGORIES]
    colors = (PALETTE * 2)[: len(ids)]
    bars = ax.bar(ids, sizes, color=colors)
    for bar, val in zip(bars, sizes):
        ax.text(bar.get_x() + bar.get_width() / 2, val + 1.5, str(val),
                ha="center", fontsize=9)
    ax.set_ylabel("Количество кейсов")
    ax.set_xlabel("Категория")
    ax.set_title("Рис. 5.2 — Размер репрезентативной выдержки по категориям")
    ax.grid(axis="y", alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=35, ha="right")
    save(fig, "fig_5_2_sample_sizes")


# ---------------- BIRD financial difficulty ----------------
def chart_bird_difficulty() -> None:
    rows = load_csv("D2_1_bird_financial.csv")
    counts = Counter(r["difficulty"] for r in rows)
    order = ["simple", "moderate", "challenging"]
    vals = [counts.get(k, 0) for k in order]
    colors = ["#2ca02c", "#ff7f0e", "#d62728"]
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(order, vals, color=colors)
    for bar, v in zip(bars, vals):
        pct = v / sum(vals) * 100 if sum(vals) else 0
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.5,
                f"{v}\n({pct:.0f}%)", ha="center", fontsize=10)
    ax.set_ylabel("Количество запросов")
    ax.set_xlabel("Уровень сложности (BIRD-Bench)")
    ax.set_title("Рис. 5.3 — Распределение запросов BIRD financial по сложности")
    ax.grid(axis="y", alpha=0.3)
    save(fig, "fig_5_3_bird_difficulty")


# ---------------- Intent distribution D1 ----------------
def chart_intent_distribution() -> None:
    rows = load_csv("D1_intent_classifier.csv")
    counts = Counter(r["expected_intent"] for r in rows)
    items = counts.most_common()
    labels = [k for k, _ in items]
    vals = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(labels, vals, color="#1f77b4")
    for bar, v in zip(bars, vals):
        ax.text(v + 0.05, bar.get_y() + bar.get_height() / 2,
                str(v), va="center", fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Количество запросов")
    ax.set_title("Рис. 5.4 — Распределение D1 по интентам классификатора")
    ax.grid(axis="x", alpha=0.3)
    save(fig, "fig_5_4_intent_distribution")


# ---------------- Attack class distribution D7 ----------------
def chart_attack_classes() -> None:
    rows = load_csv("D7_prompt_injection.csv")
    counts = Counter(r["class"] for r in rows)
    items = counts.most_common()
    labels = [k.replace("_", " ") for k, _ in items]
    vals = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(labels, vals, color="#d62728")
    for bar, v in zip(bars, vals):
        ax.text(v + 0.05, bar.get_y() + bar.get_height() / 2,
                str(v), va="center", fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Количество атак")
    ax.set_title("Рис. 5.5 — Распределение D7 prompt-injection по классам (OWASP LLM01)")
    ax.grid(axis="x", alpha=0.3)
    save(fig, "fig_5_5_attack_classes")


# ---------------- D10 engine distribution ----------------
def chart_d10_engines() -> None:
    rows = load_csv("D10_connection_management.csv")
    counts = Counter(r["engine"] for r in rows)
    items = counts.most_common()
    labels = [k for k, _ in items]
    vals = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(8, 5))
    colors = (PALETTE * 2)[: len(labels)]
    bars = ax.bar(labels, vals, color=colors)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.2,
                str(v), ha="center", fontsize=10)
    ax.set_ylabel("Количество сценариев")
    ax.set_xlabel("Целевой движок СУБД")
    ax.set_title("Рис. 5.6 — D10: покрытие сценариев подключения по движкам")
    ax.grid(axis="y", alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    save(fig, "fig_5_6_d10_engines")


# ---------------- D6 sandbox mutation classes ----------------
def chart_d6_mutations() -> None:
    rows = load_csv("D6_sandbox_corpus.csv")
    counts = Counter(r["mutation_class"] for r in rows if r["expected"] == "broken")
    items = counts.most_common()
    labels = [k.replace("_", " ") for k, _ in items]
    vals = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(labels, vals, color="#9467bd")
    for bar, v in zip(bars, vals):
        ax.text(v + 0.05, bar.get_y() + bar.get_height() / 2,
                str(v), va="center", fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Количество мутаций")
    ax.set_title("Рис. 5.7 — D6: классы мутаций sandbox-корпуса (broken artifacts)")
    ax.grid(axis="x", alpha=0.3)
    save(fig, "fig_5_7_d6_mutations")


# ---------------- Metrics M-01..M-20 targets ----------------
METRICS = [
    ("M-01", "Intent accuracy", 0.90, "[0,1]"),
    ("M-02", "Tool precision macro", 0.92, "[0,1]"),
    ("M-03", "Tool recall macro", 0.88, "[0,1]"),
    ("M-04", "Text-to-SQL EX BIRD", 0.50, "[0,1]"),
    ("M-05", "Text-to-SQL Chinook/Sakila", 0.70, "[0,1]"),
    ("M-06", "Cross-engine SQL validity", 0.90, "[0,1]"),
    ("M-07", "DAG quality (LLM-judge)", 0.75, "[0,1]"),  # 7.5 / 10
    ("M-08", "PySpark quality (LLM-judge)", 0.70, "[0,1]"),  # 7.0 / 10
    ("M-09", "Sandbox catch rate", 0.95, "[0,1]"),
    ("M-10", "Sandbox FPR (≤)", 0.05, "[0,1]"),
    ("M-16", "Prompt injection block", 0.80, "[0,1]"),
    ("M-17", "Halluc tool calls (≤)", 0.05, "[0,1]"),
    ("M-18", "DB connection success", 0.95, "[0,1]"),
    ("M-20", "Git auth rate", 1.00, "[0,1]"),
]


def chart_metrics_targets() -> None:
    fig, ax = plt.subplots(figsize=(11, 7))
    codes = [m[0] for m in METRICS]
    names = [f"{m[0]}: {m[1]}" for m in METRICS]
    targets = [m[2] for m in METRICS]
    # Color: green if target >= 0.9, orange if >= 0.7, red if >= 0.5, blue else
    colors = []
    for t in targets:
        if t >= 0.95:
            colors.append("#2ca02c")
        elif t >= 0.85:
            colors.append("#5fbb6e")
        elif t >= 0.70:
            colors.append("#ff7f0e")
        elif t >= 0.50:
            colors.append("#d62728")
        else:
            colors.append("#1f77b4")
    bars = ax.barh(names, targets, color=colors)
    for bar, v in zip(bars, targets):
        ax.text(v + 0.01, bar.get_y() + bar.get_height() / 2,
                f"{v:.2f}", va="center", fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 1.05)
    ax.set_xlabel("Целевое значение (доля или нормированная оценка)")
    ax.set_title("Рис. 5.8 — Целевые значения ключевых метрик качества M-01…M-20")
    ax.grid(axis="x", alpha=0.3)
    ax.axvline(0.5, linestyle=":", color="gray", linewidth=0.8)
    ax.axvline(0.8, linestyle=":", color="gray", linewidth=0.8)
    save(fig, "fig_5_8_metrics_targets")


# ---------------- Wilson confidence interval illustration ----------------
def chart_wilson_intervals() -> None:
    """Иллюстрация: при n=30 и 50 показать как ширина 95% Wilson-интервала
    зависит от наблюдаемой p̂. Это даёт интуицию почему n_per_intent=30 OK для MVP."""
    import math
    z = 1.96
    def wilson(p, n):
        denom = 1 + z * z / n
        center = (p + z * z / (2 * n)) / denom
        half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
        return center - half, center + half

    ps = [i / 100 for i in range(0, 101, 5)]
    fig, ax = plt.subplots(figsize=(10, 6))
    for n, color, label in [(30, "#d62728", "n = 30 (MVP)"),
                            (50, "#ff7f0e", "n = 50"),
                            (100, "#2ca02c", "n = 100"),
                            (385, "#1f77b4", "n = 385 (Кохран, e=0.05)")]:
        widths = [wilson(p, n)[1] - wilson(p, n)[0] for p in ps]
        ax.plot(ps, widths, label=label, color=color, linewidth=2)
    ax.set_xlabel("Наблюдаемая доля p̂")
    ax.set_ylabel("Ширина 95 % Wilson-интервала")
    ax.set_title("Рис. 5.9 — Зависимость ширины 95 % Wilson-интервала от размера выборки")
    ax.grid(alpha=0.3)
    ax.legend(frameon=False)
    save(fig, "fig_5_9_wilson_widths")


def write_stats() -> None:
    """Сохранить численную сводку — пригодится для текста ПЗ."""
    rows_bird = load_csv("D2_1_bird_financial.csv")
    rows_d1 = load_csv("D1_intent_classifier.csv")
    rows_d7 = load_csv("D7_prompt_injection.csv")
    rows_d6 = load_csv("D6_sandbox_corpus.csv")
    rows_d10 = load_csv("D10_connection_management.csv")
    rows_d11 = load_csv("D11_bash_git_security.csv")

    stats = {
        "categories_total_cases": sum(c[2] for c in CATEGORIES),
        "categories_count": len(CATEGORIES),
        "bird_financial": {
            "total": len(rows_bird),
            "by_difficulty": dict(Counter(r["difficulty"] for r in rows_bird)),
        },
        "d1_intents": {
            "total": len(rows_d1),
            "by_intent": dict(Counter(r["expected_intent"] for r in rows_d1)),
        },
        "d6_sandbox": {
            "total": len(rows_d6),
            "by_label": dict(Counter(r["expected"] for r in rows_d6)),
            "by_mutation_broken": dict(Counter(r["mutation_class"] for r in rows_d6 if r["expected"] == "broken")),
        },
        "d7_prompt_injection": {
            "total": len(rows_d7),
            "by_class": dict(Counter(r["class"] for r in rows_d7)),
        },
        "d10_connection_mgmt": {
            "total": len(rows_d10),
            "by_engine": dict(Counter(r["engine"] for r in rows_d10)),
        },
        "d11_bash_git": {
            "total": len(rows_d11),
        },
        "metrics_targets": {
            m[0]: {"name": m[1], "target": m[2]} for m in METRICS
        },
    }
    (ROOT / "stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> None:
    chart_dataset_composition()
    chart_sample_sizes()
    chart_bird_difficulty()
    chart_intent_distribution()
    chart_attack_classes()
    chart_d10_engines()
    chart_d6_mutations()
    chart_metrics_targets()
    chart_wilson_intervals()
    write_stats()
    figs = sorted((FIG.glob("*.png")))
    print(f"Created {len(figs)} PNG (+ SVG) in {FIG}/")
    for f in figs:
        print(f"  - {f.name}")
    print(f"Stats: {ROOT / 'stats.json'}")


if __name__ == "__main__":
    main()
