"""Сводный график «достигнуто vs цель» с 95% Wilson-интервалами по результатам прогона."""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
RES = ROOT / "results"
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.dpi": 140,
    "savefig.dpi": 140,
    "savefig.bbox": "tight",
})


def collect() -> list[dict]:
    rows = []
    for f in sorted(RES.glob("D*.json")):
        # Skip raw/backup files
        if "raw" in f.stem or "backup" in f.stem or "first_run" in f.stem \
                or "judge" in f.stem:
            continue
        d = json.load(open(f))
        if not isinstance(d, dict):
            continue
        if d.get("category") == "D13":
            rows.append({
                "cat": "D13 Spark (apache/spark)",
                "metric": d.get("metric", ""),
                "n": d.get("n", 0),
                "val": d.get("rate", 0),
                "lo": d.get("wilson_lo", 0),
                "hi": d.get("wilson_hi", 0),
                "target": d.get("target", 0),
                "passed": d.get("passed", False),
            })
            continue
        if d.get("category") == "D12_v2":
            rows.append({
                "cat": "D12 v2 DAG (apache/airflow)",
                "metric": d.get("metric", ""),
                "n": d.get("n", 0),
                "val": d.get("rate", 0),
                "lo": d.get("wilson_lo", 0),
                "hi": d.get("wilson_hi", 0),
                "target": d.get("target", 0),
                "passed": d.get("passed", False),
            })
            continue
        if d.get("category") == "D7_extended":
            rows.append({
                "cat": "D7 ext (deepset)",
                "metric": d.get("metric", ""),
                "n": d.get("n", 0),
                "val": d.get("block_rate", 0),
                "lo": d.get("wilson_lo", 0),
                "hi": d.get("wilson_hi", 0),
                "target": d.get("target", 0),
                "passed": d.get("passed", False),
            })
            continue
        if d.get("category") == "D2_dataset_validation":
            for sub, sd in d.items():
                if isinstance(sd, dict) and "n" in sd:
                    rows.append({
                        "cat": f"D2/{sub}",
                        "metric": "gold SQL execute",
                        "n": sd["n"],
                        "val": sd["rate"],
                        "lo": sd["wilson_lo"],
                        "hi": sd["wilson_hi"],
                        "target": 1.0,
                        "passed": sd["wilson_lo"] >= 1.0,
                    })
            continue
        rows.append({
            "cat": d.get("category", f.stem),
            "metric": d.get("metric", ""),
            "n": d.get("n", 0),
            "val": d.get("accuracy", d.get("rate", d.get("block_rate", 0))),
            "lo": d.get("wilson_lo", 0),
            "hi": d.get("wilson_hi", 0),
            "target": d.get("target", 0),
            "passed": d.get("passed", False),
        })
    return rows


def main() -> None:
    rows = collect()
    fig, ax = plt.subplots(figsize=(12, 7))
    labels = [f"{r['cat']} (n={r['n']})" for r in rows]
    y = list(range(len(rows)))
    vals = [r["val"] * 100 for r in rows]
    lo = [r["lo"] * 100 for r in rows]
    hi = [r["hi"] * 100 for r in rows]
    err_low = [v - l for v, l in zip(vals, lo)]
    err_high = [h - v for v, h in zip(vals, hi)]
    colors = ["#2ca02c" if r["passed"] else "#d62728" for r in rows]
    ax.barh(y, vals, xerr=[err_low, err_high], color=colors, capsize=4, alpha=0.85)
    # Target markers
    for yi, r in zip(y, rows):
        ax.scatter(r["target"] * 100, yi, marker="|", color="black", s=400, zorder=3)
        ax.text(102, yi, f"тгт {r['target']*100:.0f}%", va="center", fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlim(0, 115)
    ax.set_xlabel("Достигнутая доля (95% Wilson CI), %")
    ax.set_title("Результаты прогона датасета: достигнуто vs целевое значение")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    ax.axvline(50, linestyle=":", color="gray", linewidth=0.7)
    ax.axvline(80, linestyle=":", color="gray", linewidth=0.7)
    # Legend
    import matplotlib.patches as mpatches
    pass_p = mpatches.Patch(color="#2ca02c", label="Wilson нижняя ≥ target")
    fail_p = mpatches.Patch(color="#d62728", label="Wilson нижняя < target")
    ax.legend(handles=[pass_p, fail_p], loc="lower right", frameon=False)
    out = FIG / "fig_5_10_results_target_vs_achieved"
    for ext in ("png", "svg"):
        fig.savefig(f"{out}.{ext}")
    plt.close(fig)
    print(f"Saved {out}.png/.svg")


if __name__ == "__main__":
    main()
