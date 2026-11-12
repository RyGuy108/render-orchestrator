"""Charts for the README from results/*.json.   python -m bench.plot"""
from __future__ import annotations

import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

R = "results"
INK, MUTED, GRID = "#1f2328", "#6e7781", "#e5e7eb"
BASE, ORCH = "#c2410c", "#2563eb"


def load(name):
    p = f"{R}/{name}.json"
    return json.load(open(p)) if os.path.exists(p) else None


def style(ax, title):
    ax.set_title(title, loc="left", fontsize=11, color=INK, fontweight="bold")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def main():
    runs = [("naive", "orch", "steady"), ("naive-chaos", "orch-chaos", "with crashes")]
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8), dpi=150)

    # 1) success rate
    ax = axes[0]
    labels, b, o = [], [], []
    for nb, no, lab in runs:
        rb, ro = load(nb), load(no)
        if rb and ro:
            labels.append(lab); b.append(rb["success_rate_pct"]); o.append(ro["success_rate_pct"])
    x = range(len(labels))
    ax.bar([i - 0.2 for i in x], b, 0.4, color=BASE, label="direct integration")
    ax.bar([i + 0.2 for i in x], o, 0.4, color=ORCH, label="orchestrator")
    for i, (vb, vo) in enumerate(zip(b, o)):
        ax.text(i - 0.2, vb + 0.3, f"{vb:.1f}%", ha="center", fontsize=8, color=INK)
        ax.text(i + 0.2, vo + 0.3, f"{vo:.1f}%", ha="center", fontsize=8, color=INK)
    ax.set_xticks(list(x), labels)
    ax.set_ylim(min(b + o + [90]) - 5, 101.5)
    style(ax, "Jobs that succeed (10k jobs)")


    # 2) small-tenant p99 latency
    ax = axes[1]
    b = [load(nb)["latency_s_small_tenants"]["p99"] for nb, no, _ in runs if load(nb) and load(no)]
    o = [load(no)["latency_s_small_tenants"]["p99"] for nb, no, _ in runs if load(nb) and load(no)]
    ax.bar([i - 0.2 for i in x], b, 0.4, color=BASE)
    ax.bar([i + 0.2 for i in x], o, 0.4, color=ORCH)
    axes[2].set_ylim(50, 100)
    for i, (vb, vo) in enumerate(zip(b, o)):
        ax.text(i - 0.2, vb * 1.02, f"{vb:.0f}s", ha="center", fontsize=8, color=INK)
        ax.text(i + 0.2, vo * 1.02 + 1, f"{vo:.1f}s", ha="center", fontsize=8, color=INK)
    ax.set_xticks(list(x), labels)
    style(ax, "p99 latency, small customers")

    # 3) AIMD limit trace
    ax = axes[2]
    ro = load("orch")
    if ro:
        tr = [p for p in ro["limit_trace"] if p["limit"]]
        ax.plot([p["t"] for p in tr], [p["limit"] for p in tr], color=ORCH, linewidth=1.6)
        ax.axhline(64, color=MUTED, linestyle="--", linewidth=1)
        ax.text(120, 55.5, "provider's concurrency cap (hidden)", fontsize=8, color=MUTED)
        ax.set_xlabel("seconds", color=MUTED, fontsize=9)
    style(ax, "Adaptive in-flight limit (AIMD)")

    ax.set_ylim(top=None)
    fig.legend(*axes[0].get_legend_handles_labels(), loc="lower center", ncol=2, frameon=False, fontsize=9)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(f"{R}/summary.png")
    print("wrote results/summary.png")


if __name__ == "__main__":
    main()
