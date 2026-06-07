"""Render the single-loop echo benchmark chart from results.json.

Single source of truth: bench_uvloop/results.json (written by bench_ci.py --json).
Regenerates the docs/assets/echo-bench.svg chart (uvloop vs zloop, single loop).
The README/docs Markdown tables are curated by hand because they also cover the
io_uring completion backend and the free-threaded results, which this single-loop
pipeline does not measure.

Run: python docs/render_bench.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench_uvloop" / "results.json"
CHART = ROOT / "docs" / "assets" / "echo-bench.svg"

UVLOOP_C = "#9fa8da"  # muted indigo
ZLOOP_C = "#ffb300"  # amber

_MODE_SHORT = {"proto": "proto", "buffered": "buf", "streams": "streams"}


def _size_kib(size: int) -> int:
    return round(size / 1024) or 1


def _bar_label(mode: str, size: int) -> str:
    return f"{_MODE_SHORT.get(mode, mode)} {_size_kib(size)}K"


def render_chart(rows: list[dict]) -> None:
    labels = [_bar_label(r["mode"], r["size"]) for r in rows]
    uvloop = [r["rps"].get("uvloop", 0) / 1000 for r in rows]
    zloop = [r["rps"].get("zloop", 0) / 1000 for r in rows]

    fig, ax = plt.subplots(figsize=(10, 4.2))
    x = range(len(labels))
    w = 0.38
    b1 = ax.bar([i - w / 2 for i in x], uvloop, w, label="uvloop", color=UVLOOP_C)
    b2 = ax.bar([i + w / 2 for i in x], zloop, w, label="zloop", color=ZLOOP_C)

    ax.set_ylabel("requests/sec (thousands)")
    ax.set_title("Echo throughput - higher is better")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=8)
    ax.set_ylim(0, max([*uvloop, *zloop]) * 1.2)
    ax.bar_label(b1, fmt="%.0f", padding=2, fontsize=8, color="#888")
    ax.bar_label(b2, fmt="%.0f", padding=2, fontsize=8, color="#888")
    legend = ax.legend(frameon=False, loc="upper right")
    for text in legend.get_texts():
        text.set_color("#888")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#888")
    ax.tick_params(colors="#888")
    ax.yaxis.label.set_color("#888")
    ax.title.set_color("#888")

    fig.tight_layout()
    fig.savefig(CHART, transparent=True)
    print(f"wrote {CHART.relative_to(ROOT)}")


def main() -> int:
    data = json.loads(RESULTS.read_text())
    rows = data["rows"]
    if not rows:
        print("results.json has no rows yet; nothing to chart")
        return 0
    render_chart(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
