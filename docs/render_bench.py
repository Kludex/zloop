"""Render the echo benchmark results into the chart and the README/docs tables.

Single source of truth: bench_uvloop/results.json (written by bench_ci.py --json).
Regenerates docs/assets/echo-bench.svg and splices the Markdown table between the
`<!-- BENCH:START -->` / `<!-- BENCH:END -->` markers in README.md and the docs
performance page.

Run: python docs/render_bench.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench_uvloop" / "results.json"
CHART = ROOT / "docs" / "assets" / "echo-bench.svg"
README = ROOT / "README.md"
DOCS_PERF = ROOT / "docs" / "reference" / "performance.md"

START = "<!-- BENCH:START -->"
END = "<!-- BENCH:END -->"

UVLOOP_C = "#9fa8da"  # muted indigo
ZLOOP_C = "#ffb300"  # amber

_MODE_SHORT = {"proto": "proto", "buffered": "buf", "streams": "streams"}


def _size_kib(size: int) -> int:
    return round(size / 1024) or 1


def _bar_label(mode: str, size: int) -> str:
    return f"{_MODE_SHORT.get(mode, mode)} {_size_kib(size)}K"


def _k(rps: float) -> str:
    return f"{rps / 1000:.0f}k"


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


def render_table(rows: list[dict], loops: list[str]) -> str:
    lines = [
        "| Message | Server mode | " + " | ".join(loops) + " | zloop vs uvloop |",
        "| --- | --- | " + " | ".join(["---:"] * len(loops)) + " | ---: |",
    ]
    for r in rows:
        rps = r["rps"]
        best = max((rps.get(loop, 0) for loop in loops), default=0)
        cells = []
        for loop in loops:
            v = rps.get(loop, 0)
            cell = _k(v)
            cells.append(f"**{cell}**" if v == best and v else cell)
        delta = ""
        if rps.get("uvloop"):
            pct = round((rps.get("zloop", 0) / rps["uvloop"] - 1) * 100)
            sign = "+" if pct > 0 else ""
            delta = f"**{sign}{pct}%**" if pct > 0 else f"{sign}{pct}%"
        msg = f"{_size_kib(r['size'])} KiB"
        lines.append(f"| {msg} | {r['mode']} | " + " | ".join(cells) + f" | {delta} |")
    return "\n".join(lines)


def splice(path: Path, table: str) -> None:
    text = path.read_text()
    pattern = re.compile(re.escape(START) + r".*?" + re.escape(END), re.DOTALL)
    if not pattern.search(text):
        raise RuntimeError(f"{path.relative_to(ROOT)} has no {START} / {END} markers")
    path.write_text(pattern.sub(f"{START}\n{table}\n{END}", text))
    print(f"spliced table into {path.relative_to(ROOT)}")


def main() -> int:
    data = json.loads(RESULTS.read_text())
    rows = data["rows"]
    loops = data["meta"]["loops"]
    if not rows:
        print("results.json has no rows yet; leaving the BENCH placeholders in place")
        return 0
    render_chart(rows)
    table = render_table(rows, loops)
    splice(README, table)
    splice(DOCS_PERF, table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
