"""Regenerate the echo-throughput chart in docs/assets/echo-bench.svg.

Run: python docs/gen_bench_chart.py
Numbers come from bench_uvloop/run_matrix.sh (macOS arm64, best of 3).
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

LABELS = [
    "proto 1K", "buf 1K", "streams 1K",
    "proto 10K", "buf 10K", "streams 10K",
    "proto 100K", "buf 100K", "streams 100K",
]
UVLOOP = [114, 114, 93, 116, 109, 89, 55, 56, 43]
ZLOOP = [117, 121, 95, 115, 119, 90, 52, 55, 44]

# Brand-ish colours chosen to read on both light and dark backgrounds.
UVLOOP_C = "#9fa8da"  # muted indigo
ZLOOP_C = "#ffb300"  # amber

fig, ax = plt.subplots(figsize=(10, 4.2))
x = range(len(LABELS))
w = 0.38
b1 = ax.bar([i - w / 2 for i in x], UVLOOP, w, label="uvloop", color=UVLOOP_C)
b2 = ax.bar([i + w / 2 for i in x], ZLOOP, w, label="zloop", color=ZLOOP_C)

ax.set_ylabel("requests/sec (thousands)")
ax.set_title("Echo throughput - higher is better")
ax.set_xticks(list(x))
ax.set_xticklabels(LABELS, rotation=20, ha="right", fontsize=8)
ax.set_ylim(0, 140)
ax.bar_label(b1, padding=2, fontsize=8, color="#888")
ax.bar_label(b2, padding=2, fontsize=8, color="#888")
legend = ax.legend(frameon=False, loc="upper right")
for text in legend.get_texts():
    text.set_color("#888")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
# Transparent background so it works on both light and dark site themes.
fig.patch.set_alpha(0)
ax.patch.set_alpha(0)
for spine in ("left", "bottom"):
    ax.spines[spine].set_color("#888")
ax.tick_params(colors="#888")
ax.yaxis.label.set_color("#888")
ax.title.set_color("#888")

fig.tight_layout()
fig.savefig("docs/assets/echo-bench.svg", transparent=True)
print("wrote docs/assets/echo-bench.svg")
