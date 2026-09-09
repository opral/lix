"""Render npm's reported weekly totals: python -m pip install matplotlib."""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

root = Path(__file__).parent
data = json.loads((root / "npm-downloads.json").read_text())
days = data["downloads"]
assert len(days) == 28
totals = [sum(day["downloads"] for day in days[i:i + 7]) for i in range(0, 28, 7)]
assert totals[-1] == 509341
labels = ["Aug 10–16", "Aug 17–23", "Aug 24–30", "Aug 31–Sep 6"]

fig, ax = plt.subplots(figsize=(12, 6.3), dpi=160)
fig.patch.set_facecolor("#FAFAF7")
ax.set_facecolor("#FAFAF7")
fig.subplots_adjust(left=.09, right=.97, top=.70, bottom=.19)
fig.text(.09, .90, "@lix-js/sdk", fontsize=16, color="#555555")
fig.text(.09, .81, "509,341 weekly downloads", fontsize=29, weight="bold", color="#0A0A0A")
ax.plot(labels, totals, color="#08B5D6", linewidth=3, marker="o", markersize=8, zorder=3)
ax.margins(x=.08)
ax.set_ylim(0, 600000)
ax.set_yticks([0, 200000, 400000, 600000])
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: "0" if v == 0 else f"{v / 1000:.0f}k"))
ax.grid(axis="y", color="#E4E4E0", zorder=0)
ax.tick_params(axis="both", length=0, labelsize=11, pad=10, colors="#555555")
for spine in ax.spines.values():
    spine.set_visible(False)
for index, total in enumerate(totals):
    ax.text(index, total + 15000, f"{total:,}",
            ha="center", va="bottom", fontsize=13, color="#0A0A0A")
fig.text(.09, .08, "Weekly npm downloads · Monday–Sunday · 2026", fontsize=11, color="#555555")
fig.text(.09, .035, "Source: api.npmjs.org · retrieved Sep 9, 2026 · reported totals, all package versions", fontsize=9, color="#777777")
fig.savefig(root / "npm-downloads.png", facecolor=fig.get_facecolor())
print(dict(zip(labels, totals)))
