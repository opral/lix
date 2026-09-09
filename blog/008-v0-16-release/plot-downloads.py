"""Render npm's reported weekly totals: python -m pip install matplotlib."""
import json
from datetime import date, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

root = Path(__file__).parent
data = json.loads((root / "npm-downloads.json").read_text())
days = data["downloads"]
daily = {date.fromisoformat(day["day"]): day["downloads"] for day in days}
# Start with the first complete Monday–Sunday week in January.
start = date(2026, 1, 5)
end = date.fromisoformat(data["end"])
weeks, totals = [], []
while start + timedelta(days=6) <= end:
    weeks.append(start + timedelta(days=6))
    totals.append(sum(daily[start + timedelta(days=i)] for i in range(7)))
    start += timedelta(days=7)
assert totals[-1] == 509341

fig, ax = plt.subplots(figsize=(12, 6.3), dpi=160)
fig.patch.set_facecolor("#FAFAF7")
ax.set_facecolor("#FAFAF7")
fig.subplots_adjust(left=.09, right=.97, top=.70, bottom=.19)
fig.text(.09, .90, "@lix-js/sdk", fontsize=16, color="#555555")
fig.text(.09, .81, "509,341 weekly downloads", fontsize=29, weight="bold", color="#0A0A0A")
ax.plot(weeks, totals, color="#08B5D6", linewidth=3, zorder=3)
ax.scatter(weeks[-1], totals[-1], color="#08B5D6", s=45, zorder=4)
ax.set_xlim(date(2026, 1, 1), end + timedelta(days=10))
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
ax.set_ylim(0, max(600000, max(totals) * 1.15))
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: "0" if v == 0 else f"{v / 1000:.0f}k"))
ax.grid(axis="y", color="#E4E4E0", zorder=0)
ax.tick_params(axis="both", length=0, labelsize=11, pad=10, colors="#555555")
for spine in ax.spines.values():
    spine.set_visible(False)
ax.annotate(f"{totals[-1]:,}", (weeks[-1], totals[-1]), xytext=(-5, 12),
            textcoords="offset points", ha="right", fontsize=13, color="#0A0A0A")
fig.text(.09, .08, "Weekly npm downloads · Jan 5–Sep 6, 2026 · complete Monday–Sunday weeks", fontsize=11, color="#555555")
fig.text(.09, .035, "Source: api.npmjs.org · retrieved Sep 9, 2026 · reported totals, all package versions", fontsize=9, color="#777777")
fig.savefig(root / "npm-downloads.png", facecolor=fig.get_facecolor())
print(f"{len(weeks)} complete weeks; latest total: {totals[-1]:,}")
