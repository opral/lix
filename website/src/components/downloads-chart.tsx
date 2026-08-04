import { useMemo, useRef, useState } from "react";
import npmDownloads from "../npm-downloads.gen.json";

const VW = 1000;
const VH = 150;
const PAD = 6;

const ranges = [
  { key: "90d", label: "90d", weeks: 13 },
  { key: "6m", label: "6m", weeks: 26 },
  { key: "12m", label: "12M", weeks: 52 },
] as const;

type RangeKey = (typeof ranges)[number]["key"];

type Week = { weekEnding: string; downloads: number };

const allWeeks: Week[] = npmDownloads.weeks;

/** Latest full-week download count, rounded down to the nearest 10k (e.g. ">440k"). */
export function coarseWeeklyDownloads(): string {
  const latest = npmDownloads.latestWeeklyDownloads;
  return `>${Math.floor(latest / 10_000) * 10}k`;
}

function formatWeekOf(weekEnding: string): string {
  const end = new Date(`${weekEnding}T00:00:00Z`);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 6);
  return start.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  });
}

/**
 * Weekly npm downloads chart with 90d / 6m / 12M ranges, rendered from the
 * statically generated npm-downloads.gen.json.
 *
 * @example
 * <DownloadsChart />
 */
export function DownloadsChart() {
  const [range, setRange] = useState<RangeKey>("12m");
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const wrapRef = useRef<HTMLDivElement>(null);

  const data = useMemo(() => {
    const take = ranges.find((r) => r.key === range)?.weeks ?? 52;
    return allWeeks.slice(Math.max(0, allWeeks.length - take));
  }, [range]);

  const { points, ticks } = useMemo(() => {
    const values = data.map((week) => week.downloads);
    const max = Math.max(...values);
    const min = Math.min(...values);
    const floor = min * 0.4;
    const span = Math.max(1, max - floor);
    const toY = (value: number) =>
      VH - PAD - ((value - floor) / span) * (VH - PAD * 2);
    const points = data.map((week, i) => {
      const x = data.length === 1 ? VW : (i / (data.length - 1)) * VW;
      return [x, toY(week.downloads)] as const;
    });

    // Pick a "nice" tick step that yields at most ~5 axis labels.
    const stepCandidates = [1, 2, 2.5, 5]
      .flatMap((base) =>
        [1_000, 10_000, 100_000, 1_000_000].map(
          (magnitude) => base * magnitude,
        ),
      )
      .sort((a, b) => a - b);
    const step =
      stepCandidates.find((candidate) => span / candidate <= 5) ??
      stepCandidates[stepCandidates.length - 1];
    const ticks: Array<{ value: number; y: number }> = [];
    for (
      let value = Math.ceil(floor / step) * step;
      value <= max;
      value += step
    ) {
      ticks.push({ value, y: toY(value) });
    }

    return { points, ticks };
  }, [data]);

  const linePath = points
    .map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)} ${y.toFixed(1)}`)
    .join(" ");
  const areaPath = `${linePath} L${VW} ${VH} L0 ${VH} Z`;
  const last = points[points.length - 1];

  const startLabel = useMemo(() => {
    const first = data[0];
    if (!first) return "";
    const start = new Date(`${first.weekEnding}T00:00:00Z`);
    start.setUTCDate(start.getUTCDate() - 6);
    return start
      .toLocaleDateString("en-US", {
        month: "long",
        year: "numeric",
        timeZone: "UTC",
      })
      .toLowerCase();
  }, [data]);

  const throughLabel = useMemo(() => {
    const lastWeek = allWeeks[allWeeks.length - 1];
    if (!lastWeek) return "";
    return new Date(`${lastWeek.weekEnding}T00:00:00Z`).toLocaleDateString(
      "en-US",
      { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" },
    );
  }, []);

  const onMouseMove = (event: React.MouseEvent<HTMLDivElement>) => {
    const wrap = wrapRef.current;
    if (!wrap || points.length === 0) return;
    const rect = wrap.getBoundingClientRect();
    const frac = Math.min(
      1,
      Math.max(0, (event.clientX - rect.left) / rect.width),
    );
    setHoverIdx(Math.round(frac * (data.length - 1)));
  };

  const hovered = hoverIdx !== null ? data[hoverIdx] : null;
  const hoveredPoint = hoverIdx !== null ? points[hoverIdx] : null;

  return (
    <section className="pt-10">
      <div className="mb-5 flex flex-wrap items-end justify-between gap-6">
        <div className="flex flex-col gap-1.5">
          <h2 className="text-[15px] font-semibold tracking-[-0.01em]">
            Adoption
          </h2>
          <p className="text-[13.5px] text-ink-muted">
            Weekly npm downloads of{" "}
            <span className="font-mono text-[12.5px] text-ink-secondary">
              @lix-js/sdk
            </span>
          </p>
        </div>
        <div className="inline-flex overflow-hidden rounded-lg border border-line-strong bg-white shadow-[0_1px_2px_rgba(20,23,26,0.04)]">
          {ranges.map((r) => (
            <button
              key={r.key}
              onClick={() => {
                setRange(r.key);
                setHoverIdx(null);
              }}
              className={`cursor-pointer px-3 py-[7px] font-mono text-[11px] leading-none transition-colors ${
                range === r.key
                  ? "bg-ink text-paper"
                  : "bg-transparent text-ink-faint"
              }`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      <div
        ref={wrapRef}
        className="relative cursor-crosshair"
        onMouseMove={onMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        <svg
          viewBox={`0 0 ${VW} ${VH}`}
          className="block h-auto w-full overflow-visible"
          aria-label="Weekly npm downloads of @lix-js/sdk"
        >
          {ticks.map((tick) => (
            <line
              key={tick.value}
              x1="0"
              y1={tick.y.toFixed(1)}
              x2={VW}
              y2={tick.y.toFixed(1)}
              stroke="#E6E4DD"
              strokeWidth="1"
              vectorEffect="non-scaling-stroke"
            />
          ))}
          <path d={areaPath} fill="rgba(7,182,213,0.10)" />
          <path
            d={linePath}
            fill="none"
            stroke="#07B6D5"
            strokeWidth="2.25"
            strokeLinejoin="round"
            strokeLinecap="round"
            vectorEffect="non-scaling-stroke"
          />
          {hoveredPoint && (
            <line
              x1={hoveredPoint[0].toFixed(1)}
              y1="0"
              x2={hoveredPoint[0].toFixed(1)}
              y2={VH}
              stroke="#9AA0A6"
              strokeWidth="1"
              strokeDasharray="3 4"
              vectorEffect="non-scaling-stroke"
            />
          )}
          {hoveredPoint && (
            <circle
              cx={hoveredPoint[0].toFixed(1)}
              cy={hoveredPoint[1].toFixed(1)}
              r="4.5"
              fill="#FFFFFF"
              stroke="#07B6D5"
              strokeWidth="2"
            />
          )}
          {last && (
            <circle
              cx={last[0].toFixed(1)}
              cy={last[1].toFixed(1)}
              r="3.5"
              fill="#07B6D5"
            />
          )}
        </svg>
        {ticks.map((tick) => (
          <span
            key={tick.value}
            className="pointer-events-none absolute left-0 -translate-y-full pb-[3px] font-mono text-[11px] leading-none text-ink-faint"
            style={{ top: `${((tick.y / VH) * 100).toFixed(2)}%` }}
          >
            {tick.value >= 1000
              ? `${(tick.value / 1000).toLocaleString("en-US")}k`
              : tick.value}
          </span>
        ))}
        {hovered && hoveredPoint && (
          <div
            className="pointer-events-none absolute z-[5] flex -translate-x-1/2 translate-y-[-128%] flex-col gap-[3px] whitespace-nowrap rounded-lg bg-ink px-[13px] py-[9px] text-paper shadow-[0_6px_18px_rgba(20,23,26,0.22)]"
            style={{
              left: `${Math.min(88, Math.max(10, (hoveredPoint[0] / VW) * 100)).toFixed(1)}%`,
              top: `${((hoveredPoint[1] / VH) * 100).toFixed(1)}%`,
            }}
          >
            <span className="text-[13.5px] font-bold tracking-[-0.01em]">
              {hovered.downloads.toLocaleString("en-US")} downloads
            </span>
            <span className="text-xs text-[#B8BDC4]">
              Week of {formatWeekOf(hovered.weekEnding)}
            </span>
          </div>
        )}
      </div>

      <div className="mt-2.5 flex items-baseline justify-between border-t border-line pt-2.5">
        <span className="font-mono text-xs text-ink-faint">{startLabel}</span>
        <span className="font-mono text-xs text-ink-faint">
          through {throughLabel}
        </span>
      </div>
    </section>
  );
}
