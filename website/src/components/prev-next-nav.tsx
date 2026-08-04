import { Link } from "@tanstack/react-router";

type PrevNextItem = {
  slug: string;
  title: string;
} | null;

type PrevNextNavProps = {
  prev: PrevNextItem;
  next: PrevNextItem;
  basePath: string;
  paramName?: string;
  prevLabel?: string;
  nextLabel?: string;
  className?: string;
};

/**
 * Reusable previous/next navigation component for docs, blog, and RFCs.
 *
 * @example
 * <PrevNextNav
 *   prev={{ slug: "intro", title: "Introduction" }}
 *   next={{ slug: "advanced", title: "Advanced Topics" }}
 *   basePath="/docs"
 *   paramName="slugId"
 *   prevLabel="Previous"
 *   nextLabel="Next"
 * />
 */
export function PrevNextNav({
  prev,
  next,
  basePath,
  paramName = "slug",
  prevLabel = "Previous",
  nextLabel = "Next",
  className = "",
}: PrevNextNavProps) {
  if (!prev && !next) return null;

  return (
    <nav
      className={`flex justify-between gap-4 border-t border-line pt-5 ${className}`}
    >
      {prev ? (
        <Link
          to={`${basePath}/$${paramName}` as string}
          params={{ [paramName]: prev.slug } as Record<string, string>}
          className="flex min-w-0 flex-col gap-1"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.08em] text-ink-faint">
            {prevLabel}
          </span>
          <span className="text-[15px] font-semibold text-cyan-deep">
            ← {prev.title}
          </span>
        </Link>
      ) : (
        <span />
      )}
      {next ? (
        <Link
          to={`${basePath}/$${paramName}` as string}
          params={{ [paramName]: next.slug } as Record<string, string>}
          className="flex min-w-0 flex-col items-end gap-1 text-right"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.08em] text-ink-faint">
            {nextLabel}
          </span>
          <span className="text-[15px] font-semibold text-cyan-deep">
            {next.title} →
          </span>
        </Link>
      ) : (
        <span />
      )}
    </nav>
  );
}
