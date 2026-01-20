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
      className={`grid grid-cols-2 gap-4 border-t border-slate-200 pt-8 ${className}`}
    >
      <div>
        {prev && (
          <Link
            to={`${basePath}/$${paramName}` as string}
            params={{ [paramName]: prev.slug } as Record<string, string>}
            className="group block rounded-xl border border-slate-200 p-4 transition-colors hover:border-slate-300"
          >
            <span className="flex items-center gap-1.5 text-sm text-slate-400">
              <svg
                className="h-3 w-3"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M19 12H5M12 19l-7-7 7-7" />
              </svg>
              {prevLabel}
            </span>
            <span className="mt-1 block font-medium text-[#3451b2] group-hover:text-[#3a5ccc]">
              {prev.title}
            </span>
          </Link>
        )}
      </div>

      <div className="flex justify-end">
        {next && (
          <Link
            to={`${basePath}/$${paramName}` as string}
            params={{ [paramName]: next.slug } as Record<string, string>}
            className="group block rounded-xl border border-slate-200 p-4 transition-colors hover:border-slate-300"
          >
            <span className="flex items-center justify-end gap-1.5 text-sm text-slate-400">
              {nextLabel}
              <svg
                className="h-3 w-3"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M5 12h14M12 5l7 7-7 7" />
              </svg>
            </span>
            <span className="mt-1 block font-medium text-right text-[#3451b2] group-hover:text-[#3a5ccc]">
              {next.title}
            </span>
          </Link>
        )}
      </div>
    </nav>
  );
}
