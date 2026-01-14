import { Link } from "@tanstack/react-router";

type DocRoute = {
  slug: string;
  title?: string;
};

const navTitleOverrides: Record<string, string> = {
  "next-js": "Next.js",
  "api-reference": "API Reference",
};

function formatNavTitle(input: string) {
  const normalized = input.toLowerCase();
  if (normalized in navTitleOverrides) {
    return navTitleOverrides[normalized];
  }
  return normalized
    .split("-")
    .filter(Boolean)
    .map((word) => word[0]?.toUpperCase() + word.slice(1))
    .join(" ");
}

export function DocsPrevNext({
  currentSlug,
  routes,
}: {
  currentSlug: string;
  routes: DocRoute[];
}) {
  const currentIndex = routes.findIndex((item) => item.slug === currentSlug);
  if (currentIndex === -1 || routes.length <= 1) return null;

  const prev = currentIndex > 0 ? routes[currentIndex - 1] : null;
  const next = currentIndex < routes.length - 1 ? routes[currentIndex + 1] : null;

  if (!prev && !next) return null;

  return (
    <nav className="mt-8 grid grid-cols-2 gap-4 border-t border-slate-200 pt-8">
      <div>
        {prev && (
          <Link
            to={`/docs/${prev.slug}`}
            className="group block rounded-xl border border-slate-200 p-4 transition-colors hover:border-slate-300"
          >
            <span className="text-sm text-slate-400">Previous</span>
            <span className="mt-1 block font-medium text-[#3451b2] group-hover:text-[#3a5ccc]">
              {prev.title ?? formatNavTitle(prev.slug)}
            </span>
          </Link>
        )}
      </div>
      <div>
        {next && (
          <Link
            to={`/docs/${next.slug}`}
            className="group block rounded-xl border border-slate-200 p-4 text-right transition-colors hover:border-slate-300"
          >
            <span className="text-sm text-slate-400">Next</span>
            <span className="mt-1 block font-medium text-[#3451b2] group-hover:text-[#3a5ccc]">
              {next.title ?? formatNavTitle(next.slug)}
            </span>
          </Link>
        )}
      </div>
    </nav>
  );
}
