import { PrevNextNav } from "./prev-next-nav";

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

  const prevRoute = currentIndex > 0 ? routes[currentIndex - 1] : null;
  const nextRoute =
    currentIndex < routes.length - 1 ? routes[currentIndex + 1] : null;

  const prev = prevRoute
    ? { slug: prevRoute.slug, title: prevRoute.title ?? formatNavTitle(prevRoute.slug) }
    : null;
  const next = nextRoute
    ? { slug: nextRoute.slug, title: nextRoute.title ?? formatNavTitle(nextRoute.slug) }
    : null;

  return (
    <PrevNextNav
      prev={prev}
      next={next}
      basePath="/docs"
      paramName="slugId"
      prevLabel="Previous"
      nextLabel="Next"
      className="mt-8"
    />
  );
}
