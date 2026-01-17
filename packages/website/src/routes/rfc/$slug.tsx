import { createFileRoute, Link, redirect } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { useEffect } from "react";
import markdownPageCss from "../../components/markdown-page.style.css?url";
import { Footer } from "../../components/footer";
import { Header } from "../../components/header";
import { PrevNextNav } from "../../components/prev-next-nav";

const rfcMarkdownFiles = import.meta.glob<string>("../../../../../rfcs/**/*.md", {
  query: "?raw",
  import: "default",
});

const rfcRootPrefix = "../../../../../rfcs/";

type RfcPrevNext = {
  slug: string;
  title: string;
} | null;

async function getTitleForSlug(slug: string): Promise<string> {
  const path = `${rfcRootPrefix}${slug}/index.md`;
  const loader = rfcMarkdownFiles[path];
  if (!loader) return slug;

  const rawMarkdown = await loader();
  const parsed = await parse(rawMarkdown);

  if (parsed.frontmatter?.title) {
    return parsed.frontmatter.title as string;
  }
  const h1Match = rawMarkdown.match(/^#\s+(.+)$/m);
  if (h1Match) {
    return h1Match[1];
  }
  return slug;
}

/**
 * Rewrite RFC links to remove index.md suffix
 * Handles both relative paths (../001-slug/index.md) and absolute paths (/rfc/001-slug/index.md)
 */
function rewriteRfcLinks(html: string): string {
  return html
    // Handle relative paths: ../001-slug/index.md or ./001-slug/index.md
    .replace(
      /href="\.\.?\/([\d]+-[^/]+)\/index\.md"/g,
      'href="/rfc/$1"'
    )
    // Handle absolute paths that were resolved by assetBaseUrl: /rfc/001-slug/index.md
    .replace(
      /href="\/rfc\/([\d]+-[^/]+)\/index\.md"/g,
      'href="/rfc/$1"'
    );
}

async function loadRfc(slug: string) {
  if (!slug) {
    throw new Error("Missing RFC slug");
  }

  const path = `${rfcRootPrefix}${slug}/index.md`;
  const loader = rfcMarkdownFiles[path];

  if (!loader) {
    throw new Error(`RFC not found: ${slug}`);
  }

  // Auto-discover all RFCs for prev/next navigation
  const rfcPaths = Object.keys(rfcMarkdownFiles);
  const allSlugs = rfcPaths
    .map((p) => p.replace(rfcRootPrefix, "").replace("/index.md", ""))
    .sort((a, b) => b.localeCompare(a)); // Sort Z-A

  const currentIndex = allSlugs.findIndex((s) => s === slug);
  const prevSlug = currentIndex > 0 ? allSlugs[currentIndex - 1] : null;
  const nextSlug =
    currentIndex < allSlugs.length - 1 ? allSlugs[currentIndex + 1] : null;

  const prevRfc: RfcPrevNext = prevSlug
    ? { slug: prevSlug, title: await getTitleForSlug(prevSlug) }
    : null;
  const nextRfc: RfcPrevNext = nextSlug
    ? { slug: nextSlug, title: await getTitleForSlug(nextSlug) }
    : null;

  const rawMarkdown = await loader();
  const parsed = await parse(rawMarkdown, {
    assetBaseUrl: `/rfc/${slug}/`,
  });

  // Rewrite relative RFC links to absolute paths
  const html = rewriteRfcLinks(parsed.html);

  // Extract title from frontmatter or first h1
  let title = slug;
  if (parsed.frontmatter?.title) {
    title = parsed.frontmatter.title as string;
  } else {
    const h1Match = rawMarkdown.match(/^#\s+(.+)$/m);
    if (h1Match) {
      title = h1Match[1];
    }
  }

  return {
    slug,
    title,
    html,
    prevRfc,
    nextRfc,
  };
}

export const Route = createFileRoute("/rfc/$slug")({
  loader: async ({ params }) => {
    try {
      return await loadRfc(params.slug);
    } catch {
      throw redirect({ to: "/rfc" });
    }
  },
  head: ({ loaderData }) => {
    const title = loaderData?.title;
    const slug = loaderData?.slug;

    const links: Array<{ rel: string; href: string }> = [
      { rel: "stylesheet", href: markdownPageCss },
    ];

    if (slug) {
      links.push({ rel: "canonical", href: `https://lix.opral.com/rfc/${slug}` });
    }

    if (loaderData?.prevRfc?.slug) {
      links.push({
        rel: "prev",
        href: `https://lix.opral.com/rfc/${loaderData.prevRfc.slug}`,
      });
    }

    if (loaderData?.nextRfc?.slug) {
      links.push({
        rel: "next",
        href: `https://lix.opral.com/rfc/${loaderData.nextRfc.slug}`,
      });
    }

    return {
      meta: [
        { title: title ? `${title} | Lix RFCs` : "RFC | Lix" },
        { name: "description", content: title ?? "Lix RFC" },
      ],
      links,
    };
  },
  component: RfcPage,
});

function RfcPage() {
  const { title, html, prevRfc, nextRfc } = Route.useLoaderData();

  useEffect(() => {
    // @ts-expect-error - JS-only module
    import("../../components/markdown-page.interactive.js");
  }, [html]);

  return (
    <div className="flex min-h-screen flex-col bg-white text-slate-900">
      <Header />
      <main className="flex-1">
        <div className="mx-auto max-w-3xl px-6 py-12">
          <nav className="mb-8">
            <Link
              to="/rfc"
              className="inline-flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700 transition-colors"
            >
              <svg
                className="h-4 w-4"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M19 12H5M12 19l-7-7 7-7" />
              </svg>
              All RFCs
            </Link>
          </nav>

          <h1 className="text-2xl md:text-3xl lg:text-4xl font-bold text-slate-900 mb-8">
            {title}
          </h1>

          <article
            className="markdown-wc-body [&>h1:first-child]:hidden"
            dangerouslySetInnerHTML={{ __html: html }}
          />

          <PrevNextNav
            prev={prevRfc}
            next={nextRfc}
            basePath="/rfc"
            prevLabel="Previous RFC"
            nextLabel="Next RFC"
            className="mt-16"
          />
        </div>
      </main>
      <Footer />
    </div>
  );
}
