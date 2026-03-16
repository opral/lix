import { createFileRoute, Link, redirect } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { useEffect } from "react";
import markdownPageCss from "../../components/markdown-page.style.css?url";
import { Footer } from "../../components/footer";
import { Header } from "../../components/header";
import { PrevNextNav } from "../../components/prev-next-nav";
import {
  buildBreadcrumbJsonLd,
  buildCanonicalUrl,
  buildWebPageJsonLd,
  extractOgMeta,
  extractTwitterMeta,
  getMarkdownDescription,
  getMarkdownTitle,
  resolveOgImage,
  splitTitleFromHtml,
} from "../../lib/seo";

const rfcMarkdownFiles = import.meta.glob<string>(
  "../../../../../rfcs/**/index.md",
  {
    query: "?raw",
    import: "default",
  }
);

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

  return (
    getMarkdownTitle({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    }) ?? slug
  );
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

  const rendered = splitTitleFromHtml(rewriteRfcLinks(parsed.html));
  const title =
    getMarkdownTitle({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    }) ?? rendered.title ?? slug;
  const description =
    getMarkdownDescription({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    }) ?? `Design proposal for ${title}.`;
  const date = parsed.frontmatter?.date as string | undefined;

  return {
    slug,
    title,
    description,
    date,
    html: rendered.body,
    frontmatter: parsed.frontmatter,
    prevRfc,
    nextRfc,
  };
}

type RfcLoaderData = Awaited<ReturnType<typeof loadRfc>>;

export function buildRfcHead(loaderData?: RfcLoaderData) {
  const title = loaderData?.title;
  const description = loaderData?.description;
  const slug = loaderData?.slug;
  const canonicalUrl = slug
    ? buildCanonicalUrl(`/rfc/${slug}`)
    : buildCanonicalUrl("/rfc");
  const ogImage = resolveOgImage(loaderData?.frontmatter);
  const ogMeta = extractOgMeta(loaderData?.frontmatter);
  const twitterMeta = extractTwitterMeta(loaderData?.frontmatter);
  const pageTitle = title ? `${title} | Lix RFCs` : "Lix RFCs";

  const links: Array<{ rel: string; href: string }> = [
    { rel: "stylesheet", href: markdownPageCss },
    { rel: "canonical", href: canonicalUrl },
  ];

  if (loaderData?.prevRfc?.slug) {
    links.push({
      rel: "prev",
      href: buildCanonicalUrl(`/rfc/${loaderData.prevRfc.slug}`),
    });
  }

  if (loaderData?.nextRfc?.slug) {
    links.push({
      rel: "next",
      href: buildCanonicalUrl(`/rfc/${loaderData.nextRfc.slug}`),
    });
  }

  const meta: Array<
    | { title: string }
    | { name: string; content: string }
    | { property: string; content: string }
  > = [
    { title: pageTitle },
    { property: "og:title", content: pageTitle },
    { property: "og:description", content: description ?? "Lix RFC" },
    { property: "og:url", content: canonicalUrl },
    { property: "og:type", content: "article" },
    { property: "og:site_name", content: "Lix" },
    { property: "og:locale", content: "en_US" },
    { property: "og:image", content: ogImage.url },
    { property: "og:image:alt", content: ogImage.alt },
    { name: "twitter:card", content: "summary_large_image" },
    { name: "twitter:title", content: pageTitle },
    { name: "twitter:description", content: description ?? "Lix RFC" },
    { name: "twitter:image", content: ogImage.url },
    { name: "twitter:image:alt", content: ogImage.alt },
  ];

  if (description) {
    meta.push({ name: "description", content: description });
  }

  if (loaderData?.date) {
    meta.push({
      property: "article:published_time",
      content: loaderData.date,
    });
  }

  const webPageJsonLd = buildWebPageJsonLd({
    title: pageTitle,
    description,
    canonicalUrl,
    image: ogImage.url,
  });
  const breadcrumbJsonLd = buildBreadcrumbJsonLd([
    { name: "Lix", item: buildCanonicalUrl("/") },
    { name: "RFCs", item: buildCanonicalUrl("/rfc") },
    ...(title ? [{ name: title, item: canonicalUrl }] : []),
  ]);

  const scripts = [
    {
      type: "application/ld+json",
      children: JSON.stringify({
        "@context": "https://schema.org",
        "@type": "TechArticle",
        headline: title ?? "Lix RFC",
        description,
        url: canonicalUrl,
        image: ogImage.url,
        ...(loaderData?.date ? { datePublished: loaderData.date } : {}),
      }),
    },
    {
      type: "application/ld+json",
      children: JSON.stringify(webPageJsonLd),
    },
    {
      type: "application/ld+json",
      children: JSON.stringify(breadcrumbJsonLd),
    },
  ];

  return {
    meta: [...meta, ...ogMeta, ...twitterMeta],
    links,
    scripts,
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
  head: ({ loaderData }) => buildRfcHead(loaderData),
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
        <div className="mx-auto max-w-4xl px-6 py-12">
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
            className="markdown-wc-body"
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
