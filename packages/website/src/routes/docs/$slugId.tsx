import { createFileRoute, notFound, redirect } from "@tanstack/react-router";
import {
  DocsLayout,
  type PageTocItem,
  type SidebarSection,
} from "../../components/docs-layout";
import { MarkdownPage } from "../../components/markdown-page";
import tableOfContents from "../../../content/docs/table_of_contents.json";
import {
  buildDocMaps,
  buildTocMap,
  normalizeRelativePath,
  type Toc,
} from "../../lib/build-doc-map";
import {
  buildCanonicalUrl,
  buildBreadcrumbJsonLd,
  buildWebPageJsonLd,
  extractOgMeta,
  extractTwitterMeta,
  getMarkdownDescription,
  getMarkdownTitle,
  resolveOgImage,
} from "../../lib/seo";
import { parse } from "@opral/markdown-wc";
import markdownPageCss from "../../components/markdown-page.style.css?url";

const docs = import.meta.glob<string>("/content/docs/**/*.md", {
  eager: true,
  import: "default",
  query: "?raw",
});

const tocMap = buildTocMap(tableOfContents as Toc);
const { bySlug: docsBySlug } = buildDocMaps(docs);
const docsByRelativePath = Object.values(docsBySlug).reduce((acc, doc) => {
  acc[doc.relativePath] = doc;
  return acc;
}, {} as Record<string, (typeof docsBySlug)[string]>);

/**
 * Builds a list of heading links from rendered HTML for the "On this page" TOC.
 *
 * @example
 * buildPageToc('<h2 id="intro">Intro</h2>') // [{ id: "intro", label: "Intro", level: 2 }]
 */
function buildPageToc(html: string): PageTocItem[] {
  const headings: PageTocItem[] = [];
  const regex = /<h2\b[^>]*id="([^"]+)"[^>]*>([\s\S]*?)<\/h2>/g;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(html)) !== null) {
    const id = match[1];
    const label = decodeHtmlEntities(stripHtml(match[2])).trim();
    if (!id || !label) continue;
    headings.push({ id, label, level: 2 });
  }

  return headings;
}

/**
 * Removes HTML tags from a string.
 *
 * @example
 * stripHtml("<strong>Title</strong>") // "Title"
 */
function stripHtml(input: string): string {
  return input.replace(/<[^>]*>/g, "");
}

/**
 * Decodes a minimal set of HTML entities for heading labels.
 *
 * @example
 * decodeHtmlEntities("Foo &amp; Bar") // "Foo & Bar"
 */
function decodeHtmlEntities(input: string): string {
  return input
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function buildSidebarSections(toc: Toc): SidebarSection[] {
  return toc.sidebar
    .map((section) => {
      const items = section.items
        .map((item) => {
          const relativePath = normalizeRelativePath(item.file);
          const doc = docsByRelativePath[relativePath];
          if (!doc) {
            return null;
          }

          return {
            label: item.label,
            href: `/docs/${doc.slug}`,
            relativePath,
          };
        })
        .filter((value): value is NonNullable<typeof value> => Boolean(value));

      return { label: section.label, items };
    })
    .filter((section) => section.items.length > 0);
}

export const Route = createFileRoute("/docs/$slugId")({
  head: ({ loaderData }) => {
    const frontmatter = loaderData?.frontmatter as
      | Record<string, unknown>
      | undefined;
    const rawMarkdown = loaderData?.doc?.content ?? "";
    const title = getMarkdownTitle({ rawMarkdown, frontmatter });
    const description = getMarkdownDescription({ rawMarkdown, frontmatter });
    const canonicalUrl = loaderData?.doc?.slug
      ? buildCanonicalUrl(`/docs/${loaderData.doc.slug}`)
      : buildCanonicalUrl("/docs");
    const ogImage = resolveOgImage(frontmatter);
    const ogMeta = extractOgMeta(frontmatter);
    const twitterMeta = extractTwitterMeta(frontmatter);
    const pageTitle = title
      ? `${title} | Lix Documentation`
      : "Lix Documentation";
    const jsonLd = buildWebPageJsonLd({
      title: pageTitle,
      description,
      canonicalUrl,
      image: ogImage.url,
    });
    const breadcrumbJsonLd = buildBreadcrumbJsonLd(
      [
        { name: "Lix", item: buildCanonicalUrl("/") },
        { name: "Documentation", item: buildCanonicalUrl("/docs") },
        title ? { name: title, item: canonicalUrl } : undefined,
      ].filter(Boolean) as Array<{ name: string; item: string }>,
    );
    const meta: Array<
      | { title: string }
      | { name: string; content: string }
      | { property: string; content: string }
    > = [
      {
        title: pageTitle,
      },
      { property: "og:url", content: canonicalUrl },
      { property: "og:type", content: "article" },
      { property: "og:site_name", content: "Lix" },
      { property: "og:locale", content: "en_US" },
      { property: "og:image", content: ogImage.url },
      { property: "og:image:alt", content: ogImage.alt },
      { name: "twitter:card", content: "summary_large_image" },
      { name: "twitter:image", content: ogImage.url },
      { name: "twitter:image:alt", content: ogImage.alt },
    ];

    if (description) {
      meta.push(
        { name: "description", content: description },
        { property: "og:description", content: description },
        { name: "twitter:description", content: description },
      );
    }

    if (title) {
      meta.push(
        { property: "og:title", content: pageTitle },
        { name: "twitter:title", content: pageTitle },
      );
    }

    return {
      meta: [...meta, ...ogMeta, ...twitterMeta],
      links: [
        {
          rel: "stylesheet",
          href: markdownPageCss,
        },
        {
          rel: "canonical",
          href: canonicalUrl,
        },
      ],
      scripts: [
        {
          type: "application/ld+json",
          children: JSON.stringify(jsonLd),
        },
        {
          type: "application/ld+json",
          children: JSON.stringify(breadcrumbJsonLd),
        },
      ],
    };
  },
  loader: async ({ params }) => {
    const doc = docsBySlug[params.slugId];

    if (!doc) {
      throw notFound();
    }

    const tocEntry = tocMap.get(doc.relativePath);
    const parsedMarkdown = await parse(doc.content, { externalLinks: true });
    const pageToc = buildPageToc(parsedMarkdown.html);

    return {
      doc,
      tocEntry,
      sidebarSections: buildSidebarSections(tableOfContents as Toc),
      html: parsedMarkdown.html,
      frontmatter: parsedMarkdown.frontmatter,
      pageToc,
    };
  },
  component: DocsPage,
});

function DocsPage() {
  const { doc, sidebarSections, html, frontmatter, pageToc } =
    Route.useLoaderData();

  return (
    <DocsLayout
      toc={tableOfContents as Toc}
      sidebarSections={sidebarSections}
      activeRelativePath={doc.relativePath}
      pageToc={pageToc}
    >
      <MarkdownPage
        html={html}
        markdown={doc.content}
        imports={(frontmatter.imports as string[] | undefined) ?? undefined}
      />
    </DocsLayout>
  );
}
