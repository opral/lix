import { readFileSync } from "node:fs";
import { parse } from "@opral/markdown-wc";
import { describe, expect, test } from "vitest";
import { getBlogDescription, getBlogTitle } from "../blog/blogMetadata";
import { resolveOgImageUrl } from "../blog/og-image";
import {
  getMarkdownDescription,
  getMarkdownTitle,
  splitTitleFromHtml,
} from "../lib/seo";
import { buildBlogPostHead } from "./blog/$slug";
import { buildDocsPageHead } from "./docs/$slugId";
import { buildRfcHead } from "./rfc/$slug";

function findLink(
  links: Array<{ rel: string; href: string }> | undefined,
  rel: string,
) {
  return links?.find((entry) => entry.rel === rel)?.href;
}

function findMetaContent(
  meta:
    | Array<
        | { title: string }
        | { name: string; content: string }
        | { property: string; content: string }
      >
    | undefined,
  key: string,
) {
  const entry = meta?.find(
    (item) =>
      ("name" in item && item.name === key) ||
      ("property" in item && item.property === key),
  );
  if (!entry || !("content" in entry)) {
    return undefined;
  }
  return entry.content;
}

function jsonLdScripts(
  scripts: Array<{ type?: string; children?: string }> | undefined,
) {
  return (
    scripts
      ?.filter((script) => script.type === "application/ld+json")
      .map((script) => JSON.parse(script.children ?? "{}")) ?? []
  );
}

describe("SEO route smoke tests", () => {
  test("docs head stays canonical and strips the rendered h1 once", async () => {
    const rawMarkdown = readFileSync(
      new URL("../../../docs/comparison-to-git.md", import.meta.url),
      "utf8",
    );
    const parsed = await parse(rawMarkdown, {
      externalLinks: true,
      assetBaseUrl: "/docs/comparison-to-git/",
    });
    const rendered = splitTitleFromHtml(parsed.html);
    const head = buildDocsPageHead({
      doc: {
        slug: "comparison-to-git",
        content: rawMarkdown,
      },
      frontmatter: parsed.frontmatter,
      html: rendered.body,
      pageToc: [],
      sidebarSections: [],
      tocEntry: undefined,
    } as any);

    expect(findLink(head.links, "canonical")).toBe(
      "https://lix.dev/docs/comparison-to-git",
    );
    expect(findMetaContent(head.meta, "og:title")).toBe(
      "How Lix compares to Git | Lix Documentation",
    );
    expect(findMetaContent(head.meta, "twitter:description")).toBe(
      "Compare Git's source-code workflow with Lix's file-format version control for Markdown, DOCX, XLSX, JSON, PDFs, and custom formats.",
    );
    expect(rendered.title).toBe("How Lix compares to Git");
    expect(rendered.body).not.toContain("<h1");
  });

  test("blog head includes social metadata and keeps cover assets in the post folder", async () => {
    const slug = "002-modeling-a-company-as-a-repository";
    const rawMarkdown = readFileSync(
      new URL(`../../../blog/${slug}/index.md`, import.meta.url),
      "utf8",
    );
    const parsed = await parse(rawMarkdown, {
      assetBaseUrl: `/blog/${slug}/`,
    });
    const rendered = splitTitleFromHtml(parsed.html);
    const title = getBlogTitle({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    });
    const description = getBlogDescription({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    });
    const ogImage = resolveOgImageUrl(
      parsed.frontmatter?.["og:image"] as string,
      slug,
    );
    const head = buildBlogPostHead({
      post: {
        slug,
        title,
        description,
        date: parsed.frontmatter?.date as string | undefined,
        dateModified: parsed.frontmatter?.date as string | undefined,
        authors: undefined,
        readingTime: 4,
        ogImage,
        ogImageAlt: parsed.frontmatter?.["og:image:alt"] as string | undefined,
        imports: undefined,
      },
      html: rendered.body,
      rawMarkdown,
      prevPost: null,
      nextPost: null,
    });

    expect(findLink(head.links, "canonical")).toBe(
      `https://lix.dev/blog/${slug}`,
    );
    expect(findMetaContent(head.meta, "og:title")).toBe(
      "Your Company should be a Repository for AI agents | Lix Blog",
    );
    expect(findMetaContent(head.meta, "twitter:image")).toBe(
      "https://lix.dev/blog/002-modeling-a-company-as-a-repository/cover.jpg",
    );
    const schemas = jsonLdScripts(head.scripts);
    expect(schemas).toHaveLength(1);
    expect(schemas[0]).toMatchObject({
      "@context": "https://schema.org",
      "@type": "BlogPosting",
      headline: "Your Company should be a Repository for AI agents",
      datePublished: "2026-02-23",
      dateModified: "2026-02-23",
      mainEntityOfPage: {
        "@type": "WebPage",
        "@id": `https://lix.dev/blog/${slug}`,
      },
      publisher: {
        "@type": "Organization",
        name: "Lix",
      },
    });
    expect(schemas[0]["@type"]).not.toBe("Blog");
    expect(rendered.title).toBe(
      "Your Company should be a Repository for AI agents",
    );
    expect(rendered.body).not.toContain("<h1");
  });

  test("rfc head includes canonical and social metadata with summary-based descriptions", async () => {
    const slug = "001-preprocess-writes";
    const rawMarkdown = readFileSync(
      new URL(`../../../rfcs/${slug}/index.md`, import.meta.url),
      "utf8",
    );
    const parsed = await parse(rawMarkdown, {
      assetBaseUrl: `/rfc/${slug}/`,
    });
    const rendered = splitTitleFromHtml(parsed.html);
    const title = getMarkdownTitle({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    });
    const description = getMarkdownDescription({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    });
    const head = buildRfcHead({
      slug,
      title: title ?? slug,
      description: description ?? `Design proposal for ${title ?? slug}.`,
      date: parsed.frontmatter?.date as string | undefined,
      html: rendered.body,
      frontmatter: parsed.frontmatter,
      prevRfc: null,
      nextRfc: null,
    });

    expect(findLink(head.links, "canonical")).toBe(
      `https://lix.dev/rfc/${slug}`,
    );
    expect(findMetaContent(head.meta, "og:title")).toBe(
      "Preprocess writes to avoid vtable overhead | Lix RFCs",
    );
    expect(findMetaContent(head.meta, "twitter:description")).toBe(
      "Write operations in Lix are slow due to the vtable mechanism crossing the JS ↔ SQLite WASM boundary multiple times per row.",
    );
    expect(rendered.title).toBe("Preprocess writes to avoid vtable overhead");
    expect(rendered.body).not.toContain("<h1");
  });
});
