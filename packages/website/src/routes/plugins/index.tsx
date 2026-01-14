import { createFileRoute } from "@tanstack/react-router";
import { MarkdownPage } from "../../components/markdown-page";
import { DocsLayout } from "../../components/docs-layout";
import { parse } from "@opral/markdown-wc";
import markdownPageCss from "../../components/markdown-page.style.css?url";
import pluginRegistry from "./plugin.registry.json";
import { buildPluginSidebarSections } from "../../lib/plugin-sidebar";
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

const pluginIndexMarkdownFiles = import.meta.glob<string>(
  "/content/plugins/index.md",
  {
    eager: true,
    import: "default",
    query: "?raw",
  }
);
const pluginIndexMarkdown =
  pluginIndexMarkdownFiles["/content/plugins/index.md"];

export const Route = createFileRoute("/plugins/")({
  head: ({ loaderData }) => {
    const frontmatter = loaderData?.frontmatter as
      | Record<string, unknown>
      | undefined;
    const rawMarkdown = loaderData?.markdown ?? "";
    const title =
      getMarkdownTitle({ rawMarkdown, frontmatter }) ?? "Lix Plugins";
    const description =
      getMarkdownDescription({ rawMarkdown, frontmatter }) ??
      "Discover Lix plugins and integrations.";
    const canonicalUrl = buildCanonicalUrl("/plugins");
    const ogImage = resolveOgImage(frontmatter);
    const ogMeta = extractOgMeta(frontmatter);
    const twitterMeta = extractTwitterMeta(frontmatter);
    const jsonLd = buildWebPageJsonLd({
      title,
      description,
      canonicalUrl,
      image: ogImage.url,
    });
    const breadcrumbJsonLd = buildBreadcrumbJsonLd([
      { name: "Lix", item: buildCanonicalUrl("/") },
      { name: "Plugins", item: canonicalUrl },
    ]);
    const meta: Array<
      | { title: string }
      | { name: string; content: string }
      | { property: string; content: string }
    > = [
      { title },
      { name: "description", content: description },
      { property: "og:title", content: title },
      { property: "og:description", content: description },
      { property: "og:url", content: canonicalUrl },
      { property: "og:type", content: "website" },
      { property: "og:site_name", content: "Lix" },
      { property: "og:locale", content: "en_US" },
      { property: "og:image", content: ogImage.url },
      { property: "og:image:alt", content: ogImage.alt },
      { name: "twitter:card", content: "summary_large_image" },
      { name: "twitter:title", content: title },
      { name: "twitter:description", content: description },
      { name: "twitter:image", content: ogImage.url },
      { name: "twitter:image:alt", content: ogImage.alt },
    ];

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
  loader: async () => {
    const parsed = await parse(pluginIndexMarkdown, { externalLinks: true });
    return {
      html: parsed.html,
      frontmatter: parsed.frontmatter,
      markdown: pluginIndexMarkdown,
    };
  },
  component: PluginsIndexPage,
});

/**
 * Renders the plugins landing page from markdown content.
 *
 * @example
 * <PluginsIndexPage />
 */
function PluginsIndexPage() {
  const { html, frontmatter, markdown } = Route.useLoaderData();

  return (
    <DocsLayout
      toc={{ sidebar: [] }}
      sidebarSections={buildPluginSidebarSections(pluginRegistry)}
    >
      <MarkdownPage
        html={html}
        markdown={markdown}
        imports={(frontmatter.imports as string[] | undefined) ?? undefined}
      />
    </DocsLayout>
  );
}
