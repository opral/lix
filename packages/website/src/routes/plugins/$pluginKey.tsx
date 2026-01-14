import { createFileRoute, notFound } from "@tanstack/react-router";
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

const pluginMarkdown = import.meta.glob<string>("/content/plugins/*.md", {
  eager: true,
  import: "default",
  query: "?raw",
});

type PluginEntry = {
  key: string;
  name?: string;
  description?: string;
};

/**
 * Finds a plugin entry by key.
 *
 * @example
 * findPluginEntry("plugin_md")
 */
function findPluginEntry(pluginKey: string): PluginEntry | undefined {
  const plugins = Array.isArray(pluginRegistry.plugins)
    ? pluginRegistry.plugins
    : [];
  return plugins.find((plugin) => plugin.key === pluginKey);
}

/**
 * Loads the raw markdown for a plugin.
 *
 * @example
 * loadPluginMarkdown("plugin_md")
 */
function loadPluginMarkdown(pluginKey: string): string | undefined {
  return pluginMarkdown[`/content/plugins/${pluginKey}.md`];
}

export const Route = createFileRoute("/plugins/$pluginKey")({
  head: ({ loaderData }) => {
    const frontmatter = loaderData?.frontmatter as
      | Record<string, unknown>
      | undefined;
    const rawMarkdown = loaderData?.markdown ?? "";
    const plugin = loaderData?.plugin;
    const fallbackTitle = plugin?.name;
    const fallbackDescription = plugin?.description;
    const title =
      getMarkdownTitle({ rawMarkdown, frontmatter }) ?? fallbackTitle;
    const description =
      getMarkdownDescription({ rawMarkdown, frontmatter }) ??
      fallbackDescription;
    const canonicalUrl = loaderData?.plugin?.key
      ? buildCanonicalUrl(`/plugins/${loaderData.plugin.key}`)
      : buildCanonicalUrl("/plugins");
    const ogImage = resolveOgImage(frontmatter);
    const ogMeta = extractOgMeta(frontmatter);
    const twitterMeta = extractTwitterMeta(frontmatter);
    const pageTitle = title ? `${title} | Lix Plugins` : "Lix Plugins";
    const jsonLd = buildWebPageJsonLd({
      title: pageTitle,
      description,
      canonicalUrl,
      image: ogImage.url,
    });
    const breadcrumbJsonLd = buildBreadcrumbJsonLd(
      [
        { name: "Lix", item: buildCanonicalUrl("/") },
        { name: "Plugins", item: buildCanonicalUrl("/plugins") },
        title ? { name: title, item: canonicalUrl } : undefined,
      ].filter(Boolean) as Array<{ name: string; item: string }>,
    );
    const meta: Array<
      | { title: string }
      | { name: string; content: string }
      | { property: string; content: string }
    > = [
      { title: pageTitle },
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
    const plugin = findPluginEntry(params.pluginKey);
    if (!plugin) {
      throw notFound();
    }

    const markdown = loadPluginMarkdown(params.pluginKey);
    if (!markdown) {
      throw notFound();
    }

    const parsed = await parse(markdown, { externalLinks: true });
    return {
      html: parsed.html,
      frontmatter: parsed.frontmatter,
      markdown,
      plugin,
    };
  },
  component: PluginPage,
});

/**
 * Renders a plugin README page.
 *
 * @example
 * <PluginPage />
 */
function PluginPage() {
  const { html, frontmatter, markdown } = Route.useLoaderData();
  const { pluginKey } = Route.useParams();

  return (
    <DocsLayout
      toc={{ sidebar: [] }}
      sidebarSections={buildPluginSidebarSections(pluginRegistry)}
      activeRelativePath={pluginKey}
    >
      <MarkdownPage
        html={html}
        markdown={markdown}
        imports={(frontmatter.imports as string[] | undefined) ?? undefined}
      />
    </DocsLayout>
  );
}
