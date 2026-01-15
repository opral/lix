import { createFileRoute } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import LandingPage from "../components/landing-page";
import {
  buildCanonicalUrl,
  buildWebSiteJsonLd,
  resolveOgImage,
} from "../lib/seo";
import markdownPageCss from "../components/markdown-page.style.css?url";
import readmeMarkdown from "../../../../README.md?raw";

async function loadReadmeContent() {
  const parsed = await parse(readmeMarkdown);
  return { html: parsed.html };
}

export const Route = createFileRoute("/")({
  loader: async () => {
    return await loadReadmeContent();
  },
  head: () => {
    const title = "Lix - The version control system for AI agents";
    const description =
      "Lix lets you branch, track, and review every change an AI agent does on the filesystem.";
    const canonicalUrl = buildCanonicalUrl("/");
    const ogImage = resolveOgImage();
    const jsonLd = buildWebSiteJsonLd({
      title,
      description,
      canonicalUrl,
    });

    return {
      meta: [
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
      ],
      links: [
        { rel: "canonical", href: canonicalUrl },
        { rel: "stylesheet", href: markdownPageCss },
      ],
      scripts: [
        {
          type: "application/ld+json",
          children: JSON.stringify(jsonLd),
        },
      ],
    };
  },
  component: LandingPageWrapper,
});

function LandingPageWrapper() {
  const { html } = Route.useLoaderData();
  return <LandingPage readmeHtml={html} />;
}
