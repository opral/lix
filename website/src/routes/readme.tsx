import { createFileRoute } from "@tanstack/react-router";
import { Header } from "../components/header";
import { LandingReadme } from "../components/landing-page";
import markdownPageCss from "../components/markdown-page.style.css?url";
import { loadReadmeContent } from "../lib/readme-content";
import {
  buildCanonicalUrl,
  buildWebSiteJsonLd,
  resolveOgImage,
} from "../lib/seo";

export const Route = createFileRoute("/readme")({
  loader: async () => {
    return await loadReadmeContent();
  },
  head: () => {
    const title = "Lix README | Version control system for every file format";
    const description =
      "Read how Lix tracks, reviews, branches, merges, and rolls back changes across Markdown, DOCX, XLSX, JSON, PDFs, and custom file formats.";
    const canonicalUrl = buildCanonicalUrl("/readme");
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
  component: ReadmeRoute,
});

function ReadmeRoute() {
  const { html } = Route.useLoaderData();

  return (
    <>
      <Header />
      <LandingReadme readmeHtml={html} />
    </>
  );
}
