import { createFileRoute } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { Header } from "../components/header";
import { LandingReadme } from "../components/landing-page";
import { V2Footer, V2Hero } from "../components/v2-landing-page";
import {
  buildCanonicalUrl,
  buildWebSiteJsonLd,
  resolveOgImage,
} from "../lib/seo";
import markdownPageCss from "../components/markdown-page.style.css?url";
import readmeMarkdown from "../../../../README.md?raw";

async function loadReadmeContent() {
  const parsed = await parse(readmeMarkdown);
  return {
    html: parsed.html.replaceAll('src="./assets/', 'src="/assets/'),
  };
}

export const Route = createFileRoute("/v3")({
  loader: async () => {
    return await loadReadmeContent();
  },
  head: () => {
    const title =
      "Lix | Version control as a library for AI agents and structured data";
    const description =
      "Lix gives AI agents and applications branchable, reviewable change control for structured files, binary formats, and SQL-backed workflows.";
    const canonicalUrl = buildCanonicalUrl("/v3");
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
  component: V3Route,
});

function V3Route() {
  const { html } = Route.useLoaderData();

  return (
    <>
      <Header />
      <div className="bg-[#fafaf7] text-[#0a0a0a] font-[Geist,ui-sans-serif,system-ui,sans-serif] tracking-[-0.005em] [font-feature-settings:'ss01','ss02','cv11']">
        <main>
          <V2Hero />
          <LandingReadme readmeHtml={html} />
          <V2Footer />
        </main>
      </div>
    </>
  );
}
