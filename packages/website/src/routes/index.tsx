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
import { normalizeMarkdownHtml } from "../lib/markdown-html";
import markdownPageCss from "../components/markdown-page.style.css?url";
import readmeMarkdown from "../../../../README.md?raw";

async function loadReadmeContent() {
  const parsed = await parse(readmeMarkdown);
  return {
    html: normalizeMarkdownHtml(parsed.html).replaceAll(
      'src="./assets/',
      'src="/assets/',
    ),
  };
}

export const Route = createFileRoute("/")({
  loader: async () => {
    return await loadReadmeContent();
  },
  head: () => {
    const title = "Lix | An embeddable version control system for AI agents";
    const description =
      "Lix gives agents branches, checkpoints, semantic diffs, rollback, immutable history, and SQL-queryable context without wrapping Git.";
    const canonicalUrl = buildCanonicalUrl("/");
    const ogImage = resolveOgImage();
    const jsonLd = buildWebSiteJsonLd({
      title,
      description,
      canonicalUrl,
    });
    const softwareJsonLd = {
      "@context": "https://schema.org",
      "@type": "SoftwareApplication",
      name: "Lix",
      description,
      url: canonicalUrl,
      applicationCategory: "DeveloperApplication",
      operatingSystem: "Web, Node.js",
      programmingLanguage: "TypeScript",
      codeRepository: "https://github.com/opral/lix",
      offers: {
        "@type": "Offer",
        price: "0",
        priceCurrency: "USD",
      },
    };

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
        {
          type: "application/ld+json",
          children: JSON.stringify(softwareJsonLd),
        },
      ],
    };
  },
  component: HomeRoute,
});

function HomeRoute() {
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
