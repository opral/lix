import { createFileRoute } from "@tanstack/react-router";
import LandingPage from "../components/landing-page";
import markdownPageCss from "../components/markdown-page.style.css?url";
import { loadReadmeContent } from "../lib/readme-content";
import {
  buildCanonicalUrl,
  buildWebSiteJsonLd,
  resolveOgImage,
} from "../lib/seo";

export const Route = createFileRoute("/")({
  loader: async () => {
    return await loadReadmeContent();
  },
  head: () => {
    const title = "Lix | Version control system for files and tables";
    const description =
      "Embed version control for files of any format and SQL tables you define in Lix. Branch, merge, and roll back both; plugins add structured file diffs.";
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
      operatingSystem: "Any",
      programmingLanguage: ["Rust", "TypeScript"],
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

  return <LandingPage readmeHtml={html} />;
}
