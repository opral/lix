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
    const title = "Lix | Files, a SQL database, and version control in one repository";
    const description =
      "Lix is a repository you embed in your product. Agents read and write normal files, your product queries SQL, and Lix versions everything both write.";
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

  return <LandingPage readmeHtml={html} />;
}
