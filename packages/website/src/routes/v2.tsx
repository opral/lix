import { createFileRoute } from "@tanstack/react-router";
import { Header } from "../components/header";
import { V2LandingPage } from "../components/v2-landing-page";
import {
  buildCanonicalUrl,
  buildWebSiteJsonLd,
  resolveOgImage,
} from "../lib/seo";

export const Route = createFileRoute("/v2")({
  head: () => {
    const title = "Lix v0.6 | An embeddable version control system";
    const description =
      "Lix v0.6 brings branches, semantic diffs, immutable history, and SQL-backed version control into your app as a library.";
    const canonicalUrl = buildCanonicalUrl("/v2");
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
      links: [{ rel: "canonical", href: canonicalUrl }],
      scripts: [
        {
          type: "application/ld+json",
          children: JSON.stringify(jsonLd),
        },
      ],
    };
  },
  component: V2Route,
});

function V2Route() {
  return (
    <>
      <Header />
      <V2LandingPage />
    </>
  );
}
