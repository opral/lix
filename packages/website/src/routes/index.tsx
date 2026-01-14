import { createFileRoute } from "@tanstack/react-router";
import LandingPage from "../components/landing-page";
import {
  buildCanonicalUrl,
  buildWebSiteJsonLd,
  resolveOgImage,
} from "../lib/seo";

export const Route = createFileRoute("/")({
  head: () => {
    const description =
      "Lix is an embeddable change control system that enables Git-like history, versions, diffs, and blame for any file format.";
    const canonicalUrl = buildCanonicalUrl("/");
    const ogImage = resolveOgImage();
    const jsonLd = buildWebSiteJsonLd({
      title: "Lix",
      description,
      canonicalUrl,
    });

    return {
      meta: [
        { title: "Lix" },
        { name: "description", content: description },
        { property: "og:title", content: "Lix" },
        { property: "og:description", content: description },
        { property: "og:url", content: canonicalUrl },
        { property: "og:type", content: "website" },
        { property: "og:site_name", content: "Lix" },
        { property: "og:locale", content: "en_US" },
        { property: "og:image", content: ogImage.url },
        { property: "og:image:alt", content: ogImage.alt },
        { name: "twitter:card", content: "summary_large_image" },
        { name: "twitter:title", content: "Lix" },
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
  component: LandingPage,
});
