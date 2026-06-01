import { createFileRoute } from "@tanstack/react-router";
import { Header } from "../components/header";
import { LandingReadme } from "../components/landing-page";
import { V2Footer, V2Hero } from "../components/v2-landing-page";
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
    const title = "Lix README | An embeddable version control system";
    const description =
      "Read the Lix project README, including package details, examples, and repository information.";
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
