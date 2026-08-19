import { createFileRoute } from "@tanstack/react-router";
import { Footer } from "../components/footer";
import { Header } from "../components/header";
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
    const title = "Lix README | Embeddable repository backend";
    const description =
      "The Lix README: an embeddable repository backend with files, SQL, and version control in one system.";
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
    <div className="flex min-h-screen flex-col bg-paper text-ink">
      <Header />
      <main className="mx-auto w-full max-w-[880px] flex-1 px-8 pb-[104px] pt-[72px]">
        <div className="overflow-hidden rounded-xl border border-line bg-white">
          <div className="flex flex-wrap items-center justify-between gap-5 border-b border-line-soft bg-[#FDFCFA] px-7 py-3.5">
            <span className="font-mono text-[12.5px] text-ink-faint">
              README.md · opral/lix
            </span>
            <a
              href="https://github.com/opral/lix"
              target="_blank"
              rel="noopener noreferrer"
              className="font-mono text-[12.5px] text-ink-muted transition-colors hover:text-cyan-deep"
            >
              view on GitHub →
            </a>
          </div>
          <div className="max-w-[800px] px-6 pb-12 pt-10 sm:px-12">
            <article
              className="markdown-wc-body"
              dangerouslySetInnerHTML={{ __html: html }}
            />
          </div>
        </div>
      </main>
      <Footer />
    </div>
  );
}
