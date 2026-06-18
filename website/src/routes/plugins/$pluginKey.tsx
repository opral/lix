import { createFileRoute } from "@tanstack/react-router";
import { Header } from "../../components/header";
import { Footer } from "../../components/footer";
import {
  buildBreadcrumbJsonLd,
  buildCanonicalUrl,
  buildWebPageJsonLd,
} from "../../lib/seo";

const title = "Lix Plugins";
const description =
  "File plugins map formats into semantic entities. The public plugin directory is being rebuilt.";

export const Route = createFileRoute("/plugins/$pluginKey")({
  head: () => {
    const canonicalUrl = buildCanonicalUrl("/plugins");
    const jsonLd = buildWebPageJsonLd({
      title,
      description,
      canonicalUrl,
    });
    const breadcrumbJsonLd = buildBreadcrumbJsonLd([
      { name: "Lix", item: buildCanonicalUrl("/") },
      { name: "Plugins", item: canonicalUrl },
    ]);

    return {
      meta: [
        { title },
        { name: "description", content: description },
        { property: "og:title", content: title },
        { property: "og:description", content: description },
        { property: "og:url", content: canonicalUrl },
        { property: "og:type", content: "website" },
        { property: "og:site_name", content: "Lix" },
        { name: "twitter:card", content: "summary" },
        { name: "twitter:title", content: title },
        { name: "twitter:description", content: description },
      ],
      links: [{ rel: "canonical", href: canonicalUrl }],
      scripts: [
        {
          type: "application/ld+json",
          children: JSON.stringify(jsonLd),
        },
        {
          type: "application/ld+json",
          children: JSON.stringify(breadcrumbJsonLd),
        },
      ],
    };
  },
  component: PluginsComingSoonPage,
});

function PluginsComingSoonPage() {
  return (
    <div className="min-h-screen bg-white text-slate-900">
      <Header />
      <main className="mx-auto flex min-h-[60vh] w-full max-w-3xl flex-col justify-center px-6 py-24 text-center">
        <p className="text-sm font-medium uppercase tracking-wide text-[#0891B2]">
          Plugins
        </p>
        <h1 className="mt-4 text-4xl font-semibold tracking-tight sm:text-5xl">
          Plugin directory is being rebuilt
        </h1>
        <p className="mt-6 text-lg leading-8 text-slate-600">
          File plugins are available today. We are rewriting this section as
          part of the website cleanup.
        </p>
      </main>
      <Footer />
    </div>
  );
}
