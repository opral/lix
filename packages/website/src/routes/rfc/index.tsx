import { createFileRoute, Link } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { Footer } from "../../components/footer";
import { Header } from "../../components/header";
import {
  buildCanonicalUrl,
  buildWebPageJsonLd,
  resolveOgImage,
} from "../../lib/seo";

const rfcMarkdownFiles = import.meta.glob<string>(
  "../../../../../rfcs/**/index.md",
  {
  query: "?raw",
  import: "default",
  }
);

const rfcRootPrefix = "../../../../../rfcs/";

type RfcEntry = {
  slug: string;
  title: string;
  date?: string;
};

async function loadRfcIndex(): Promise<{ rfcs: RfcEntry[] }> {
  const rfcPaths = Object.keys(rfcMarkdownFiles);

  const rfcs = await Promise.all(
    rfcPaths.map(async (path) => {
      // Extract slug from path like "../../../../../rfcs/001-preprocess-writes/index.md"
      const slug = path.replace(rfcRootPrefix, "").replace("/index.md", "");
      const rawMarkdown = await rfcMarkdownFiles[path]();
      const parsed = await parse(rawMarkdown);

      // Extract title from frontmatter or first h1
      let title = slug;
      if (parsed.frontmatter?.title) {
        title = parsed.frontmatter.title as string;
      } else {
        const h1Match = rawMarkdown.match(/^#\s+(.+)$/m);
        if (h1Match) {
          title = h1Match[1];
        }
      }

      // Extract date from frontmatter
      const date = parsed.frontmatter?.date as string | undefined;

      return { slug, title, date };
    })
  );

  // Sort Z-A (descending by slug, so 002 comes before 001)
  rfcs.sort((a, b) => b.slug.localeCompare(a.slug));

  return { rfcs };
}

export function buildRfcIndexHead() {
  const title = "Lix RFCs | Design proposals, architecture decisions, and roadmap notes";
  const description =
    "Read Lix RFCs covering architecture decisions, engine design, and upcoming changes before they land in the product.";
  const canonicalUrl = buildCanonicalUrl("/rfc");
  const ogImage = resolveOgImage();
  const jsonLd = buildWebPageJsonLd({
    title,
    description,
    canonicalUrl,
    image: ogImage.url,
  });

  return {
    links: [{ rel: "canonical", href: canonicalUrl }],
    scripts: [
      {
        type: "application/ld+json",
        children: JSON.stringify(jsonLd),
      },
    ],
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
  };
}

export const Route = createFileRoute("/rfc/")({
  loader: async () => {
    return await loadRfcIndex();
  },
  head: () => buildRfcIndexHead(),
  component: RfcIndexPage,
});

function formatDate(dateString: string): string {
  try {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  } catch {
    return dateString;
  }
}

function RfcIndexPage() {
  const { rfcs } = Route.useLoaderData();

  return (
    <div className="flex min-h-screen flex-col bg-white text-slate-900">
      <Header />
      <main className="flex-1">
        <div className="mx-auto max-w-4xl px-6 py-16">
          <h1 className="mb-12 text-4xl font-bold tracking-tight text-slate-900">
            RFCs
          </h1>

          <p className="mb-10 max-w-3xl text-base leading-7 text-slate-600">
            Request for Comments capture the design proposals, architectural
            tradeoffs, and implementation plans behind major Lix changes.
          </p>

          <div className="flex flex-col gap-8">
            {rfcs.map((rfc) => {
              const rfcNumber = rfc.slug.match(/^(\d+)/)?.[1] ?? "";
              return (
                <Link
                  key={rfc.slug}
                  to="/rfc/$slug"
                  params={{ slug: rfc.slug }}
                  className="group block transition-colors hover:text-cyan-700"
                >
                  <div className="flex items-baseline justify-between mb-1">
                    <span className="text-sm text-slate-400 font-mono">
                      RFC {rfcNumber}
                    </span>
                    {rfc.date && (
                      <span className="text-sm text-slate-400">
                        {formatDate(rfc.date)}
                      </span>
                    )}
                  </div>
                  <span className="text-base font-medium underline decoration-slate-300 underline-offset-4 group-hover:decoration-cyan-500">
                    {rfc.title}
                  </span>
                </Link>
              );
            })}
          </div>
        </div>
      </main>
      <Footer />
    </div>
  );
}
