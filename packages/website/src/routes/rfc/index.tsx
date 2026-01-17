import { createFileRoute, Link } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { Footer } from "../../components/footer";
import { Header } from "../../components/header";

const rfcMarkdownFiles = import.meta.glob<string>("../../../../../rfcs/**/*.md", {
  query: "?raw",
  import: "default",
});

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

export const Route = createFileRoute("/rfc/")({
  loader: async () => {
    return await loadRfcIndex();
  },
  head: () => {
    return {
      meta: [
        { title: "RFCs | Lix" },
        { name: "description", content: "Lix Request for Comments" },
      ],
    };
  },
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
        <div className="mx-auto max-w-3xl px-6 py-16">
          <h1 className="mb-12 text-4xl font-bold tracking-tight text-slate-900">
            RFCs
          </h1>

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
