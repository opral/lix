import { createFileRoute, Link } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { getBlogDescription, getBlogTitle } from "../../blog/blogMetadata";
import { resolveBlogAssetPath } from "../../blog/og-image";
import { Footer } from "../../components/footer";
import { Header } from "../../components/header";
import { buildCanonicalUrl, resolveOgImage } from "../../lib/seo";

type Author = {
  name: string;
  avatar?: string | null;
};

const blogMarkdownFiles = import.meta.glob<string>("../../../../blog/**/*.md", {
  query: "?raw",
  import: "default",
});
const blogJsonFiles = import.meta.glob<string>("../../../../blog/*.json", {
  query: "?raw",
  import: "default",
});
const blogRootPrefix = "../../../../blog/";

async function loadBlogIndex() {
  const authorsContent = await getBlogJson("authors.json");
  const authorsMap = JSON.parse(authorsContent) as Record<
    string,
    { name: string; avatar?: string | null }
  >;

  const tocContent = await getBlogJson("table_of_contents.json");
  const toc = JSON.parse(tocContent) as Array<{
    path: string;
    slug: string;
    authors?: string[];
  }>;

  const posts = await Promise.all(
    toc.map(async (item) => {
      const relativePath = item.path.startsWith("./")
        ? item.path.slice(2)
        : item.path;
      const rawMarkdown = await getBlogMarkdown(relativePath);
      const parsed = await parse(rawMarkdown);
      const title = getBlogTitle({
        rawMarkdown,
        frontmatter: parsed.frontmatter,
      });
      const description = getBlogDescription({
        rawMarkdown,
        frontmatter: parsed.frontmatter,
      });

      const authors = item.authors
        ?.map((authorId) => authorsMap[authorId])
        .filter(Boolean) as Author[] | undefined;

      // Extract folder name from path (e.g., "001-introducing-lix" from "001-introducing-lix/index.md")
      const folderName = relativePath.replace(/\/index\.md$/, "");
      const ogImageRaw =
        typeof parsed.frontmatter?.["og:image"] === "string"
          ? parsed.frontmatter["og:image"]
          : undefined;
      const ogImage = ogImageRaw
        ? resolveBlogAssetPath(ogImageRaw, folderName)
        : undefined;
      const ogImageAlt =
        (typeof parsed.frontmatter?.["og:image:alt"] === "string"
          ? parsed.frontmatter["og:image:alt"]
          : undefined) ??
        (typeof parsed.frontmatter?.["twitter:image:alt"] === "string"
          ? parsed.frontmatter["twitter:image:alt"]
          : undefined) ??
        (title ? `${title} cover image` : undefined);

      // Get date from frontmatter
      const date = parsed.frontmatter?.date as string | undefined;

      return {
        slug: item.slug,
        title,
        description,
        date,
        authors,
        ogImage,
        ogImageAlt,
      };
    }),
  );

  posts.sort((a, b) => {
    if (!a.date && !b.date) return 0;
    if (!a.date) return 1;
    if (!b.date) return -1;
    return new Date(b.date).getTime() - new Date(a.date).getTime();
  });

  return { posts };
}

export const Route = createFileRoute("/blog/")({
  loader: async () => {
    return await loadBlogIndex();
  },
  head: () => {
    const canonicalUrl = buildCanonicalUrl("/blog");
    const description =
      "Product updates, architecture notes, and experiments from building Lix for AI agents and structured file workflows.";
    const ogImage = resolveOgImage();
    const title =
      "Lix Blog | Product updates, architecture notes, and AI workflow ideas";

    return {
      links: [{ rel: "canonical", href: canonicalUrl }],
      scripts: [
        {
          type: "application/ld+json",
          children: JSON.stringify({
            "@context": "https://schema.org",
            "@type": "Blog",
            name: "Blog | Lix",
            description,
            url: canonicalUrl,
          }),
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
        { name: "twitter:image", content: ogImage.url },
        { name: "twitter:image:alt", content: ogImage.alt },
        { name: "twitter:title", content: title },
        { name: "twitter:description", content: description },
      ],
    };
  },
  component: BlogIndexPage,
});

function BlogIndexPage() {
  const { posts } = Route.useLoaderData();

  return (
    <div className="flex min-h-screen flex-col bg-paper text-ink">
      <Header />
      <main className="mx-auto w-full max-w-[880px] flex-1 px-8 pb-[104px] pt-[72px]">
        <h1 className="text-[44px] font-bold tracking-[-0.032em]">Blog</h1>
        <p className="mt-4 text-[17px] leading-[1.6] text-ink-muted">
          Release notes and engineering updates from the Lix team.
        </p>

        <div className="mt-12 flex flex-col">
          {posts.map((post, index) => (
            <Link
              key={post.slug}
              to="/blog/$slug"
              params={{ slug: post.slug }}
              className={`group grid grid-cols-1 items-center gap-4 border-t border-line py-[30px] sm:grid-cols-[120px_236px_1fr] sm:gap-8 ${
                index === posts.length - 1 ? "border-b" : ""
              }`}
            >
              <span className="font-mono text-[12.5px] text-ink-faint">
                {post.date ? formatDate(post.date) : ""}
              </span>
              {post.ogImage ? (
                <img
                  src={post.ogImage}
                  alt={
                    post.ogImageAlt ?? `${post.title ?? post.slug} cover image`
                  }
                  className="block aspect-[1.91/1] w-full max-w-[236px] rounded-lg border border-line bg-white object-cover"
                />
              ) : (
                <span
                  className="flex aspect-[1.91/1] w-full max-w-[236px] items-center justify-center rounded-lg border border-line"
                  style={{
                    backgroundImage:
                      "repeating-linear-gradient(135deg, #F4F2EC 0px, #F4F2EC 8px, #FBFAF7 8px, #FBFAF7 16px)",
                  }}
                >
                  <span className="rounded-[5px] border border-line bg-paper px-[9px] py-[5px] font-mono text-[11px] text-ink-faint">
                    no og image
                  </span>
                </span>
              )}
              <span className="flex flex-col gap-2">
                <span className="text-xl font-bold tracking-[-0.015em] text-ink transition-colors group-hover:text-cyan-deep">
                  {post.title ?? post.slug}
                </span>
                {post.description && (
                  <span className="text-[15px] leading-[1.6] text-ink-muted">
                    {post.description}
                  </span>
                )}
              </span>
            </Link>
          ))}
        </div>
      </main>
      <Footer />
    </div>
  );
}

function formatDate(dateString: string): string {
  try {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
      timeZone: "UTC",
    });
  } catch {
    return dateString;
  }
}

function getBlogJson(filename: string): Promise<string> {
  const loader = blogJsonFiles[`${blogRootPrefix}${filename}`];
  if (!loader) {
    throw new Error(`Missing blog file: ${filename}`);
  }
  return loader();
}

function getBlogMarkdown(relativePath: string): Promise<string> {
  const normalized = relativePath.replace(/^[./]+/, "");
  const loader = blogMarkdownFiles[`${blogRootPrefix}${normalized}`];
  if (!loader) {
    throw new Error(`Missing blog markdown: ${relativePath}`);
  }
  return loader();
}
