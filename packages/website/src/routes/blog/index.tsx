import { createFileRoute, Link } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { parse } from "@opral/markdown-wc";
import { getBlogDescription, getBlogTitle } from "../../blog/blogMetadata";
import { Footer } from "../../components/footer";
import { Header } from "../../components/header";
import { buildCanonicalUrl, resolveOgImage } from "../../lib/seo";

type Author = {
  name: string;
  avatar?: string | null;
};

const blogMarkdownFiles = import.meta.glob<string>(
  "../../../../../blog/**/*.md",
  {
    query: "?raw",
    import: "default",
  },
);
const blogJsonFiles = import.meta.glob<string>("../../../../../blog/*.json", {
  query: "?raw",
  import: "default",
});
const blogRootPrefix = "../../../../../blog/";

const loadBlogIndex = createServerFn({ method: "GET" }).handler(async () => {
  const authorsContent = await getBlogJson("authors.json");
  const authorsMap = JSON.parse(authorsContent) as Record<
    string,
    { name: string; avatar?: string | null }
  >;

  const tocContent = await getBlogJson("table_of_contents.json");
  const toc = JSON.parse(tocContent) as Array<{
    path: string;
    slug: string;
    date?: string;
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

      const ogImageRaw =
        typeof parsed.frontmatter?.["og:image"] === "string"
          ? parsed.frontmatter["og:image"]
          : undefined;
      const ogImage = ogImageRaw
        ? resolveLocalBlogAsset(ogImageRaw, item.slug)
        : undefined;

      return {
        slug: item.slug,
        title,
        description,
        date: item.date,
        authors,
        ogImage,
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
});

export const Route = createFileRoute("/blog/")({
  loader: async () => {
    return await loadBlogIndex();
  },
  head: () => {
    const canonicalUrl = buildCanonicalUrl("/blog");
    const description =
      "Updates and insights on Lix change control and developer workflows.";
    const ogImage = resolveOgImage();

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
        { title: "Blog | Lix" },
        { name: "description", content: description },
        { property: "og:title", content: "Blog | Lix" },
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
        { name: "twitter:title", content: "Blog | Lix" },
        { name: "twitter:description", content: description },
      ],
    };
  },
  component: BlogIndexPage,
});

function BlogIndexPage() {
  const { posts } = Route.useLoaderData();

  return (
    <div className="flex min-h-screen flex-col bg-white text-slate-900">
      <Header />
      <main className="flex-1">
        <div className="mx-auto max-w-3xl px-6 py-16">
          <h1 className="mb-6 text-4xl font-bold tracking-tight text-slate-900">
            Blog
          </h1>

          <form
            action="https://buttondown.com/api/emails/embed-subscribe/lix-blog"
            method="post"
            target="_blank"
            className="embeddable-buttondown-form mb-12"
          >
            <p className="mb-3 text-sm text-slate-500">
              Get notified about new blog posts
            </p>
            <div className="flex gap-2">
              <label htmlFor="bd-email" className="sr-only">
                Enter your email
              </label>
              <input
                type="email"
                name="email"
                id="bd-email"
                placeholder="your@email.com"
                required
                className="flex-1 rounded-md border border-slate-300 px-4 py-2 text-sm focus:border-transparent focus:outline-none focus:ring-2 focus:ring-slate-900"
              />
              <input
                type="submit"
                value="Subscribe"
                className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-900 transition-colors hover:bg-slate-50"
              />
            </div>
          </form>

          <div className="flex flex-col gap-6">
            {posts.map((post) => (
              <Link
                key={post.slug}
                to="/blog/$slug"
                params={{ slug: post.slug }}
                className="group -mx-6 block rounded-xl p-6 transition-colors hover:bg-slate-50"
              >
                <article className="flex gap-6">
                  {post.ogImage && (
                    <div className="h-24 w-40 flex-shrink-0 overflow-hidden rounded-lg bg-slate-100">
                      <img
                        src={post.ogImage}
                        alt=""
                        className="h-full w-full object-cover"
                      />
                    </div>
                  )}
                  <div className="min-w-0 flex-1">
                    <h2 className="text-xl font-semibold text-slate-900 transition-colors group-hover:text-slate-700">
                      {post.title ?? post.slug}
                    </h2>
                    {post.description && (
                      <p className="mt-2 line-clamp-2 text-sm text-slate-600">
                        {post.description}
                      </p>
                    )}
                    <div className="mt-3 flex items-center gap-2 text-sm text-slate-500">
                      {post.authors && post.authors.length > 0 && (
                        <>
                          {post.authors.map((author, index) => (
                            <div
                              key={index}
                              className="flex items-center gap-2"
                            >
                              {author.avatar ? (
                                <img
                                  src={author.avatar}
                                  alt={author.name}
                                  className="h-5 w-5 rounded-full object-cover"
                                />
                              ) : (
                                <div className="flex h-5 w-5 items-center justify-center rounded-full bg-slate-300 text-xs font-medium text-slate-600">
                                  {author.name.charAt(0)}
                                </div>
                              )}
                              <span>{author.name}</span>
                            </div>
                          ))}
                          {post.date && (
                            <span className="text-slate-300">·</span>
                          )}
                        </>
                      )}
                      {post.date && <time>{formatDate(post.date)}</time>}
                    </div>
                  </div>
                </article>
              </Link>
            ))}
          </div>
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
      month: "long",
      day: "numeric",
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

function resolveLocalBlogAsset(value: string, slug: string): string {
  if (/^[a-z][a-z0-9+.-]*:/.test(value)) return value;
  const normalized = value.replace(/^\.\//, "");
  return `/blog/${slug}/${normalized}`;
}
