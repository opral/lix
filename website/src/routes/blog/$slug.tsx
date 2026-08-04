import { createFileRoute, Link, redirect } from "@tanstack/react-router";
import { parse } from "@opral/markdown-wc";
import { useEffect } from "react";
import markdownPageCss from "../../components/markdown-page.style.css?url";
import { getBlogDescription, getBlogTitle } from "../../blog/blogMetadata.js";
import { Footer } from "../../components/footer.js";
import { Header } from "../../components/header.js";
import { resolveOgImageUrl } from "../../blog/og-image.js";
import {
  buildCanonicalUrl,
  resolveOgImage,
  splitTitleFromHtml,
} from "../../lib/seo.js";
import { normalizeMarkdownHtml } from "../../lib/markdown-html.js";

const blogMarkdownFiles = import.meta.glob<string>("../../../../blog/**/*.md", {
  query: "?raw",
  import: "default",
});
const blogJsonFiles = import.meta.glob<string>("../../../../blog/*.json", {
  query: "?raw",
  import: "default",
});
const blogRootPrefix = "../../../../blog/";

const ogImageWidth = 1200;
const ogImageHeight = 630;

type Author = {
  name: string;
  role?: string;
  avatar?: string | null;
  twitter?: string;
  github?: string;
};

type BlogPrevNext = {
  slug: string;
  title: string;
} | null;

function calculateReadingTime(text: string): number {
  const wordsPerMinute = 200;
  const words = text.trim().split(/\s+/).length;
  return Math.max(1, Math.ceil(words / wordsPerMinute));
}

async function loadBlogPost(slug: string) {
  if (!slug) {
    throw new Error("Missing blog slug");
  }

  const authorsContent = await getBlogJson("authors.json");
  const authorsMap = JSON.parse(authorsContent) as Record<string, Author>;

  const tocContent = await getBlogJson("table_of_contents.json");
  const toc = JSON.parse(tocContent) as Array<{
    path: string;
    slug: string;
    authors?: string[];
  }>;

  // Load all posts to get dates from frontmatter for sorting
  const postsWithDates = await Promise.all(
    toc.map(async (item) => {
      const relPath = item.path.startsWith("./")
        ? item.path.slice(2)
        : item.path;
      const md = await getBlogMarkdown(relPath);
      const parsedMd = await parse(md);
      const date = parsedMd.frontmatter?.date as string | undefined;
      const title =
        getBlogTitle({ rawMarkdown: md, frontmatter: parsedMd.frontmatter }) ??
        item.slug;
      return { ...item, date, title };
    }),
  );

  const sortedToc = [...postsWithDates].sort((a, b) => {
    if (!a.date && !b.date) return 0;
    if (!a.date) return 1;
    if (!b.date) return -1;
    return new Date(b.date).getTime() - new Date(a.date).getTime();
  });

  const currentIndex = sortedToc.findIndex((item) => item.slug === slug);
  const entry = sortedToc[currentIndex];
  if (!entry) {
    throw new Error(`Blog post not found: ${slug}`);
  }

  const prevEntry = currentIndex > 0 ? sortedToc[currentIndex - 1] : null;
  const nextEntry =
    currentIndex < sortedToc.length - 1 ? sortedToc[currentIndex + 1] : null;

  const prevPost: BlogPrevNext = prevEntry
    ? { slug: prevEntry.slug, title: prevEntry.title }
    : null;
  const nextPost: BlogPrevNext = nextEntry
    ? { slug: nextEntry.slug, title: nextEntry.title }
    : null;

  const relativePath = entry.path.startsWith("./")
    ? entry.path.slice(2)
    : entry.path;
  // Extract folder name from path (e.g., "001-introducing-lix" from "001-introducing-lix/index.md")
  const folderName = relativePath.replace(/\/index\.md$/, "");
  const rawMarkdown = await getBlogMarkdown(relativePath);
  const parsed = await parse(rawMarkdown, {
    assetBaseUrl: `/blog/${folderName}/`,
  });
  const rawFrontmatterAuthors = parsed.frontmatter?.authors;
  const frontmatterAuthors = Array.isArray(rawFrontmatterAuthors)
    ? rawFrontmatterAuthors.filter(
        (authorId): authorId is string => typeof authorId === "string",
      )
    : undefined;
  const authorIds = frontmatterAuthors?.length
    ? frontmatterAuthors
    : entry.authors;
  const authors = authorIds
    ?.map((authorId) => authorsMap[authorId])
    .filter(Boolean);
  const rendered = splitTitleFromHtml(normalizeMarkdownHtml(parsed.html));
  const title =
    getBlogTitle({
      rawMarkdown,
      frontmatter: parsed.frontmatter,
    }) ?? rendered.title;
  const description = getBlogDescription({
    rawMarkdown,
    frontmatter: parsed.frontmatter,
  });

  // Get date from frontmatter
  const date = parsed.frontmatter?.date as string | undefined;
  const dateModified =
    (parsed.frontmatter?.dateModified as string | undefined) ??
    (parsed.frontmatter?.modified as string | undefined) ??
    (parsed.frontmatter?.updated as string | undefined) ??
    date;

  const ogImageOverrideRaw =
    typeof parsed.frontmatter?.["og:image"] === "string"
      ? parsed.frontmatter["og:image"]
      : undefined;
  const ogImageOverride = ogImageOverrideRaw
    ? resolveOgImageUrl(ogImageOverrideRaw, folderName)
    : undefined;
  const ogImageAlt =
    typeof parsed.frontmatter?.["og:image:alt"] === "string"
      ? parsed.frontmatter["og:image:alt"]
      : undefined;

  const readingTime = calculateReadingTime(rawMarkdown);
  const imports = parsed.frontmatter?.imports as string[] | undefined;

  // The cover is rendered above the article; drop a leading standalone image
  // from the body so it does not appear twice.
  let body = rendered.body;
  if (ogImageOverride) {
    body = body.replace(
      /^\s*<p[^>]*>\s*(?:<a[^>]*>)?\s*<img[^>]*>\s*(?:<\/a>)?\s*<\/p>/,
      "",
    );
  }

  return {
    post: {
      slug: entry.slug,
      title,
      description,
      date,
      dateModified,
      authors,
      readingTime,
      ogImage: ogImageOverride,
      ogImageAlt,
      imports,
    },
    html: body,
    rawMarkdown,
    prevPost,
    nextPost,
  };
}

type BlogPostLoaderData = Awaited<ReturnType<typeof loadBlogPost>>;

export function buildBlogPostHead(loaderData?: BlogPostLoaderData) {
  const title = loaderData?.post.title;
  const description = loaderData?.post.description;
  const slug = loaderData?.post.slug;
  const defaultOg = resolveOgImage();
  const ogImageUrl = loaderData?.post.ogImage ?? defaultOg.url;
  const ogImageAlt =
    loaderData?.post.ogImageAlt ?? (title ? `${title} cover` : "Lix blog post");
  const dateModified = loaderData?.post.dateModified ?? loaderData?.post.date;
  const canonicalUrl = slug
    ? buildCanonicalUrl(`/blog/${slug}`)
    : buildCanonicalUrl("/blog");
  const meta: Array<
    | { title: string }
    | { name: string; content: string }
    | { property: string; content: string }
  > = [
    { title: title ? `${title} | Lix Blog` : "Lix Blog" },
    { property: "og:url", content: canonicalUrl },
    { property: "og:type", content: "article" },
    { property: "og:site_name", content: "Lix" },
    { property: "og:locale", content: "en_US" },
    { property: "og:image", content: ogImageUrl },
    { property: "og:image:width", content: String(ogImageWidth) },
    { property: "og:image:height", content: String(ogImageHeight) },
    { property: "og:image:alt", content: ogImageAlt },
    { name: "twitter:card", content: "summary_large_image" },
    { name: "twitter:image", content: ogImageUrl },
    { name: "twitter:image:alt", content: ogImageAlt },
  ];

  if (description) {
    meta.push(
      { name: "description", content: description },
      { property: "og:description", content: description },
      { name: "twitter:description", content: description },
    );
  }

  if (title) {
    const pageTitle = `${title} | Lix Blog`;
    meta.push(
      { property: "og:title", content: pageTitle },
      { name: "twitter:title", content: pageTitle },
    );
  }

  if (loaderData?.post.date) {
    meta.push({
      property: "article:published_time",
      content: loaderData.post.date,
    });
  }

  if (loaderData?.post.authors) {
    loaderData.post.authors.forEach((author) => {
      meta.push({
        property: "article:author",
        content: author.name,
      });
    });
  }

  const links = [
    { rel: "stylesheet", href: markdownPageCss },
    { rel: "canonical", href: canonicalUrl },
  ];
  if (loaderData?.prevPost?.slug) {
    links.push({
      rel: "prev",
      href: buildCanonicalUrl(`/blog/${loaderData.prevPost.slug}`),
    });
  }
  if (loaderData?.nextPost?.slug) {
    links.push({
      rel: "next",
      href: buildCanonicalUrl(`/blog/${loaderData.nextPost.slug}`),
    });
  }

  return {
    meta,
    links,
    scripts: slug
      ? [
          {
            type: "application/ld+json",
            children: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "BlogPosting",
              headline: title ?? slug,
              description,
              mainEntityOfPage: {
                "@type": "WebPage",
                "@id": canonicalUrl,
              },
              image: ogImageUrl,
              ...(loaderData?.post.date
                ? { datePublished: loaderData.post.date }
                : {}),
              ...(dateModified ? { dateModified } : {}),
              ...(loaderData?.post.authors
                ? {
                    author: loaderData.post.authors.map((author) => ({
                      "@type": "Person",
                      name: author.name,
                      ...(author.avatar ? { image: author.avatar } : {}),
                      ...(author.twitter || author.github
                        ? {
                            sameAs: [author.twitter, author.github].filter(
                              (value): value is string => Boolean(value),
                            ),
                          }
                        : {}),
                    })),
                  }
                : {}),
              publisher: {
                "@type": "Organization",
                name: "Lix",
                url: buildCanonicalUrl("/"),
                logo: {
                  "@type": "ImageObject",
                  url: buildCanonicalUrl("/opengraph/lix.png"),
                },
              },
            }),
          },
        ]
      : [],
  };
}

export const Route = createFileRoute("/blog/$slug")({
  loader: async ({ params }) => {
    try {
      return await loadBlogPost(params.slug);
    } catch {
      throw redirect({ to: "/blog" });
    }
  },
  head: ({ loaderData }) => buildBlogPostHead(loaderData),
  component: BlogPostPage,
});

function BlogPostPage() {
  const { post, html, prevPost, nextPost } = Route.useLoaderData();

  useEffect(() => {
    if (!post.imports || post.imports.length === 0) return;
    post.imports.forEach((url) => {
      import(/* @vite-ignore */ url).catch((err) => {
        console.error(`Failed to load web component from ${url}:`, err);
      });
    });
  }, [post.imports]);

  useEffect(() => {
    // @ts-expect-error - JS-only module
    import("../../components/markdown-page.interactive.js");
  }, [html]);

  return (
    <div className="flex min-h-screen flex-col bg-paper text-ink">
      <Header />
      <main className="mx-auto w-full max-w-[784px] flex-1 px-8 pb-[104px] pt-16">
        <Link
          to="/blog"
          className="font-mono text-[12.5px] text-ink-muted transition-colors hover:text-cyan-deep"
        >
          ← Blog
        </Link>
        {post.date && (
          <p className="mt-9 font-mono text-[12.5px] text-ink-faint">
            {formatDate(post.date)}
          </p>
        )}
        <h1 className="mt-3.5 text-balance text-[32px] font-bold leading-[1.12] tracking-[-0.03em] sm:text-[40px]">
          {post.title}
        </h1>

        {post.authors && post.authors.length > 0 && (
          <div className="mt-[22px] flex flex-wrap items-center gap-5">
            {post.authors.map((author, index) => (
              <div key={index} className="flex items-center gap-2.5">
                {author.avatar ? (
                  <img
                    src={author.avatar}
                    alt={author.name}
                    className="block h-7 w-7 rounded-full object-cover"
                  />
                ) : (
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-line text-xs font-medium text-ink-muted">
                    {author.name.charAt(0)}
                  </span>
                )}
                {author.github ? (
                  <a
                    href={author.github}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm font-semibold text-ink-secondary transition-colors hover:text-cyan-deep"
                  >
                    {author.name}
                  </a>
                ) : (
                  <span className="text-sm font-semibold text-ink-secondary">
                    {author.name}
                  </span>
                )}
              </div>
            ))}
          </div>
        )}

        {post.ogImage && (
          <img
            src={post.ogImage}
            alt={post.ogImageAlt ?? `${post.title} cover`}
            className="mt-9 block h-auto w-full rounded-[10px] border border-line bg-white"
          />
        )}

        <article
          className="markdown-wc-body mt-9"
          dangerouslySetInnerHTML={{ __html: html }}
        />

        {(prevPost || nextPost) && (
          <div className="mt-16 flex justify-between gap-4 border-t border-line pt-5">
            {nextPost ? (
              <Link
                to="/blog/$slug"
                params={{ slug: nextPost.slug }}
                className="flex flex-col gap-1"
              >
                <span className="font-mono text-[11px] uppercase tracking-[0.08em] text-ink-faint">
                  Older
                </span>
                <span className="text-[15px] font-semibold text-cyan-deep">
                  ← {nextPost.title}
                </span>
              </Link>
            ) : (
              <span />
            )}
            {prevPost ? (
              <Link
                to="/blog/$slug"
                params={{ slug: prevPost.slug }}
                className="flex flex-col items-end gap-1 text-right"
              >
                <span className="font-mono text-[11px] uppercase tracking-[0.08em] text-ink-faint">
                  Newer
                </span>
                <span className="text-[15px] font-semibold text-cyan-deep">
                  {prevPost.title} →
                </span>
              </Link>
            ) : (
              <span />
            )}
          </div>
        )}
      </main>
      <Footer />
    </div>
  );
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
