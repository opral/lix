const SITE_URL = "https://lix.dev";
const DEFAULT_OG_IMAGE_PATH = "/lix-features.svg";
const DEFAULT_OG_IMAGE_ALT = "Lix";

type MarkdownMetaInput = {
  rawMarkdown: string;
  frontmatter?: Record<string, unknown>;
};

type MetaEntry =
  | { name: string; content: string }
  | { property: string; content: string };

export function buildCanonicalUrl(pathname: string): string {
  if (!pathname) return SITE_URL;
  const normalized = pathname.startsWith("/") ? pathname : `/${pathname}`;
  return `${SITE_URL}${normalized}`;
}

export function resolveOgImage(frontmatter?: Record<string, unknown>) {
  const ogImage =
    (typeof frontmatter?.["og:image"] === "string"
      ? frontmatter["og:image"]
      : undefined) ??
    (typeof frontmatter?.["twitter:image"] === "string"
      ? frontmatter["twitter:image"]
      : undefined) ??
    DEFAULT_OG_IMAGE_PATH;
  const ogImageAlt =
    (typeof frontmatter?.["og:image:alt"] === "string"
      ? frontmatter["og:image:alt"]
      : undefined) ??
    (typeof frontmatter?.["twitter:image:alt"] === "string"
      ? frontmatter["twitter:image:alt"]
      : undefined) ??
    DEFAULT_OG_IMAGE_ALT;

  const url = normalizeAssetUrl(ogImage);
  return { url, alt: ogImageAlt };
}

export function getMarkdownTitle(input: MarkdownMetaInput) {
  const ogTitle =
    typeof input.frontmatter?.["og:title"] === "string"
      ? input.frontmatter["og:title"]
      : undefined;
  if (ogTitle) {
    return ogTitle;
  }

  return extractMarkdownH1(input.rawMarkdown);
}

export function getMarkdownDescription(input: MarkdownMetaInput) {
  const ogDescription =
    typeof input.frontmatter?.["og:description"] === "string"
      ? input.frontmatter["og:description"]
      : undefined;
  if (ogDescription) {
    return ogDescription;
  }

  return extractMarkdownDescription(input.rawMarkdown);
}

export function extractOgMeta(frontmatter?: Record<string, unknown>): MetaEntry[] {
  if (!frontmatter) return [];
  return Object.entries(frontmatter)
    .filter(
      ([key, value]) =>
        key.startsWith("og:") &&
        typeof value === "string" &&
        key !== "og:image" &&
        key !== "og:image:alt",
    )
    .map(([key, value]) => ({
      property: key,
      content: value as string,
    }));
}

export function extractTwitterMeta(
  frontmatter?: Record<string, unknown>,
): MetaEntry[] {
  if (!frontmatter) return [];
  return Object.entries(frontmatter)
    .filter(
      ([key, value]) =>
        key.startsWith("twitter:") &&
        typeof value === "string" &&
        key !== "twitter:image" &&
        key !== "twitter:image:alt",
    )
    .map(([key, value]) => ({
      name: key,
      content: value as string,
    }));
}

export function extractMarkdownH1(markdown: string) {
  if (!markdown) return undefined;
  const sanitized = stripFrontmatter(markdown);
  const lines = sanitized.split(/\r?\n/);
  for (const line of lines) {
    if (line.startsWith("# ")) {
      return line.slice(2).trim() || undefined;
    }
  }
  return undefined;
}

export function extractMarkdownDescription(markdown: string) {
  if (!markdown) return undefined;
  const sanitized = stripFrontmatter(markdown);
  const lines = sanitized.split(/\r?\n/);
  let collecting = false;
  const paragraph: string[] = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      if (collecting) break;
      continue;
    }
    if (trimmed.startsWith("#")) continue;
    if (trimmed.startsWith("![")) continue;
    if (
      trimmed.startsWith("- ") ||
      trimmed.startsWith("* ") ||
      /^\d+\.\s/.test(trimmed)
    ) {
      continue;
    }
    collecting = true;
    paragraph.push(trimmed);
  }

  if (!paragraph.length) return undefined;
  return paragraph.join(" ");
}

type WebPageJsonLdInput = {
  title: string;
  description?: string;
  canonicalUrl: string;
  image?: string;
};

export function buildWebPageJsonLd(input: WebPageJsonLdInput) {
  return {
    "@context": "https://schema.org",
    "@type": "WebPage",
    name: input.title,
    description: input.description,
    url: input.canonicalUrl,
    ...(input.image ? { image: input.image } : {}),
  };
}

type WebSiteJsonLdInput = {
  title: string;
  description?: string;
  canonicalUrl: string;
};

export function buildWebSiteJsonLd(input: WebSiteJsonLdInput) {
  return {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: input.title,
    description: input.description,
    url: input.canonicalUrl,
  };
}

type BreadcrumbItem = {
  name: string;
  item: string;
};

export function buildBreadcrumbJsonLd(items: BreadcrumbItem[]) {
  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: items.map((entry, index) => ({
      "@type": "ListItem",
      position: index + 1,
      name: entry.name,
      item: entry.item,
    })),
  };
}

function normalizeAssetUrl(value: string) {
  if (value.startsWith("http://") || value.startsWith("https://")) {
    return value;
  }
  if (value.startsWith("/")) {
    return `${SITE_URL}${value}`;
  }
  return `${SITE_URL}/${value}`;
}

function stripFrontmatter(markdown: string) {
  if (!markdown.startsWith("---")) return markdown;
  const end = markdown.indexOf("\n---", 3);
  if (end === -1) return markdown;
  return markdown.slice(end + 4).trimStart();
}
