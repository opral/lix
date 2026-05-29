const SITE_URL = "https://lix.dev";
const DEFAULT_OG_IMAGE_PATH = "/lix-features.svg";
const DEFAULT_OG_IMAGE_ALT = "Lix";
const DESCRIPTION_MAX_LENGTH = 160;
const DESCRIPTION_SENTENCE_MIN_LENGTH = 120;

type MarkdownMetaInput = {
  rawMarkdown: string;
  frontmatter?: Record<string, unknown>;
};

type MetaEntry =
  | { name: string; content: string }
  | { property: string; content: string };

export function buildCanonicalUrl(pathname: string): string {
  if (!pathname || pathname === "/") return SITE_URL;
  const normalized = pathname.startsWith("/") ? pathname : `/${pathname}`;
  const withoutTrailingSlash =
    normalized.endsWith("/") && normalized.length > 1
      ? normalized.slice(0, -1)
      : normalized;
  return `${SITE_URL}${withoutTrailingSlash}`;
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
  const title =
    typeof input.frontmatter?.title === "string"
      ? input.frontmatter.title
      : undefined;
  if (title) {
    return title.trim() || undefined;
  }

  const ogTitle =
    typeof input.frontmatter?.["og:title"] === "string"
      ? input.frontmatter["og:title"]
      : undefined;
  if (ogTitle) {
    return ogTitle.trim() || undefined;
  }

  return extractMarkdownH1(input.rawMarkdown);
}

export function getMarkdownDescription(input: MarkdownMetaInput) {
  const description =
    typeof input.frontmatter?.description === "string"
      ? input.frontmatter.description
      : undefined;
  if (description) {
    return normalizeDescriptionText(description);
  }

  const ogDescription =
    typeof input.frontmatter?.["og:description"] === "string"
      ? input.frontmatter["og:description"]
      : undefined;
  if (ogDescription) {
    return normalizeDescriptionText(ogDescription);
  }

  return extractMarkdownDescription(input.rawMarkdown);
}

export function extractOgMeta(
  frontmatter?: Record<string, unknown>,
): MetaEntry[] {
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
  let inCodeFence = false;
  let collecting = false;
  const paragraph: string[] = [];

  for (const line of lines) {
    const trimmed = line.trim();

    if (trimmed.startsWith("```") || trimmed.startsWith("~~~")) {
      inCodeFence = !inCodeFence;
      if (collecting) break;
      continue;
    }
    if (inCodeFence) continue;

    if (!trimmed) {
      if (collecting) break;
      continue;
    }
    if (trimmed.startsWith("#")) continue;
    if (trimmed.startsWith(">")) continue;
    if (trimmed.startsWith("![")) continue;
    if (trimmed.startsWith("<")) continue;
    if (isMarkdownTableLine(trimmed)) continue;
    if (isMarkdownListLine(trimmed)) {
      continue;
    }

    const normalized = normalizeDescriptionText(trimmed);
    if (!normalized) {
      if (collecting) break;
      continue;
    }

    collecting = true;
    paragraph.push(normalized);
  }

  if (!paragraph.length) return undefined;
  return clampDescription(paragraph.join(" "));
}

export function splitTitleFromHtml(html: string): {
  title?: string;
  body: string;
} {
  const match = html.match(/<h1\b[^>]*>([\s\S]*?)<\/h1>/i);
  if (!match) {
    return { body: html };
  }

  const title = decodeHtmlEntities(stripHtml(match[1])).trim();
  const body = html.replace(match[0], "").trimStart();
  return { title: title || undefined, body };
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

function normalizeDescriptionText(value: string) {
  return value
    .replace(/!\[([^\]]*)\]\(([^)]+)\)/g, "$1")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/__([^_]+)__/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/_([^_]+)_/g, "$1")
    .replace(/~~([^~]+)~~/g, "$1")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function clampDescription(value: string) {
  if (value.length <= DESCRIPTION_MAX_LENGTH) {
    return value;
  }

  const withinLimit = value.slice(0, DESCRIPTION_MAX_LENGTH);
  const sentenceBoundary = Math.max(
    withinLimit.lastIndexOf(". "),
    withinLimit.lastIndexOf("! "),
    withinLimit.lastIndexOf("? "),
  );
  if (sentenceBoundary >= DESCRIPTION_SENTENCE_MIN_LENGTH - 1) {
    return withinLimit.slice(0, sentenceBoundary + 1).trim();
  }

  const wordBoundary = withinLimit.lastIndexOf(" ");
  if (wordBoundary > 0) {
    return `${withinLimit.slice(0, wordBoundary).trim()}...`;
  }

  return `${withinLimit.trim()}...`;
}

function isMarkdownListLine(value: string) {
  return (
    value.startsWith("- ") ||
    value.startsWith("* ") ||
    value.startsWith("+ ") ||
    /^\d+\.\s/.test(value)
  );
}

function isMarkdownTableLine(value: string) {
  return (
    value.startsWith("|") ||
    /^\|?[\s:-]+\|[\s|:-]*$/.test(value) ||
    value.includes("| ---")
  );
}

function stripHtml(input: string): string {
  return input.replace(/<[^>]*>/g, "");
}

function decodeHtmlEntities(input: string): string {
  return input
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}
