export type TocItem = {
  path: string;
  label: string;
};

export type Toc = Record<string, TocItem[]>;

export type DocRecord = {
  slug: string;
  /**
   * Raw markdown including frontmatter.
   */
  content: string;
  relativePath: string;
};

export type DocsByRelativePath = Record<string, DocRecord>;

/**
 * Converts file path entries in the table of contents into a quick lookup map.
 *
 * @example
 * buildTocMap({ Overview: [{ path: "./what-is-lix.md", label: "What is Lix?" }] });
 */
export function buildTocMap(toc: Toc): Map<string, TocItem> {
  const map = new Map<string, TocItem>();

  for (const items of Object.values(toc)) {
    for (const item of items) {
      const normalized = normalizeRelativePath(item.path);
      map.set(normalized, item);
    }
  }

  return map;
}

/**
 * Builds doc lookup maps keyed by slug.
 *
 * @example
 * buildDocMaps({ "/docs/what-is-lix.md": rawMarkdown });
 */
export function buildDocMaps(entries: Record<string, string>) {
  return Object.entries(entries).reduce(
    (acc, [filePath, raw]) => {
      const relativePath = normalizeRelativePath(filePath);
      const frontmatter = extractFrontmatter(raw);
      const frontmatterSlug = frontmatter?.slug?.trim() ?? "";
      const normalizedSlug = frontmatterSlug
        ? slugifyValue(frontmatterSlug)
        : "";
      const slug = normalizedSlug || slugifyFileName(relativePath);

      const record: DocRecord = {
        slug,
        content: raw,
        relativePath,
      };

      acc.bySlug[slug] = record;

      return acc;
    },
    {
      bySlug: {} as Record<string, DocRecord>,
    },
  );
}

/**
 * Resolves portable markdown file links to clean docs routes.
 *
 * Markdown files stay portable with links like `./storage.md`, while the site
 * renders them as `/docs/storage`.
 *
 * @example
 * resolveDocsMarkdownHref("./storage.md", { slug: "persistence", content: "", relativePath: "./persistence.md" }, { "./storage.md": { slug: "storage", content: "", relativePath: "./storage.md" } })
 */
export function resolveDocsMarkdownHref(
  href: string,
  currentDoc: DocRecord,
  docsByRelativePath: DocsByRelativePath,
) {
  if (
    href.startsWith("#") ||
    /^[a-z][a-z0-9+.-]*:/i.test(href) ||
    !href.replace(/[?#].*$/, "").endsWith(".md")
  ) {
    return undefined;
  }

  const hashIndex = href.indexOf("#");
  const hash = hashIndex === -1 ? "" : href.slice(hashIndex);
  const withoutHash = hashIndex === -1 ? href : href.slice(0, hashIndex);
  const queryIndex = withoutHash.indexOf("?");
  const query = queryIndex === -1 ? "" : withoutHash.slice(queryIndex);
  const pathOnly =
    queryIndex === -1 ? withoutHash : withoutHash.slice(0, queryIndex);

  const candidates = buildDocsLinkCandidates(pathOnly, currentDoc);

  for (const candidate of candidates) {
    const doc = docsByRelativePath[candidate];
    if (doc) {
      return `/docs/${doc.slug}${query}${hash}`;
    }
  }

  return undefined;
}

function buildDocsLinkCandidates(pathOnly: string, currentDoc: DocRecord) {
  const candidates = new Set<string>();
  const currentSlugPrefix = `/docs/${currentDoc.slug}/`;

  if (pathOnly.startsWith(currentSlugPrefix)) {
    candidates.add(
      resolveRelativeDocPath(
        currentDoc.relativePath,
        pathOnly.slice(currentSlugPrefix.length),
      ),
    );
  }

  if (pathOnly.startsWith("/docs/")) {
    candidates.add(normalizeRelativePath(pathOnly));
  } else {
    candidates.add(resolveRelativeDocPath(currentDoc.relativePath, pathOnly));
  }

  const fileName = pathOnly.split("/").pop();
  if (fileName?.endsWith(".md")) {
    candidates.add(resolveRelativeDocPath(currentDoc.relativePath, fileName));
  }

  return [...candidates];
}

function resolveRelativeDocPath(currentRelativePath: string, hrefPath: string) {
  const currentPath = currentRelativePath.replace(/^\.\//, "");
  const currentDirectory = currentPath.includes("/")
    ? currentPath.slice(0, currentPath.lastIndexOf("/"))
    : ".";
  const normalized = posixNormalize(`${currentDirectory}/${hrefPath}`);
  return normalized.startsWith(".") ? normalized : `./${normalized}`;
}

function posixNormalize(value: string) {
  const parts: string[] = [];
  for (const part of value.replace(/\\/g, "/").split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return parts.join("/");
}

/**
 * Normalizes a doc file path to a relative form rooted at docs.
 *
 * @example
 * normalizeRelativePath("/docs/guide/setup.md") // "./guide/setup.md"
 */
export function normalizeRelativePath(filePath: string) {
  return filePath
    .replace(/\\/g, "/")
    .replace(/^.*\/docs\//, "./")
    .replace(/^docs\//, "./");
}

/**
 * Produces a URL-safe slug base from a relative file path.
 *
 * @example
 * slugifyRelativePath("./guide/hello-world.md") // "guide-hello-world"
 */
export function slugifyRelativePath(relativePath: string) {
  const withoutExt = relativePath.replace(/\.md$/, "");
  return withoutExt
    .replace(/^\.\//, "")
    .replace(/[\/\\]+/g, "-")
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/**
 * Produces a URL-safe slug from a single filename.
 *
 * @example
 * slugifyFileName("./guide/hello-world.md") // "hello-world"
 */
export function slugifyFileName(relativePath: string) {
  const fileName = relativePath.split(/[\\/]/).pop() ?? relativePath;
  const withoutExt = fileName.replace(/\.md$/, "");
  return slugifyValue(withoutExt);
}

/**
 * Produces a URL-safe slug from a string value.
 *
 * @example
 * slugifyValue("Hello World") // "hello-world"
 */
export function slugifyValue(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/**
 * Extracts a minimal YAML frontmatter object from markdown.
 *
 * Only supports simple `key: value` pairs.
 *
 * @example
 * extractFrontmatter("---\\ntitle: Hello\\n---\\n# Title") // { title: "Hello" }
 */
function extractFrontmatter(markdown: string): Record<string, string> | null {
  const match = markdown.match(/^---\s*\n([\s\S]*?)\n---\s*\n?/);
  if (!match) {
    return null;
  }

  const lines = match[1].split("\n");
  const data: Record<string, string> = {};

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const separatorIndex = trimmed.indexOf(":");
    if (separatorIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    if (!key) {
      continue;
    }

    data[key] = value.replace(/^['"]|['"]$/g, "");
  }

  return data;
}
