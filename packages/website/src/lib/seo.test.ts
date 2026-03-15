import { describe, expect, test } from "vitest";
import { resolveOgImageUrl } from "../blog/og-image";
import {
  buildCanonicalUrl,
  getMarkdownDescription,
  getMarkdownTitle,
  splitTitleFromHtml,
} from "./seo";

describe("buildCanonicalUrl", () => {
  test("keeps the site root canonical without changing it", () => {
    expect(buildCanonicalUrl("/")).toBe("https://lix.dev");
  });

  test("normalizes route paths to no-trailing-slash canonicals", () => {
    expect(buildCanonicalUrl("/blog")).toBe("https://lix.dev/blog");
    expect(buildCanonicalUrl("docs/what-is-lix")).toBe(
      "https://lix.dev/docs/what-is-lix",
    );
    expect(buildCanonicalUrl("/rfc/002-rewrite-in-rust/")).toBe(
      "https://lix.dev/rfc/002-rewrite-in-rust",
    );
  });

  test("keeps file-like paths canonicalized without extra slash", () => {
    expect(buildCanonicalUrl("/lix-features.svg")).toBe(
      "https://lix.dev/lix-features.svg",
    );
  });
});

describe("getMarkdownTitle", () => {
  test("prefers explicit frontmatter title over og title and markdown h1", () => {
    expect(
      getMarkdownTitle({
        rawMarkdown: "# Markdown Title",
        frontmatter: {
          title: "Frontmatter Title",
          "og:title": "OG Title",
        },
      }),
    ).toBe("Frontmatter Title");
  });

  test("falls back to og title when explicit title is missing", () => {
    expect(
      getMarkdownTitle({
        rawMarkdown: "# Markdown Title",
        frontmatter: {
          "og:title": "OG Title",
        },
      }),
    ).toBe("OG Title");
  });
});

describe("getMarkdownDescription", () => {
  test("prefers explicit frontmatter description over og description and prose", () => {
    expect(
      getMarkdownDescription({
        rawMarkdown: "# Title\n\nMarkdown description.",
        frontmatter: {
          description: "Frontmatter description.",
          "og:description": "OG description.",
        },
      }),
    ).toBe("Frontmatter description.");
  });

  test("falls back to og description when explicit description is missing", () => {
    expect(
      getMarkdownDescription({
        rawMarkdown: "# Title\n\nMarkdown description.",
        frontmatter: {
          "og:description": "OG description.",
        },
      }),
    ).toBe("OG description.");
  });

  test("extracts clean prose and skips admonitions, code, lists, tables, and images", () => {
    const markdown = `# Validation Rules

> [!NOTE]
> Proposed feature.

\`\`\`ts
const nope = true;
\`\`\`

- list item
| name | value |
| --- | --- |
![Diagram](/example.png)

Validation rules catch **mistakes** before [changes](/docs/change-proposals) ship and keep \`agents\` and humans aligned.

## Next

More content here.
`;

    expect(getMarkdownDescription({ rawMarkdown: markdown })).toBe(
      "Validation rules catch mistakes before changes ship and keep agents and humans aligned.",
    );
  });

  test("clamps long fallback descriptions at a safe boundary", () => {
    const markdown = `# Long Form

Lix tracks semantic changes across structured files so teams can review AI-generated edits, audit what changed, and restore safe states without relying on brittle line-based diffs or app-specific APIs alone.
`;

    const description = getMarkdownDescription({ rawMarkdown: markdown });
    expect(description).toBe(
      "Lix tracks semantic changes across structured files so teams can review AI-generated edits, audit what changed, and restore safe states without relying on...",
    );
    expect(description?.length).toBeLessThanOrEqual(160);
  });
});

describe("splitTitleFromHtml", () => {
  test("removes the first h1 from rendered html", () => {
    expect(
      splitTitleFromHtml("<h1>RFC &amp; Notes</h1><p>Body copy</p>"),
    ).toEqual({
      title: "RFC & Notes",
      body: "<p>Body copy</p>",
    });
  });
});

describe("resolveOgImageUrl", () => {
  test("resolves blog-local images within the post folder", () => {
    expect(
      resolveOgImageUrl(
        "./cover.jpg",
        "002-modeling-a-company-as-a-repository",
      ),
    ).toBe(
      "https://lix.dev/blog/002-modeling-a-company-as-a-repository/cover.jpg",
    );
  });
});
