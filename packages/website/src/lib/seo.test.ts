import { describe, expect, test } from "vitest";
import { buildCanonicalUrl } from "./seo";

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
