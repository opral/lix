import { describe, expect, test } from "vitest";
import { normalizeMarkdownHtml } from "./markdown-html";

describe("normalizeMarkdownHtml", () => {
  test("turns markdown-wc GitHub alerts into callouts without literal markers", () => {
    expect(
      normalizeMarkdownHtml(
        '<blockquote data-mwc-alert="note"><p><span data-mwc-alert-marker="">[!NOTE]</span> Body</p></blockquote>',
      ),
    ).toBe(
      '<div class="callout note"><p class="callout-title">Note</p><p>Body</p></div>',
    );
  });

  test("wraps tables for scrolling without flattening table layout", () => {
    expect(
      normalizeMarkdownHtml(
        "<p>Before</p><table><thead><tr><th>A</th></tr></thead></table>",
      ),
    ).toContain(
      '<div class="table-wrapper"><table><thead><tr><th>A</th></tr></thead></table></div>',
    );
  });

  test("normalizes double-encoded entities once", () => {
    expect(
      normalizeMarkdownHtml("<p>AT&amp;amp;T &amp;lt;tag&amp;gt;</p>"),
    ).toBe("<p>AT&amp;T &lt;tag&gt;</p>");
  });
});
