import { describe, expect, test } from "vitest";
import { buildSeoFrontmatter } from "./plugin-readme-sync";

describe("buildSeoFrontmatter", () => {
  test("does not add a second period when the base description already ends with one", () => {
    const frontmatter = buildSeoFrontmatter({
      key: "plugin_json",
      name: "JSON Plugin",
      description: "Tracks JSON changes.",
      readme: "https://example.com/README.md",
    });

    expect(frontmatter).toContain(
      'description: "Tracks JSON changes. Learn how to install it, supported file types, and how it fits into Lix workflows."',
    );
    expect(frontmatter).not.toContain(".. Learn");
  });

  test("adds sentence punctuation when the base description is missing it", () => {
    const frontmatter = buildSeoFrontmatter({
      key: "plugin_json",
      name: "JSON Plugin",
      description: "Tracks JSON changes",
      readme: "https://example.com/README.md",
    });

    expect(frontmatter).toContain(
      'description: "Tracks JSON changes. Learn how to install it, supported file types, and how it fits into Lix workflows."',
    );
  });
});
