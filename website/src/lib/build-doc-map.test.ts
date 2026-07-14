import { describe, expect, test } from "vitest";
import {
  buildDocMaps,
  buildTocMap,
  normalizeRelativePath,
  resolveDocsMarkdownHref,
  slugifyFileName,
  slugifyRelativePath,
} from "./build-doc-map";

describe("buildDocMaps", () => {
  test("creates slug records from markdown frontmatter", () => {
    const { bySlug } = buildDocMaps({
      "/docs/guide/hello-world.md": `---
slug: hello-doc
title: Hello World
description: Sample doc
---

# Hello world`,
      "/docs/reference/api.md": `---
title: API
---

API contents`,
    });

    expect(bySlug["hello-doc"].relativePath).toBe("./guide/hello-world.md");
    expect(bySlug["api"].relativePath).toBe("./reference/api.md");
  });
});

describe("buildTocMap", () => {
  test("normalizes relative file paths", () => {
    const tocMap = buildTocMap({
      Overview: [
        { path: "./what-is-lix.md", label: "What is Lix?" },
        { path: "/docs/guide/setup.md", label: "Setup" },
      ],
    });

    expect(tocMap.get("./what-is-lix.md")?.label).toBe("What is Lix?");
    expect(tocMap.get("./guide/setup.md")?.label).toBe("Setup");
  });
});

describe("path helpers", () => {
  test("normalizeRelativePath removes docs prefix", () => {
    expect(normalizeRelativePath("/docs/guide/setup.md")).toBe(
      "./guide/setup.md",
    );
  });

  test("normalizeRelativePath handles website-local legacy docs paths", () => {
    expect(normalizeRelativePath("/content/docs/guide/setup.md")).toBe(
      "./guide/setup.md",
    );
  });

  test("slugifyRelativePath flattens path into url safe slug", () => {
    expect(slugifyRelativePath("./guide/hello-world.md")).toBe(
      "guide-hello-world",
    );
  });

  test("slugifyFileName uses the filename without extension", () => {
    expect(slugifyFileName("./guide/hello-world.md")).toBe("hello-world");
  });
});

describe("resolveDocsMarkdownHref", () => {
  const currentDoc = {
    slug: "persistence",
    content: "",
    relativePath: "./persistence.md",
  };
  const docsByRelativePath = {
    "./storage.md": {
      slug: "storage",
      content: "",
      relativePath: "./storage.md",
    },
    "./versions.md": {
      slug: "versions",
      content: "",
      relativePath: "./versions.md",
    },
  };

  test("resolves portable markdown links to clean docs routes", () => {
    expect(
      resolveDocsMarkdownHref("./storage.md", currentDoc, docsByRelativePath),
    ).toBe("/docs/storage");
  });

  test("resolves page-url based markdown links to clean docs routes", () => {
    expect(
      resolveDocsMarkdownHref(
        "/docs/persistence/storage.md",
        currentDoc,
        docsByRelativePath,
      ),
    ).toBe("/docs/storage");
  });

  test("preserves heading hashes", () => {
    expect(
      resolveDocsMarkdownHref(
        "./versions.md#merge",
        currentDoc,
        docsByRelativePath,
      ),
    ).toBe("/docs/versions#merge");
  });
});
