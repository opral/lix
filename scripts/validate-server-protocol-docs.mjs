#!/usr/bin/env node
// Keeps the endpoint table in docs/server-protocol.md in sync with the
// canonical OpenAPI document. The prose on that page is hand-written on
// purpose; only the path list is derived, so only the path list is checked.
import { readFileSync } from "node:fs";
import path from "node:path";

const SPEC = "packages/lix/server-protocol.openapi.yaml";
const DOC = "docs/server-protocol.md";

/**
 * Reads the top-level path keys from an OpenAPI document.
 *
 * @example
 * specPaths("paths:\n  /lix/v1:\n    get: {}\n") // ["/lix/v1"]
 */
export function specPaths(yaml) {
  const body = yaml.split(/^paths:$/m)[1];
  if (body === undefined) {
    throw new Error(`${SPEC} has no top-level "paths:" block`);
  }
  const found = [];
  for (const line of body.split("\n")) {
    if (/^\S/.test(line) && line.trim() !== "") break; // next top-level key
    const match = line.match(/^ {2}(\/\S*):$/);
    if (match) found.push(match[1]);
  }
  return found;
}

/**
 * Reads the paths listed in the doc's "## Surface" table, expanding `{a,b}`
 * shorthand into one path each. Prose elsewhere on the page is ignored.
 *
 * @example
 * docPaths("## Surface\n| SQL | `/lix/v1/transaction/{begin,commit}` |")
 * // ["/lix/v1/transaction/begin", "/lix/v1/transaction/commit"]
 */
export function docPaths(markdown) {
  const section = markdown.split(/^## Surface$/m)[1];
  if (section === undefined) {
    throw new Error(`${DOC} has no "## Surface" section`);
  }
  const table = section
    .split(/^## /m)[0]
    .split("\n")
    .filter((line) => line.startsWith("|"))
    .join("\n");

  const found = new Set();
  for (const [, token] of table.matchAll(/`(\/lix\/v1[^`]*)`/g)) {
    // Only comma-separated braces are documentation shorthand. OpenAPI path
    // parameters such as `{lix_id}` must remain literal.
    const group = token.match(/^(.*)\{([^}]*,[^}]*)\}(.*)$/);
    if (group) {
      for (const option of group[2].split(",")) {
        found.add(`${group[1]}${option.trim()}${group[3]}`);
      }
    } else {
      found.add(token);
    }
  }
  return [...found];
}

function main() {
  const root = process.cwd();
  const spec = specPaths(readFileSync(path.join(root, SPEC), "utf8"));
  const documented = docPaths(readFileSync(path.join(root, DOC), "utf8"));

  if (spec.length === 0) {
    throw new Error(`Parsed 0 paths from ${SPEC}; the parser needs updating.`);
  }

  const undocumented = spec.filter((p) => !documented.includes(p));
  const stale = documented.filter((p) => !spec.includes(p));

  if (undocumented.length > 0 || stale.length > 0) {
    const lines = [`${DOC} does not match ${SPEC}.`];
    if (undocumented.length > 0) {
      lines.push(`  Missing from the docs: ${undocumented.join(", ")}`);
    }
    if (stale.length > 0) {
      lines.push(`  Documented but not in the spec: ${stale.join(", ")}`);
    }
    throw new Error(lines.join("\n"));
  }

  console.log(`Validated ${spec.length} server protocol path(s).`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
