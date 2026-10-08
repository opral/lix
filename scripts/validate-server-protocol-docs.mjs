#!/usr/bin/env node
// Keeps the documented endpoint table and protocol versions in sync with
// the canonical OpenAPI document and Rust-owned compatibility constants.
// Other prose remains hand-written and is not validated here.
import { readFileSync } from "node:fs";
import path from "node:path";
import { getCompatibility } from "./compatibility.mjs";

const SPEC = "packages/lix/server-protocol.openapi.yaml";
const DOC = "docs/server-protocol.md";
const PUBLIC_LOCATOR_DOCS = [
  "docs/hosting.md",
  "docs/js-api-reference.md",
  "docs/persistence.md",
  "docs/snapshots.md",
  "docs/what-is-lix.md",
  "packages/js-sdk/README.md",
];

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

/** Reads one named OpenAPI header parameter and its numeric schema const. */
export function openApiHeaderParameter(yaml, parameterName) {
  const lines = yaml.split(/\r?\n/);
  const start = lines.findIndex((line) => line === `    ${parameterName}:`);
  if (start < 0) {
    throw new Error(`${SPEC} has no ${parameterName} parameter`);
  }
  let end = start + 1;
  while (end < lines.length && !/^    [A-Z][A-Za-z0-9_]*:$/.test(lines[end])) {
    end += 1;
  }
  const block = lines.slice(start + 1, end);
  const value = (key) => block.find((line) => line.startsWith(`      ${key}:`))?.slice(`      ${key}:`.length).trim();
  const headerName = value("name");
  const location = value("in");
  const required = value("required");
  const schema = block.find((line) => /^      schema:/.test(line));
  const type = schema?.match(/\btype:\s*([A-Za-z]+)/)?.[1];
  const match = schema?.match(/\bconst:\s*(\d+)\s*\}/);
  if (!headerName || !location || !required || type !== "integer" || !match) {
    throw new Error(`${SPEC} ${parameterName} must declare a required header with an integer const`);
  }
  return { headerName, location, required: required === "true", constant: Number(match[1]) };
}

function assertDocumentedHeaderVersion(markdown, file, headerName, expected) {
  const escaped = headerName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const matches = [...markdown.matchAll(new RegExp(`\\b${escaped}:\\s*(\\d+)`, "g"))];
  if (matches.length === 0) {
    throw new Error(`${file} does not document ${headerName}`);
  }
  const observed = matches.map((match) => Number(match[1]));
  if (observed.some((version) => version !== expected)) {
    throw new Error(`${file} documents ${headerName} version(s) ${observed.join(", ")}; canonical version is ${expected}`);
  }
}

function assertDocumentedNumericField(markdown, file, fieldName, expected) {
  const escaped = fieldName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const matches = [...markdown.matchAll(new RegExp(`\\b${escaped}\\s*:\\s*(\\d+)`, "g"))];
  if (matches.length === 0) {
    throw new Error(`${file} does not document ${fieldName}`);
  }
  const observed = matches.map((match) => Number(match[1]));
  if (observed.some((version) => version !== expected)) {
    throw new Error(`${file} documents ${fieldName} value(s) ${observed.join(", ")}; canonical version is ${expected}`);
  }
}

/** Check wire header constants and their public prose against Rust-owned versions. */
export function validateProtocolVersionContract(openApi, serverProtocolDoc, admissionDoc, compatibility) {
  const checks = [
    ["ServerProtocolVersion", "lix-server-protocol-version", compatibility.serverProtocolVersion],
    ["SyncProtocolVersion", "lix-sync-protocol-version", compatibility.syncProtocolVersion],
  ];
  for (const [parameterName, expectedHeader, expectedVersion] of checks) {
    const observed = openApiHeaderParameter(openApi, parameterName);
    if (observed.headerName !== expectedHeader || observed.location !== "header" || !observed.required) {
      throw new Error(`${SPEC} ${parameterName} must be the required ${expectedHeader} header`);
    }
    if (observed.constant !== expectedVersion) {
      throw new Error(`${SPEC} ${parameterName} const is ${observed.constant}; canonical version is ${expectedVersion}`);
    }
  }
  assertDocumentedHeaderVersion(
    serverProtocolDoc, "docs/server-protocol.md", "lix-server-protocol-version", compatibility.serverProtocolVersion,
  );
  assertDocumentedHeaderVersion(
    serverProtocolDoc, "docs/server-protocol.md", "lix-sync-protocol-version", compatibility.syncProtocolVersion,
  );
  assertDocumentedHeaderVersion(
    admissionDoc, "docs/architecture/http-admission.md", "lix-sync-protocol-version", compatibility.syncProtocolVersion,
  );
  assertDocumentedNumericField(
    admissionDoc, "docs/architecture/http-admission.md", "protocolEpoch", compatibility.syncProtocolVersion,
  );
  return true;
}

function main() {
  const root = process.cwd();
  const openApi = readFileSync(path.join(root, SPEC), "utf8");
  const serverProtocolDoc = readFileSync(path.join(root, DOC), "utf8");
  const admissionDoc = readFileSync(path.join(root, "docs/architecture/http-admission.md"), "utf8");
  const spec = specPaths(openApi);
  const documented = docPaths(serverProtocolDoc);
  validateProtocolVersionContract(openApi, serverProtocolDoc, admissionDoc, getCompatibility());

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

  for (const file of PUBLIC_LOCATOR_DOCS) {
    const markdown = readFileSync(path.join(root, file), "utf8");
    if (/\/repositories\/|\/@[^\s/]+\/[^\s/]+(?:\/lix\/v1)?/.test(markdown)) {
      throw new Error(
        `${file} contains a legacy server locator; use https://host/lix/{uuid}`,
      );
    }
  }

  console.log(`Validated ${spec.length} server protocol path(s) and current header versions.`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
