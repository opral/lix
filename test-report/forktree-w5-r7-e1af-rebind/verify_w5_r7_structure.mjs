#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const usage = "usage: verify_w5_r7_structure.mjs <repo> <target> <anchor> [--fixture <file>]";
const args = process.argv.slice(2);
if (args.length < 3) {
  console.error(usage);
  process.exit(2);
}

const [repo, target, anchor] = args;
const fixtureIndex = args.indexOf("--fixture");
const fixture = fixtureIndex === -1 ? null : args[fixtureIndex + 1];
if (fixtureIndex !== -1 && !fixture) {
  console.error(usage);
  process.exit(2);
}

const allowedSource = [
  /^packages\/lix\/src\/forktree\//,
  /^packages\/lix\/src\/gc\.rs$/,
  /^packages\/lix\/src\/session\/(gc|checkpoint|media_upload)\.rs$/,
  /^packages\/lix\/src\/engine\.rs$/,
  /^packages\/lix\/src\/binary_cas\//,
  /^packages\/lix\/src\/transaction\/context\.rs$/,
];
const packagePrefix = "test-report/forktree-w5-r7-e1af-rebind/";

const legacyPattern = /CHECKPOINT_RECOVERY_REF_SPACE|CHECKPOINT_GC_STATE_SPACE|GC_REACHABILITY_(DELTA|QUEUE)|GC_TREE_SWEEP_|StorageSpace::mutable|StorageSpaceId|BranchRefReader|BranchHeadControl|CachingBranchRefReader|BranchRefFallback|SecondBranchAuthority|DualSelectorAuthority|LegacyGc|LegacyGC|legacy_gc|fallback_gc|retry_gc/;

function runGit(gitArgs, allowFailure = false) {
  try {
    return execFileSync("git", ["-C", repo, ...gitArgs], { encoding: "utf8" });
  } catch (error) {
    if (allowFailure) return error.stdout?.toString() ?? "";
    throw error;
  }
}

function sourceFilesFromGit() {
  const names = runGit(["ls-tree", "-r", "--name-only", target, "--", "packages/lix/src"])
    .split("\n")
    .filter(Boolean);
  return names.map((name) => ({ name, text: runGit(["show", `${target}:${name}`]) }));
}

function sourceFilesFromFixture() {
  const name = path.resolve(fixture);
  return [{ name, text: fs.readFileSync(name, "utf8") }];
}

function changedPathErrors() {
  if (fixture) return [];
  const changed = runGit(["diff", "--name-only", anchor, target]).split("\n").filter(Boolean);
  return changed
    .filter((name) => !name.startsWith(packagePrefix) && !allowedSource.some((pattern) => pattern.test(name)))
    .map((name) => `out-of-closure path: ${name}`);
}

function extractFunctions(text) {
  const functions = [];
  const header = /\bfn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\([^{}]*\)[^{]*\{/g;
  for (const match of text.matchAll(header)) {
    const bodyStart = match.index + match[0].length;
    let depth = 1;
    let index = bodyStart;
    while (index < text.length && depth > 0) {
      if (text[index] === "{") depth += 1;
      if (text[index] === "}") depth -= 1;
      index += 1;
    }
    if (depth === 0) {
      functions.push({ name: match[1], body: text.slice(bodyStart, index - 1) });
    }
  }
  return functions;
}

function countMatches(text, pattern) {
  return [...text.matchAll(pattern)].length;
}

function structuralErrors(files) {
  const all = files.map(({ name, text }) => `// ${name}\n${text}`).join("\n");
  const errors = [];

  if (!/\b(?:const|static)\s+OBJECT_SPACE\s*[:=]/.test(all)) {
    errors.push("typed OBJECT_SPACE declaration is missing");
  }
  if (!/\b(?:const|static)\s+SELECTOR_SPACE\s*[:=]/.test(all)) {
    errors.push("typed SELECTOR_SPACE declaration is missing");
  }
  if (!/\bCoherentView\b/.test(all)) errors.push("CoherentView owner is missing");
  if (!/\bPreparedPublication\b/.test(all)) errors.push("PreparedPublication owner is missing");

  const forbiddenAliases = [
    /\b(?:fallback|legacy|alternate|second|cached|cache)[A-Za-z0-9_]*\s*[:=][^;\n]*(?:read|view|selector|root|publication)/i,
    /\b(?:HashMap|BTreeMap|DashMap|OnceLock|LazyLock|Mutex|RwLock)\s*<[^>\n]*(?:Read|View|Selector|Root|Object)/,
    /\b(?:static|const)\s+[A-Za-z0-9_]*(?:READER|AUTHORITY|CACHE|FALLBACK)[A-Za-z0-9_]*\b/i,
  ];
  for (const pattern of forbiddenAliases) {
    if (pattern.test(all)) errors.push(`alias/second-authority pattern matched: ${pattern}`);
  }

  const operations = files.flatMap(({ name, text }) =>
    extractFunctions(text)
      .filter(({ name: functionName, body }) =>
        /(?:gc|publish|upload|root|selector|checkpoint|reachability)/i.test(functionName) &&
        /(?:CoherentView|PreparedPublication|\bcas\s*\()/.test(body),
      )
      .map((operation) => ({ ...operation, file: name })),
  );
  if (operations.length === 0) errors.push("no publication/GC operation body was structurally identified");

  for (const operation of operations) {
    const { body, file, name } = operation;
    const reads = countMatches(
      body,
      /(?:CoherentView::(?:open|new)|ForkTreeReadFacade::(?:open|new)|begin_coherent_read|begin_read)\s*\(/g,
    );
    if (reads !== 1) errors.push(`${file}:${name} has ${reads} coherent-read constructions, expected 1`);
    if (!/\b(?:let|const)\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*(?:CoherentView::(?:open|new)|ForkTreeReadFacade::(?:open|new)|begin_coherent_read|begin_read)\s*\(/.test(body)) {
      errors.push(`${file}:${name} does not retain the constructed coherent read`);
    }
    for (const label of ["selector", "queue", "mark", "upload", "object"]) {
      if (!new RegExp(`\\b(?:read|view)\\.${label}\\s*\\(`).test(body)) {
        errors.push(`${file}:${name} does not use the retained read for ${label}`);
      }
    }
    if (countMatches(body, /\binto_storage_plan\s*\(/g) !== 1) {
      errors.push(`${file}:${name} must call into_storage_plan exactly once`);
    }
    if (countMatches(body, /\bprepare_write_set\s*\(/g) !== 1) {
      errors.push(`${file}:${name} must prepare the transaction write set exactly once`);
    }
    if (countMatches(body, /\.commit\s*\(/g) !== 1 || /\bPreparedPublication::commit\s*\(/.test(body)) {
      errors.push(`${file}:${name} must use exactly one transaction commit and no direct publication commit`);
    }
    if (!/(?:\.cas|\bcas|compare_and_swap)\s*\(\s*owner\s*,\s*epoch\s*,\s*progress\s*,\s*selector\s*\)/.test(body)) {
      errors.push(`${file}:${name} lacks exact owner/epoch/progress/selector CAS arguments`);
    }
  }

  return errors;
}

function legacyCount(files) {
  return files.flatMap(({ name, text }) => text.split("\n").map((line, index) => ({ name, index, line })))
    .filter(({ line }) => legacyPattern.test(line)).length;
}

const files = fixture ? sourceFilesFromFixture() : sourceFilesFromGit();
const pathErrors = changedPathErrors();
if (pathErrors.length > 0) {
  console.log(`RED-SCOPE ${pathErrors.length} forbidden closure paths`);
  for (const error of pathErrors) console.log(error);
  process.exit(1);
}

const legacyCountValue = legacyCount(files);
if (legacyCountValue > 0) {
  console.log(`RED ${legacyCountValue} forbidden production residues`);
  process.exit(1);
}

const errors = structuralErrors(files);
if (errors.length > 0) {
  console.log(`RED structural authority gate: ${errors.length} findings`);
  for (const error of errors) console.log(`- ${error}`);
  process.exit(1);
}

console.log(`GREEN structural W5/R7 authority gate target=${target} anchor=${anchor}`);
