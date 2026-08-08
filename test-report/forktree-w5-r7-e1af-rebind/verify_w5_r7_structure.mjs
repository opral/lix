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
const BASELINE_COMMIT = "e1af471b9ab0f598dafa7c2ddec7867667c81740";
const BASELINE_TREE = "bfa0d271a723da8250ab76ada16fda90926f1099";
const BASELINE_PARENT = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35";
const BASELINE_PARENT_TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b";

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
  const ancestryStatus = (() => {
    try {
      execFileSync("git", ["-C", repo, "merge-base", "--is-ancestor", anchor, target]);
      return [];
    } catch {
      return [`candidate ${target} is not descended from anchor ${anchor}`];
    }
  })();
  const anchorTree = runGit(["show", "-s", "--format=%T", anchor]).trim();
  const anchorParent = runGit(["show", "-s", "--format=%P", anchor]).trim();
  const parentTree = runGit(["show", "-s", "--format=%T", anchorParent]).trim();
  const identityErrors = [];
  if (anchor !== BASELINE_COMMIT || anchorTree !== BASELINE_TREE || anchorParent !== BASELINE_PARENT || parentTree !== BASELINE_PARENT_TREE) {
    identityErrors.push(`anchor identity mismatch: ${anchor} ${anchorTree} ${anchorParent} ${parentTree}`);
  }
  const changed = runGit(["diff", "--name-only", anchor, target]).split("\n").filter(Boolean);
  return [...ancestryStatus, ...identityErrors, ...changed
    .filter((name) => !name.startsWith(packagePrefix) && !allowedSource.some((pattern) => pattern.test(name)))
    .map((name) => `out-of-closure path: ${name}`)];
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
    if (!/\bread\s*:\s*&?\s*StorageRead\b/.test(extractFunctionHeader(files.find((entry) => entry.name === file)?.text ?? "", name))) {
      errors.push(`${file}:${name} does not accept a typed caller-owned StorageRead`);
    }
    const reads = countMatches(
      body,
      /(?:CoherentView::(?:open|new)|ForkTreeReadFacade::(?:open|new)|begin_coherent_read|begin_read)\s*\(/g,
    );
    if (reads !== 1) errors.push(`${file}:${name} has ${reads} coherent-read constructions, expected 1`);
    const readBinding = body.match(/\b(?:let|const)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*CoherentView::open\s*\(\s*&?read\s*\)/);
    if (!readBinding) {
      errors.push(`${file}:${name} does not retain the constructed coherent read`);
    }
    const viewName = readBinding?.[1] ?? "view";
    if (/\b(?:StorageRead::open|begin_read|begin_coherent_read|ForkTreeReadFacade::(?:open|new))\s*\(/.test(body)) {
      errors.push(`${file}:${name} reacquires a second read/facade`);
    }
    if (new RegExp(`\\b(?:let|const)\\s+\\w+\\s*=\\s*(?:read|${viewName})\\b`).test(body)) {
      errors.push(`${file}:${name} copies or aliases the retained read/view`);
    }
    if (new RegExp(`\\b${viewName}\\.clone\\s*\\(`).test(body) || /\bread\.clone\s*\(/.test(body)) {
      errors.push(`${file}:${name} clones the retained read/view`);
    }
    for (const identity of ["owner", "view_id", "snapshot"]) {
      if (!new RegExp(`\\b${viewName}\\.${identity}\\b`).test(body)) {
        errors.push(`${file}:${name} does not bind view identity ${identity}`);
      }
    }
    for (const label of ["selector", "queue", "mark", "upload", "object"]) {
      if (!new RegExp(`\\b${viewName}\\.${label}\\s*\\(`).test(body)) {
        errors.push(`${file}:${name} does not use the retained read for ${label}`);
      }
    }
    if (countMatches(body, new RegExp(`\\bPreparedPublication::new\\s*\\(\\s*&${viewName}\\s*\\)`, "g")) !== 1) {
      errors.push(`${file}:${name} does not construct publication from the exact retained view`);
    }
    if (countMatches(body, new RegExp(`\\.into_storage_plan\\s*\\(\\s*&?${viewName}\\s*\\)`, "g")) !== 1) {
      errors.push(`${file}:${name} does not pass the exact retained view into the plan`);
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

function extractFunctionHeader(text, functionName) {
  const match = text.match(new RegExp(`\\bfn\\s+${functionName}\\s*\\([^{}]*\\)`));
  return match?.[0] ?? "";
}

function closureWriterErrors(files, operations) {
  const errors = [];
  const operationKeys = new Set(operations.map(({ file, name }) => `${file}:${name}`));
  const rawWriter = /\b(?:begin_write|StorageWrite|StorageSpace::(?:mutable|open|new)|StorageSpaceId)\b|\b(?:storage|store|backend|cas_writer)\s*\.\s*(?:put|delete|write|commit)\s*\(/;
  const directCommit = /\.(?:commit|prepare_write_set)\s*\(/;
  for (const { name: file, text } of files) {
    if (rawWriter.test(text)) errors.push(`${file}:generic/raw writer token in closure`);
    for (const operation of extractFunctions(text)) {
      const key = `${file}:${operation.name}`;
      if (directCommit.test(operation.body) && !operationKeys.has(key)) {
        errors.push(`${file}:${operation.name}:writer/commit outside accepted operation`);
      }
      if (/\bPreparedPublication::commit\s*\(/.test(operation.body)) {
        errors.push(`${file}:${operation.name}:direct PreparedPublication commit`);
      }
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
  const operations = files.flatMap(({ name, text }) =>
    extractFunctions(text)
      .filter(({ name: functionName, body }) =>
        /(?:gc|publish|upload|root|selector|checkpoint|reachability)/i.test(functionName) &&
        /(?:CoherentView|PreparedPublication|\bcas\s*\()/.test(body),
      )
      .map((operation) => ({ ...operation, file: name })),
  );
  errors.push(...closureWriterErrors(files, operations));
if (errors.length > 0) {
  console.log(`RED structural authority gate: ${errors.length} findings`);
  for (const error of errors) console.log(`- ${error}`);
  process.exit(1);
}

console.log(`GREEN structural W5/R7 authority gate target=${target} anchor=${anchor}`);
