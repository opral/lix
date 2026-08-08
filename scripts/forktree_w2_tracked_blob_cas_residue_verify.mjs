#!/usr/bin/env node
/* W2 test/report-only source and ancestry contract. */

import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const value = (name, fallback) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
};
const root = resolve(value("--root", "."));
const base = value("--base", "e92ea2e505ee3d96abbb529dbaedb23d4908ff42");
const target = value("--target", "HEAD");
const fixtureRoot = resolve(value("--fixtures", join(root, "scripts/forktree_w2_structural_fixtures")));

const production = "packages/lix/src";
const forbiddenFiles = [
  "packages/lix/src/tracked_state/codec.rs",
  "packages/lix/src/tracked_state/storage.rs",
  "packages/lix/src/tracked_state/tree.rs",
  "packages/lix/src/binary_cas/kv.rs",
  "packages/lix/src/binary_cas/codec.rs",
  "packages/lix/src/binary_cas/chunking.rs",
];
const forbidden = [
  "TrackedStateStoreReader",
  "TrackedStateScanRequest",
  "TrackedStateFilter",
  "TrackedStateReadColumns",
  "TrackedStateRootMutationRef",
  "TrackedHeadContext",
  "tracked_state::codec",
  "tracked_state::storage",
  "tracked_state::tree",
  "BinaryCasContext",
  "BinaryCasStoreReader",
  "BinaryCasManifest",
  "BinaryCasChunkView",
  "BINARY_CAS_",
  "binary_cas::kv",
  "binary_cas.manifest",
  "binary_cas.manifest_chunk",
  "binary_cas.chunk",
  "binary_cas.chunk_presence",
  "load_commit_state_manifest",
  "load_change_record_by_id",
  "scan_change_records_from_commit_deltas",
  "StorageSpace::mutable",
  "StorageSpaceId(",
  "scan_live_batches_for_controls",
  "load_projected_batch_at_commit",
  "load_retained_commit_snapshots_for_schemas",
];
const required = ["CoherentView", "ObjectId", "BlobId", "BlobRef"];
const allowedProductionPrefixes = [
  "packages/lix/src/forktree/",
  "packages/lix/src/storage/",
  "packages/lix/src/sql2/",
  "packages/lix/src/filesystem/",
  "packages/lix/src/live_state/forktree_reader.rs",
  "packages/lix/src/engine.rs",
  "packages/lix/src/tracked_state/",
  "packages/lix/src/binary_cas/",
];
const providerPaths = [
  "packages/lix/src/sql2/providers/working_diff.rs",
  "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
  "packages/lix/src/sql2/providers/checkpoint.rs",
];

function git(...parameters) {
  return execFileSync("git", ["-C", root, ...parameters], { encoding: "utf8" }).trim();
}

function filesUnder(directory) {
  const absolute = resolve(root, directory);
  if (!statSync(absolute, { throwIfNoEntry: false })) return [];
  const output = [];
  const visit = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (/\.(rs|ts|tsx|js|mjs|h|cc|cpp)$/.test(path)) output.push(path);
    }
  };
  visit(absolute);
  return output;
}

function mask(text) {
  return text
    .replace(/\/\*[\s\S]*?\*\//g, (value) => value.replace(/[^\n]/g, " "))
    .replace(/\/\/[^\n]*/g, (value) => value.replace(/[^\n]/g, " "))
    .replace(/"(?:\\.|[^"\\])*"/g, (value) => value.replace(/[^\n]/g, " "))
    .replace(/'(?:\\.|[^'\\])*'/g, (value) => value.replace(/[^\n]/g, " "));
}

function contains(text, token) {
  return mask(text).includes(token);
}

function syntheticContract(text) {
  const code = mask(text);
  const errors = [];
  if (!/forktree_reader/.test(code) || !/CoherentView/.test(code)) errors.push("missing retained view owner");
  const beginReads = [...code.matchAll(/\bbegin_read\s*\(/g)].length;
  if (beginReads !== 0) errors.push("provider acquires a new read");
  if (/ForkTreeReadFacade\s*::\s*new/.test(code)) errors.push("provider constructs a second facade");
  if (/\bRawStorage\b|\braw\s*\./.test(code)) errors.push("raw store access");
  const sameArgument = /read_point\s*\(\s*([A-Za-z_]\w*)\s*,\s*\1\s*\)/.test(code);
  if (!sameArgument) errors.push("call arguments do not preserve the retained reader");
  return errors;
}

function selfTest() {
  const positive = readFileSync(join(fixtureRoot, "positive.rs"), "utf8");
  const failures = [];
  if (syntheticContract(positive).length) failures.push("positive fixture rejected");
  for (const name of ["negative_second_view.rs", "negative_fresh_view.rs", "negative_raw_store.rs", "negative_mismatched_argument.rs"]) {
    const errors = syntheticContract(readFileSync(join(fixtureRoot, name), "utf8"));
    if (!errors.length) failures.push(`negative fixture accepted: ${name}`);
  }
  if (failures.length) {
    for (const failure of failures) console.log(`FIXTURE-RED ${failure}`);
    return 1;
  }
  console.log("FIXTURE GREEN positive accepted; four discriminating negatives rejected");
  return 0;
}

function sourceContract() {
  const errors = [];
  let baseCommit;
  let targetCommit;
  try {
    baseCommit = git("rev-parse", `${base}^{commit}`);
    targetCommit = git("rev-parse", `${target}^{commit}`);
    execFileSync("git", ["-C", root, "merge-base", "--is-ancestor", baseCommit, targetCommit]);
  } catch {
    errors.push("base/target are not resolvable or target is not descended from exact base");
    return errors;
  }
  const changed = git("diff", "--name-only", baseCommit, targetCommit, "--", "packages/lix/src").split("\n").filter(Boolean);
  for (const path of changed) {
    if (!allowedProductionPrefixes.some((prefix) => path.startsWith(prefix))) {
      errors.push(`production path outside W2 scope: ${path}`);
    }
  }
  const paths = filesUnder(production);
  const contents = new Map(paths.map((path) => [path, readFileSync(path, "utf8")]));
  for (const path of forbiddenFiles) {
    if (existsSync(resolve(root, path))) errors.push(`${path}: legacy owner file exists`);
  }
  for (const [path, text] of contents) {
    for (const token of forbidden) {
      if (contains(text, token)) errors.push(`${relative(root, path)}: forbidden owner/token ${token}`);
    }
  }
  for (const token of required) {
    if (![...contents.values()].some((text) => contains(text, token))) errors.push(`missing required symbol ${token}`);
  }
  for (const path of providerPaths) {
    const text = contents.get(resolve(root, path));
    if (!text) {
      errors.push(`${path}: provider missing`);
      continue;
    }
    const code = mask(text);
    if (!/forktree_reader/.test(code) || !/CoherentView/.test(code)) errors.push(`${path}: no operation-owned retained view argument`);
    if (/\bbegin_read\s*\(|ForkTreeReadFacade\s*::\s*new/.test(code)) errors.push(`${path}: fresh read/facade acquisition`);
    if (!/ObjectId/.test(code) || !/BlobId/.test(code) || !/BlobRef/.test(code)) errors.push(`${path}: missing typed object/blob identity arguments`);
  }
  console.log(`ANCESTRY ${baseCommit}..${targetCommit}`);
  console.log(`SCOPE changed_source=${changed.length ? changed.join(",") : "<none>"}`);
  return errors;
}

if (args.includes("--self-test")) process.exitCode = selfTest();
else {
  const fixtureStatus = selfTest();
  const errors = sourceContract();
  if (fixtureStatus !== 0 || errors.length) {
    for (const error of errors) console.log(`RED ${error}`);
    console.log(`RED W2 source contract findings=${errors.length}`);
    process.exitCode = 1;
  } else {
    console.log("GREEN W2 source contract");
    process.exitCode = 0;
  }
}
