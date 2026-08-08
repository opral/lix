#!/usr/bin/env node

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = resolve(rootIndex < 0 ? process.cwd() : args[rootIndex + 1]);
const sourceRoot = join(root, "packages", "lix", "src");
const extensions = new Set([".rs", ".ts", ".tsx", ".js", ".mjs", ".h", ".cc", ".cpp"]);

const required = [
  "scan_direct_entity_snapshots",
  "scan_direct_entity_primary_keys",
  "direct_entity_snapshot_scope",
  "scan_forktree_operation",
  "LiveStateProjection",
  "MaterializedLiveStateBatch",
  "EntityPk",
];

const forbiddenFiles = [
  "packages/lix/src/live_state/entity_columnar.rs",
  "packages/lix/src/live_state/entity_columnar_cache.rs",
  "packages/lix/src/live_state/entity_decoded_column_cache.rs",
  "packages/lix/src/sql2/entity_columnar_layout.rs",
  "packages/lix/src/columnar_row_group.rs",
];

const forbidden = [
  ["direct row-group planner", /\bplan_direct_entity_columnar_scan\b/],
  ["entity columnar layout cache", /\b(?:EntityColumnarLayoutCache|entity_columnar_layout)\b/],
  ["row-group manifest", /\b(?:RowGroupManifest|RowGroupSetId|ColumnarRowGroup)\b/],
  ["columnar overlay owner", /\b(?:EntityColumnarOverlayRow|EntityColumnarWriteSets|EntityColumnarArrayBudget)\b/],
  ["decoded column cache", /\bentity_decoded_column_cache\b/],
  ["columnar module import", /(?:columnar_row_group::|sql2::entity_columnar_layout|live_state::entity_columnar)\b/],
  ["obsolete hot helper", /\b(?:ordered_unique_branch_row_index|finalize_ordered_unique_batch|concat_live_state_batches|filter_current_row_retention)\b/],
];

function walk(directory) {
  const result = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if ([".git", "target", "node_modules"].includes(entry.name)) continue;
    const path = join(directory, entry.name);
    if (entry.isDirectory()) result.push(...walk(path));
    else if (extensions.has(path.slice(path.lastIndexOf(".")))) result.push(path);
  }
  return result;
}

const files = walk(sourceRoot);
const contents = new Map(files.map((file) => [file, readFileSync(file, "utf8")]));
const residues = [];
for (const file of forbiddenFiles) {
  const path = join(root, file);
  if (statSync(path, { throwIfNoEntry: false })) residues.push(`${file}:FILE:obsolete owner exists`);
}
for (const [file, content] of contents) {
  const path = relative(root, file);
  content.split(/\n/).forEach((line, index) => {
    for (const [name, pattern] of forbidden) {
      if (pattern.test(line)) residues.push(`${path}:${index + 1}:${name}:${line.trim()}`);
    }
  });
}
const missing = required.filter((token) => ![...contents.values()].some((content) => content.includes(token)));
residues.sort();
console.log(`SQL W0 root=${root}`);
console.log(`SQL W0 production source files=${files.length}`);
console.log(`SQL W0 missing direct-boundary tokens=${missing.length ? missing.join(",") : "none"}`);
console.log(`SQL W0 obsolete-columnar residue count=${residues.length}`);
for (const residue of residues) console.log(residue);
if (missing.length || residues.length) {
  console.error(`RED public SQL boundary: ${missing.length} missing token(s), ${residues.length} obsolete residue(s)`);
  process.exitCode = 1;
} else {
  console.log("GREEN public SQL boundary: direct entity execution is residue-free");
}
