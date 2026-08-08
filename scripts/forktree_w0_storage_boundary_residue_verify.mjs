#!/usr/bin/env node

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const rootArg = args.indexOf("--root");
const root = resolve(rootArg >= 0 ? args[rootArg + 1] : process.cwd());
const sourceRoot = join(root, "packages", "lix", "src");
const sourceExtensions = new Set([".rs", ".ts", ".tsx", ".js", ".mjs", ".h", ".cc", ".cpp"]);

const requiredBoundary = [
  "StorageSpace",
  "ValueSemantics",
  "engine_declared",
  "OBJECT_SPACE",
  "SELECTOR_SPACE",
  "UNTRACKED_ROW_SPACE",
  "ObjectId",
  "ObjectDomain",
  "CoherentView",
];

const forbiddenFiles = [
  "packages/lix/src/columnar_row_group.rs",
  "packages/lix/src/live_state/entity_columnar.rs",
  "packages/lix/src/live_state/entity_columnar_cache.rs",
  "packages/lix/src/live_state/entity_decoded_column_cache.rs",
  "packages/lix/src/sql2/entity_batch.rs",
  "packages/lix/src/sql2/entity_columnar_layout.rs",
  "packages/lix/src/tracked_state/codec.rs",
  "packages/lix/src/tracked_state/storage.rs",
  "packages/lix/src/tracked_state/tree.rs",
  "packages/lix/src/binary_cas/kv.rs",
  "packages/lix/src/binary_cas/manifest.rs",
  "packages/lix/src/binary_cas/chunk.rs",
  "packages/lix/src/binary_cas/presence.rs",
];

const forbiddenSymbols = [
  ["raw StorageSpace mutable constructor", /StorageSpace::mutable\s*\(/],
  ["raw StorageSpace immutable constructor", /StorageSpace::immutable\s*\(/],
  ["raw StorageSpace constructor", /StorageSpace::new\s*\(/],
  ["raw StorageSpaceId constructor", /StorageSpaceId\s*\(/],
  ["raw SpaceId constructor", /(?:^|[^:])\bSpaceId\s*\(/],
  ["ColumnarRowGroup", /\bColumnarRowGroup\b/],
  ["RowGroupManifest", /\bRowGroupManifest\b/],
  ["RowGroupSetId", /\bRowGroupSetId\b/],
  ["EntityColumnar owner", /\bEntityColumnar\b/],
  ["ColumnarBaseCoordinate", /\bColumnarBaseCoordinate\b/],
  ["EntityColumnarScanLayout", /\bEntityColumnarScanLayout\b/],
  ["row-group physical space", /\b(?:ROW_GROUP_MANIFEST_SPACE|ROW_GROUP_COLUMN_SPACE)\b/],
  ["columnar row-group module", /columnar_row_group::/],
  ["tracked store reader", /\bTrackedStateStoreReader\b/],
  ["tracked scan/filter owner", /\b(?:TrackedStateScanRequest|TrackedStateFilter|TrackedStateReadColumns)\b/],
  ["tracked head context", /\bTrackedHeadContext\b/],
  ["legacy tracked module", /tracked_state::(?:codec|storage|tree)\b/],
  ["legacy changelog space", /\b(?:COMMIT_SPACE|CHANGE_SPACE|COMMIT_CHANGE_ID_SPACE)\b/],
  ["binary CAS kv owner", /binary_cas::kv\b/],
  ["binary CAS manifest owner", /\bBinaryCasManifest\b|binary_cas::manifest\b/],
  ["binary CAS chunk owner", /\bBinaryCasChunkView\b|binary_cas::(?:chunk|manifest_chunk)\b/],
  ["binary CAS presence owner", /binary_cas::chunk_presence\b|\bBINARY_CAS_(?:MANIFEST|CHUNK|PRESENCE)_SPACE\b/],
  ["legacy changelog loader", /\b(?:load_commit_state_manifest|load_change_record_by_id|scan_change_records_from_commit_deltas)\b/],
];

function walk(dir) {
  const files = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if ([".git", "node_modules", "target"].includes(entry.name)) continue;
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...walk(path));
    else if (sourceExtensions.has(path.slice(path.lastIndexOf(".")))) files.push(path);
  }
  return files;
}

const files = walk(sourceRoot);
const contents = new Map(files.map((file) => [file, readFileSync(file, "utf8")]));
const residues = [];

for (const file of forbiddenFiles) {
  const absolute = join(root, file);
  if (statSync(absolute, { throwIfNoEntry: false })) residues.push(`${file}:FILE:${"legacy physical owner exists"}`);
}

for (const [file, content] of contents) {
  const path = relative(root, file);
  content.split(/\n/).forEach((line, index) => {
    for (const [name, pattern] of forbiddenSymbols) {
      if (pattern.test(line)) residues.push(`${path}:${index + 1}:${name}:${line.trim()}`);
    }
  });
}

const missing = requiredBoundary.filter((token) => ![...contents.values()].some((content) => content.includes(token)));
residues.sort();
console.log(`W0 root=${root}`);
console.log(`W0 production source files=${files.length}`);
console.log(`W0 required boundary=${requiredBoundary.join(",")}`);
console.log(`W0 missing boundary tokens=${missing.length ? missing.join(",") : "none"}`);
console.log(`W0 forbidden residue count=${residues.length}`);
for (const residue of residues) console.log(residue);

if (missing.length || residues.length) {
  console.error(`RED W0 storage boundary: ${missing.length} missing boundary token(s), ${residues.length} forbidden residue(s)`);
  process.exitCode = 1;
} else {
  console.log("GREEN W0 storage boundary: retained descriptor/object-domain boundary is residue-free");
}
