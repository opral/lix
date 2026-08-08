#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { extname, join, resolve } from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = resolve(rootIndex >= 0 ? args[rootIndex + 1] : process.cwd());
const sourceExtensions = new Set([
  ".c",
  ".cc",
  ".cpp",
  ".h",
  ".js",
  ".mjs",
  ".rs",
  ".ts",
  ".tsx",
]);

const tracked = execFileSync("git", ["-C", root, "ls-files", "-z"], {
  encoding: "utf8",
})
  .split("\0")
  .filter(Boolean);

const sourceFiles = tracked.filter((file) => sourceExtensions.has(extname(file)));
const artifactPrefixes = [
  "packages/engine-benchmarks/tests/forktree_w0_",
  "scripts/forktree_w0_",
];
const isArtifact = (file) => artifactPrefixes.some((prefix) => file.startsWith(prefix));

const genericStorageOwners = [
  "packages/rocksdb-storage/",
  "packages/slatedb-storage/",
  "packages/lix/src/storage/conformance/",
  "packages/lix/src/storage/in_memory.rs",
];
const isGenericStorageOwner = (file) =>
  genericStorageOwners.some((prefix) => file.startsWith(prefix));

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
  "packages/lix/src/binary_cas/context.rs",
  "packages/lix/src/binary_cas/kv.rs",
  "packages/lix/src/binary_cas/manifest.rs",
  "packages/lix/src/binary_cas/chunk.rs",
  "packages/lix/src/binary_cas/presence.rs",
];

const forbiddenSymbols = [
  ["raw StorageSpace mutable constructor", /StorageSpace::mutable\s*\(/],
  ["raw StorageSpace immutable constructor", /StorageSpace::immutable\s*\(/],
  ["raw StorageSpace constructor", /StorageSpace::new\s*\(/],
  ["raw SpaceId constructor", /(?:^|[^:])\bSpaceId\s*\(/],
  ["ColumnarRowGroup", /\bColumnarRowGroup\b/],
  ["RowGroupManifest", /\bRowGroupManifest\b/],
  ["RowGroupSetId", /\bRowGroupSetId\b/],
  ["EntityColumnar owner", /\bEntityColumnar\b/],
  ["ColumnarBaseCoordinate", /\bColumnarBaseCoordinate\b/],
  ["EntityColumnarScanLayout", /\bEntityColumnarScanLayout\b/],
  [
    "row-group physical space",
    /\b(?:ROW_GROUP_MANIFEST_SPACE|ROW_GROUP_COLUMN_SPACE)\b/,
  ],
  ["tracked store reader", /\bTrackedStateStoreReader\b/],
  [
    "tracked scan/filter owner",
    /\b(?:TrackedStateScanRequest|TrackedStateFilter|TrackedStateReadColumns)\b/,
  ],
  ["tracked head context", /\bTrackedHeadContext\b/],
  ["legacy tracked module", /tracked_state::(?:codec|storage|tree)\b/],
  [
    "legacy changelog space",
    /\b(?:COMMIT_SPACE|CHANGE_SPACE|COMMIT_CHANGE_ID_SPACE)\b/,
  ],
  ["legacy changelog reader", /\b(?:Changelog|ChangeLog|load_change_record)\b/],
  ["binary CAS module owner", /\bbinary_cas::/],
  ["BinaryCas owner", /\b(?:BinaryCasContext|BinaryCasSpace|BinaryCasManifest|BinaryCasChunk)\b/],
  ["binary CAS blob reader", /\bBlobDataReader\b/],
  ["legacy native filesystem export", /\b(?:openLocalFilesystem|importFilesystemPaths|syncDiskToLix)\b/],
  ["legacy LocalFilesystem option", /\b(?:lixDir|syncAllFiles)\b/],
];

const requiredBoundary = [
  "OBJECT_SPACE",
  "SELECTOR_SPACE",
  "UNTRACKED_ROW_SPACE",
  "StorageSpace",
  "ValueSemantics",
  "ObjectId",
  "ObjectDomain",
  "CoherentView",
];

const read = (relativePath) => {
  const path = join(root, relativePath);
  return existsSync(path) ? readFileSync(path, "utf8") : "";
};

const residues = [];
const allowlisted = [];

for (const file of forbiddenFiles) {
  if (existsSync(join(root, file))) residues.push(`${file}:FILE:legacy physical owner exists`);
}

for (const file of sourceFiles) {
  if (isArtifact(file)) continue;
  const content = read(file);
  content.split("\n").forEach((line, index) => {
    for (const [name, pattern] of forbiddenSymbols) {
      if (!pattern.test(line)) continue;
      const finding = `${file}:${index + 1}:${name}:${line.trim()}`;
      if (isGenericStorageOwner(file) && name === "raw StorageSpace mutable constructor") {
        allowlisted.push(`${finding}:generic adapter test/implementation owner`);
      } else if (isGenericStorageOwner(file) && name === "raw StorageSpace immutable constructor") {
        allowlisted.push(`${finding}:generic adapter test/implementation owner`);
      } else {
        residues.push(finding);
      }
    }
  });
}

const boundaryText = sourceFiles.filter((file) => file.startsWith("packages/lix/src/")).map(read);
const missing = requiredBoundary.filter(
  (token) => !boundaryText.some((content) => content.includes(token)),
);

const structural = [];
const requirePattern = (label, file, pattern) => {
  if (!pattern.test(read(file))) structural.push(`${file}:${label}`);
};
const forbidPattern = (label, file, pattern) => {
  if (pattern.test(read(file))) structural.push(`${file}:${label}`);
};

requirePattern("private SpaceId tuple field", "packages/lix/src/storage/types.rs", /pub struct SpaceId\(u32\)/);
requirePattern("StorageSpace private struct fields", "packages/lix/src/storage/types.rs", /pub struct StorageSpace\s*\{/);
requirePattern("StorageSpace engine brand", "packages/lix/src/storage/types.rs", /_brand:\s*private::EngineDeclared/);
requirePattern("engine-only constructor", "packages/lix/src/storage/types.rs", /pub\(crate\) const fn engine_declared/);
const storageTypeImpl = read("packages/lix/src/storage/types.rs").match(
  /impl StorageSpace\s*\{[\s\S]*?\n\}/,
)?.[0] ?? "";
if (/pub\s+(?:const\s+)?fn\s+(?:new|mutable|immutable)\s*\(/.test(storageTypeImpl)) {
  structural.push("packages/lix/src/storage/types.rs:public forge constructor");
}
requirePattern("private ObjectId tuple field", "packages/lix/src/forktree/object.rs", /pub\(crate\) struct ObjectId\(\[u8; 32\]\)/);
requirePattern("non-public ObjectDomain", "packages/lix/src/forktree/object.rs", /pub\(super\) enum ObjectDomain/);
requirePattern("authenticated object domain", "packages/lix/src/forktree/object.rs", /pub\(super\) fn authenticate_object_domain/);
requirePattern("non-public CoherentView", "packages/lix/src/forktree/view.rs", /pub\(crate\) struct CoherentView<R>/);
requirePattern("private CoherentView read field", "packages/lix/src/forktree/view.rs", /\n\s+read: R,/);
forbidPattern("public CoherentView", "packages/lix/src/forktree/view.rs", /pub struct CoherentView/);
forbidPattern("public CoherentView read getter", "packages/lix/src/forktree/view.rs", /pub fn read\s*\(/);

console.log(`W0 root=${root}`);
console.log(`W0 tracked source files=${sourceFiles.length}`);
console.log(`W0 scanned source files=${sourceFiles.filter((file) => !isArtifact(file)).length}`);
console.log(`W0 required retained boundary=${requiredBoundary.join(",")}`);
console.log(`W0 missing retained boundary=${missing.length ? missing.join(",") : "none"}`);
console.log(`W0 structural sealing findings=${structural.length}`);
for (const finding of structural.sort()) console.log(`STRUCTURAL:${finding}`);
console.log(`W0 forbidden residue count=${residues.length}`);
for (const residue of residues.sort()) console.log(`RESIDUE:${residue}`);
console.log(`W0 explicit generic-storage allowlist count=${allowlisted.length}`);
for (const item of allowlisted.sort()) console.log(`ALLOWLIST:${item}`);

if (missing.length || structural.length || residues.length) {
  console.error(
    `RED W0 storage boundary: ${missing.length} missing boundary item(s), ${structural.length} structural finding(s), ${residues.length} forbidden residue(s)`,
  );
  process.exitCode = 1;
} else {
  console.log("GREEN W0 storage boundary: retained boundary is structurally sealed and residue-free");
}
