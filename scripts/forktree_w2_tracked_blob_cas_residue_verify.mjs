#!/usr/bin/env node
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = resolve(rootIndex >= 0 ? args[rootIndex + 1] : ".");
const production = "packages/lix/src";
const forbiddenFiles = [
  "packages/lix/src/tracked_state/codec.rs",
  "packages/lix/src/tracked_state/storage.rs",
  "packages/lix/src/tracked_state/tree.rs",
  "packages/lix/src/binary_cas/kv.rs",
  "packages/lix/src/binary_cas/codec.rs",
  "packages/lix/src/binary_cas/chunking.rs",
];
const required = ["CoherentView", "ObjectId", "BlobId", "BlobRef"];
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
const extensions = new Set([".rs", ".ts", ".tsx", ".js", ".mjs", ".h", ".cc", ".cpp"]);

function filesUnder(directory) {
  const absolute = resolve(root, directory);
  if (!statSync(absolute, { throwIfNoEntry: false })) return [];
  const output = [];
  const visit = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (extensions.has(path.slice(path.lastIndexOf(".")))) output.push(path);
    }
  };
  visit(absolute);
  return output;
}

const files = filesUnder(production);
const findings = [];
for (const relativePath of forbiddenFiles) {
  if (existsSync(resolve(root, relativePath))) {
    findings.push({ path: relativePath, line: 0, pattern: "legacy file exists" });
  }
}
const contents = new Map();
for (const path of files) contents.set(path, readFileSync(path, "utf8"));
for (const [path, text] of contents) {
  text.split(/\r?\n/).forEach((line, index) => {
    for (const pattern of forbidden) {
      if (line.includes(pattern)) findings.push({ path: relative(root, path), line: index + 1, pattern });
    }
  });
}
for (const pattern of required) {
  if (![...contents.values()].some(text => text.includes(pattern))) {
    findings.push({ path: production, line: 0, pattern: "MISSING required " + pattern });
  }
}
findings.sort((a, b) => a.path.localeCompare(b.path) || a.line - b.line || a.pattern.localeCompare(b.pattern));
for (const finding of findings) {
  console.log(finding.path + ":" + finding.line + ":" + finding.pattern);
}
console.log(findings.length ? "RED " + findings.length + " W2 residues/missing owners" : "GREEN W2 source contract");
process.exitCode = findings.length ? 1 : 0;

