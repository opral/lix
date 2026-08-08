#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = rootIndex >= 0 ? args[rootIndex + 1] : process.cwd();

const files = {
  model: path.join(
    root,
    "packages/engine-benchmarks/tests/forktree_w5_r7_gc_reachability_oracle.rs",
  ),
  report: path.join(
    root,
    "packages/engine-benchmarks/tests/FORKTREE_W5_R7_GC_REACHABILITY_ORACLE.md",
  ),
  manifest: path.join(
    root,
    "packages/engine-benchmarks/tests/FORKTREE_W5_R7_GC_REACHABILITY_ORACLE_MANIFEST.txt",
  ),
};

const missing = [];
const read = (name) => {
  try {
    return fs.readFileSync(files[name], "utf8");
  } catch (error) {
    missing.push(`${name}: ${error.message}`);
    return "";
  }
};

const model = read("model");
const report = read("report");
const manifest = read("manifest");
const checks = [
  ["authenticated transitive closure", /fn authenticated_transitive_closure\s*\(/],
  [
    "GC authenticates the full closure",
    /fn commit_gc[\s\S]{0,1000}authenticated_transitive_closure\(\)/,
  ],
  [
    "queue processing authenticates the full closure",
    /fn process_page[\s\S]{0,1800}authenticated_transitive_closure\(\)/,
  ],
  [
    "owner/view scoped pin storage",
    /pinned_objects:\s*BTreeMap<String,\s*BTreeSet<\(u64,\s*u64\)>>/,
  ],
  ["owner-aware pin", /fn pin_read\([\s\S]{0,100}owner:\s*u64/],
  ["owner-aware unpin", /fn unpin_read\([\s\S]{0,140}owner:\s*u64/],
  ["cross-owner rejection", /foreign_collision[\s\S]{0,500}Error::OwnerMismatch/],
  ["cursor carries owner", /struct Cursor[\s\S]{0,180}owner:\s*u64/],
  ["checkpoint survives view close", /still-live checkpoint selector[\s\S]{0,300}RootPinned/],
  ["negative owner collision test", /reader_pins_are_owner_and_view_scoped/],
];

const reportChecks = [
  ["full transitive closure contract", /full authenticated transitive closure/i],
  ["post-close checkpoint contract", /still-live checkpoint selector/i],
  ["owner-scoped pin contract", /owner-scoped and view-scoped/i],
  ["cross-owner controls", /cross-owner[^\n]*collision/i],
  ["adapter order", /Memory[\s\S]{0,80}RocksDB[\s\S]{0,80}SlateDB/],
];

const manifestChecks = [
  ["successor parent", /direct_successor_of=409d14dbdc9e91b9cc6e2bd8c7bca4b487671113/],
  ["structural verifier", /structural_verifier=scripts\/forktree_w5_r7_gc_structure_verify\.mjs/],
  ["legacy verifier", /legacy_residue_verifier=scripts\/forktree_w5_r7_residue_verify\.mjs/],
];

for (const [label, pattern] of checks) {
  if (!pattern.test(model)) missing.push(`model: ${label}`);
}
for (const [label, pattern] of reportChecks) {
  if (!pattern.test(report)) missing.push(`report: ${label}`);
}
for (const [label, pattern] of manifestChecks) {
  if (!pattern.test(manifest)) missing.push(`manifest: ${label}`);
}

if (missing.length > 0) {
  console.log("RED W5/R7 structural contract");
  for (const item of missing) console.log(`- ${item}`);
  process.exit(1);
}

console.log("GREEN W5/R7 structural contract");
console.log("- authenticated transitive closure is required for GC/page mutation");
console.log("- checkpoint closure survives view close until selector retirement");
console.log("- reader pins and cursors are owner/view scoped with collision rejection");
