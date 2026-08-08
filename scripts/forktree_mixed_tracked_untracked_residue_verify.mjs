#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = resolve(rootIndex < 0 ? process.cwd() : args[rootIndex + 1]);

const files = [
  "packages/lix/src/live_state/context.rs",
  "packages/lix/src/sql2/entity_batch.rs",
  "packages/lix/src/sql2/providers/entity.rs",
];
const required = ["LiveStateReader", "scan_batch", "MaterializedLiveStateBatch"];
const forbiddenByFile = new Map([
  [
    "packages/lix/src/live_state/context.rs",
    [
      ["direct entity row scan", /\bscan_entity_rows\s*\(/],
      ["direct entity snapshot path", /\bscan_direct_entity_snapshots\b/],
      ["direct entity primary-key path", /\bscan_direct_entity_primary_keys\b/],
      ["direct entity row helper", /\bscan_direct_entity_rows\b/],
    ],
  ],
  [
    "packages/lix/src/sql2/entity_batch.rs",
    [
      ["raw entity snapshot reader", /\b(?:EntitySnapshotReader|CurrentEntitySnapshotReader)\b/],
      ["raw adapter ownership", /\bStorageAdapterRead\b/],
      ["direct entity snapshot path", /\bscan_direct_entity_/],
    ],
  ],
  [
    "packages/lix/src/sql2/providers/entity.rs",
    [["second entity reader", /\b(?:EntitySnapshotReader|entity_snapshot_reader)\b/]],
  ],
]);

const contents = files.map((file) => [file, readFileSync(resolve(root, file), "utf8")]);
const all = contents.map(([, content]) => content).join("\n");
const missing = required.filter((token) => !all.includes(token));
const residues = [];
for (const [file, content] of contents) {
  const forbidden = forbiddenByFile.get(file) ?? [];
  content.split("\n").forEach((line, index) => {
    for (const [name, pattern] of forbidden) {
      if (pattern.test(line)) residues.push(`${file}:${index + 1}:${name}:${line.trim()}`);
    }
  });
}

console.log(`mixed-domain SQL root=${root}`);
console.log(`required canonical tokens missing=${missing.length ? missing.join(",") : "none"}`);
console.log(`forbidden alternate-read residues=${residues.length}`);
for (const residue of residues) console.log(residue);
if (missing.length || residues.length) {
  console.error("RED mixed-domain boundary: direct/alternate read ownership remains");
  process.exitCode = 1;
} else {
  console.log("GREEN mixed-domain boundary: one canonical LiveStateReader owner");
}
