#!/usr/bin/env node
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = resolve(rootIndex >= 0 ? args[rootIndex + 1] : ".");
const productionRoots = [
  "packages/lix/src",
  "packages/lix_storage_rocksdb/src",
  "packages/lix_storage_slatedb/src",
];
const forbidden = [
  "CHECKPOINT_GC_STATE_NAMESPACE",
  "CHECKPOINT_GC_STATE_SPACE",
  "CHECKPOINT_RECOVERY_REF_NAMESPACE",
  "CHECKPOINT_RECOVERY_REF_SPACE",
  "GC_REACHABILITY_DELTA_NAMESPACE",
  "GC_REACHABILITY_DELTA_SPACE",
  "GC_REACHABILITY_QUEUE_NAMESPACE",
  "GC_REACHABILITY_QUEUE_SPACE",
  "GC_TREE_SWEEP_CURSOR_NAMESPACE",
  "GC_TREE_SWEEP_CURSOR_SPACE",
  "GC_TREE_SWEEP_EPOCH_NAMESPACE",
  "GC_TREE_SWEEP_EPOCH_SPACE",
  "GC_TREE_SWEEP_MARK_NAMESPACE",
  "GC_TREE_SWEEP_MARK_SPACE",
  "checkpoint.gc_state.v1",
  "checkpoint.recovery_ref.v3",
  "gc.reachability_delta.v1",
  "gc.reachability_queue.v1",
  "gc.tree_sweep_cursor.v1",
  "gc.tree_sweep_epoch.v1",
  "gc.tree_sweep_mark.v1",
  "GC_REACHABILITY_BATCH_LIMIT",
  "GC_TREE_SWEEP_PAGE_ROWS",
  "begin_tree_sweep_epoch",
  "open_tree_sweep_epoch",
  "stage_tree_sweep_epoch_page",
  "load_reachability_queue",
  "load_reachability_batches",
  "load_recovery_ref",
  "load_recovery_refs",
  "collect_checkpoint_garbage",
  "PreparedPublication::commit",
  "StorageSpace::mutable",
  "StorageSpace::new",
  "StorageSpaceId(",
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

const findings = [];
for (const directory of productionRoots) {
  for (const path of filesUnder(directory)) {
    const lines = readFileSync(path, "utf8").split(/\r?\n/);
    lines.forEach((line, lineIndex) => {
      for (const pattern of forbidden) {
        if (line.includes(pattern)) {
          findings.push({
            pattern,
            path: relative(root, path),
            line: lineIndex + 1,
          });
        }
      }
    });
  }
}
findings.sort((a, b) =>
  a.path.localeCompare(b.path) ||
  a.line - b.line ||
  a.pattern.localeCompare(b.pattern)
);
for (const finding of findings) {
  console.log(finding.path + ":" + finding.line + ":" + finding.pattern);
}
console.log(
  findings.length
    ? "RED " + findings.length + " forbidden production residues"
    : "GREEN no forbidden production residues",
);
process.exitCode = findings.length ? 1 : 0;

