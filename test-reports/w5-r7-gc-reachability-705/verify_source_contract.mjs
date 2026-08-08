#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const args = process.argv.slice(2);
const root = resolve(args[args.indexOf("--root") + 1] || ".");
const expectedHead = "705440f55eccba9e2d55c0951d6a684737005d76";
const expectedTree = "2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d";
const git = (...argv) => execFileSync("git", ["-C", root, ...argv], { encoding: "utf8" }).trim();
const head = git("rev-parse", "HEAD");
const tree = git("rev-parse", "HEAD^{tree}");
if (head !== expectedHead || tree !== expectedTree) {
  console.error(`TARGET_MISMATCH head=${head} tree=${tree}`);
  process.exit(2);
}
console.log(`TARGET head=${head} tree=${tree}`);

const roots = ["packages/lix/src", "packages/lix_storage_rocksdb/src", "packages/lix_storage_slatedb/src"];
const forbidden = [
  "CHECKPOINT_GC_STATE_NAMESPACE", "CHECKPOINT_GC_STATE_SPACE",
  "CHECKPOINT_RECOVERY_REF_NAMESPACE", "CHECKPOINT_RECOVERY_REF_SPACE",
  "GC_REACHABILITY_DELTA_NAMESPACE", "GC_REACHABILITY_DELTA_SPACE",
  "GC_REACHABILITY_QUEUE_NAMESPACE", "GC_REACHABILITY_QUEUE_SPACE",
  "GC_TREE_SWEEP_CURSOR_NAMESPACE", "GC_TREE_SWEEP_CURSOR_SPACE",
  "GC_TREE_SWEEP_EPOCH_NAMESPACE", "GC_TREE_SWEEP_EPOCH_SPACE",
  "GC_TREE_SWEEP_MARK_NAMESPACE", "GC_TREE_SWEEP_MARK_SPACE",
  "checkpoint.gc_state.v1", "checkpoint.recovery_ref.v3", "gc.reachability_delta.v1",
  "gc.reachability_queue.v1", "gc.tree_sweep_cursor.v1", "gc.tree_sweep_epoch.v1",
  "gc.tree_sweep_mark.v1", "GC_REACHABILITY_BATCH_LIMIT", "GC_TREE_SWEEP_PAGE_ROWS",
  "begin_tree_sweep_epoch", "open_tree_sweep_epoch", "stage_tree_sweep_epoch_page",
  "load_reachability_queue", "load_reachability_batches", "load_recovery_ref",
  "load_recovery_refs", "collect_checkpoint_garbage", "PreparedPublication::commit",
  "StorageSpace::mutable", "StorageSpace::new", "StorageSpaceId(",
];
const extensions = new Set([".rs", ".ts", ".tsx", ".js", ".mjs", ".h", ".cc", ".cpp"]);
function filesUnder(directory) {
  const absolute = resolve(root, directory);
  if (!statSync(absolute, { throwIfNoEntry: false })) return [];
  const files = [];
  const visit = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (extensions.has(path.slice(path.lastIndexOf(".")))) files.push(path);
    }
  };
  visit(absolute);
  return files;
}
const findings = [];
for (const directory of roots) for (const path of filesUnder(directory)) {
  readFileSync(path, "utf8").split(/\r?\n/).forEach((line, index) => {
    for (const pattern of forbidden) if (line.includes(pattern)) findings.push({ path: relative(root, path), line: index + 1, pattern });
  });
}
findings.sort((a, b) => a.path.localeCompare(b.path) || a.line - b.line || a.pattern.localeCompare(b.pattern));
for (const finding of findings) console.log(`${finding.path}:${finding.line}:${finding.pattern}`);
console.log(findings.length ? `RED ${findings.length} forbidden legacy GC/reachability residues` : "GREEN no forbidden GC/reachability residues");
process.exitCode = findings.length ? 1 : 0;
