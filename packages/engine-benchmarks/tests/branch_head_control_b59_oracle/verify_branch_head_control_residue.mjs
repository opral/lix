#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = rootIndex === -1 ? process.cwd() : path.resolve(args[rootIndex + 1]);
const sourceRoot = path.join(root, "packages", "lix", "src");

if (!fs.existsSync(sourceRoot)) {
  console.error(`missing source root: ${sourceRoot}`);
  process.exit(2);
}

function filesUnder(directory) {
  const result = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) =>
    a.name.localeCompare(b.name))) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) result.push(...filesUnder(file));
    else if (entry.isFile() && file.endsWith(".rs")) result.push(file);
  }
  return result;
}

function count(source, expression) {
  return [...source.matchAll(expression)].length;
}

const forbidden = [
  ["BranchHeadControl", /\bBranchHeadControl\b/g],
  ["BranchHeadControlContext", /\bBranchHeadControlContext\b/g],
  ["stage_branch_head_control", /\bstage_branch_head_control\b/g],
  ["branch_head_control_precondition", /\bbranch_head_control_precondition\b/g],
  ["BranchHeadControlCache", /\bBranchHeadControlCache\b/g],
  ["BranchHead", /\bBranchHead\b/g],
  ["BranchRefReader", /\bBranchRefReader\b/g],
  ["BRANCH_REF_SCHEMA_KEY", /\bBRANCH_REF_SCHEMA_KEY\b/g],
  ["branch_ref_stage_row", /\bbranch_ref_stage_row\b/g],
  ["branch_ref_tombstone_row", /\bbranch_ref_tombstone_row\b/g],
  ["tracked_generation", /\btracked_generation\b/g],
  ["untracked_generation", /\buntracked_generation\b/g],
  ["current_state_revision", /\bcurrent_state_revision\b/g],
  ["working_diff_checkpoint_commit_id", /\bworking_diff_checkpoint_commit_id\b/g],
  ["schema_presence_bloom", /\bschema_presence_bloom\b/g],
];

const required = [
  ["GlobalSelectorV1", /\bGlobalSelectorV1\b/g],
  ["BranchSelectorV1", /\bBranchSelectorV1\b/g],
  ["global_selector_key", /\bglobal_selector_key\b/g],
  ["branch_selector_key", /\bbranch_selector_key\b/g],
  ["PreparedPublication", /\bPreparedPublication\b/g],
];

const perFile = [];
for (const file of filesUnder(sourceRoot)) {
  const source = fs.readFileSync(file, "utf8");
  const relative = path.relative(root, file).split(path.sep).join("/");
  const forbiddenHits = Object.fromEntries(
    forbidden.map(([name, expression]) => [name, count(source, expression)]).filter(([, hits]) => hits),
  );
  if (Object.keys(forbiddenHits).length) perFile.push({ file: relative, forbidden: forbiddenHits });
}

const forbiddenTotals = Object.fromEntries(
  forbidden.map(([name, expression]) => [
    name,
    perFile.reduce((sum, entry) => sum + (entry.forbidden[name] ?? 0), 0),
  ]),
);
const requiredTotals = Object.fromEntries(
  required.map(([name, expression]) => {
    let total = 0;
    for (const file of filesUnder(sourceRoot)) total += count(fs.readFileSync(file, "utf8"), expression);
    return [name, total];
  }),
);

const report = {
  source_root: sourceRoot,
  forbidden_totals: forbiddenTotals,
  required_totals: requiredTotals,
  forbidden_files: perFile,
  status:
    Object.values(forbiddenTotals).every((hits) => hits === 0) &&
    Object.values(requiredTotals).every((hits) => hits > 0)
      ? "PASS"
      : "RED",
};
console.log(JSON.stringify(report, null, 2));
process.exit(report.status === "PASS" ? 0 : 1);
