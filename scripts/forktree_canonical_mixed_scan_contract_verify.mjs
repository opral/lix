#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
const root = resolve(rootIndex < 0 ? process.cwd() : args[rootIndex + 1]);

function read(relative) {
  return readFileSync(resolve(root, relative), "utf8");
}

function functionBody(source, signature) {
  const start = source.indexOf(signature);
  if (start < 0) return "";
  const bodyStart = source.indexOf("{", start);
  if (bodyStart < 0) return "";
  let depth = 0;
  for (let index = bodyStart; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(bodyStart, index + 1);
    }
  }
  return "";
}

function countMatches(source, pattern) {
  return [...source.matchAll(pattern)].length;
}

const readerPath = "packages/lix/src/live_state/forktree_reader.rs";
const contextPath = "packages/lix/src/live_state/context.rs";
const domainContractPath = "packages/lix/src/live_state/reader.rs";
const entityPath = "packages/lix/src/sql2/entity_batch.rs";
const providerPath = "packages/lix/src/sql2/providers/entity.rs";
const reader = read(readerPath);
const context = read(contextPath);
const domainContract = read(domainContractPath);
const entity = read(entityPath);
const provider = read(providerPath);
const scanView = functionBody(reader, "pub(crate) async fn scan_view");
const operation = functionBody(context, "async fn scan_forktree_operation");
const snapshotProjection = functionBody(entity, "async fn canonical_snapshot_projection");
const primaryKeyProjection = functionBody(entity, "async fn canonical_primary_key_projection");

const checks = [];
function check(name, pass, detail) {
  checks.push({ name, pass, detail });
}

// Ordinary SQL uses the default Option<bool> value, None. That mode must be
// the complete logical overlay, not an alias for tracked-only. Explicit
// Some(false)/Some(true) are narrower modes only when the public reader
// contract exposes them; this verifier records both independently.
const hasCombinedSelector =
  /untracked\s*==\s*None/.test(scanView) ||
  /untracked\s*\.is_none\s*\(\)/.test(scanView) ||
  /LiveStateReadDomain::Combined/.test(reader + context);
const combinesTrackedAndUntracked =
  /state_range\s*\(/.test(scanView) && /scan_untracked_rows\s*\(\)/.test(scanView) &&
  /(overlay|precedence|identity|merge|dedup|duplicate)/i.test(scanView);
check(
  "untracked=None complete overlay",
  hasCombinedSelector && combinesTrackedAndUntracked,
  `selector=${hasCombinedSelector ? "present" : "missing"}; combined tracked+untracked resolver=${combinesTrackedAndUntracked ? "present" : "missing"}`,
);

const hasTrackedOnlyMode =
  /LiveStateReadDomain::Tracked/.test(domainContract) &&
  !/untracked\s*==\s*Some\(false\)[\s\S]{0,240}unsupported/.test(scanView);
const hasUntrackedOnlyMode =
  /untracked\s*==\s*Some\(true\)/.test(scanView) &&
  /scan_untracked_view/.test(scanView);
check(
  "explicit tracked-only mode",
  hasTrackedOnlyMode,
  hasTrackedOnlyMode ? "Some(false) is represented by the reader contract" : "Some(false) mode is not represented on the canonical view",
);
check(
  "explicit untracked-only mode",
  hasUntrackedOnlyMode,
  hasUntrackedOnlyMode ? "Some(true) routes to the authenticated untracked reader" : "Some(true) mode is missing",
);

const overlaySemantics =
  /(untracked|tracked)/i.test(scanView) &&
  /(deleted|tombstone)/i.test(scanView) &&
  /(identity|precedence|overlay|dedup|duplicate)/i.test(scanView);
check(
  "untracked replacement/tombstone precedence",
  combinesTrackedAndUntracked && overlaySemantics,
  "same canonical batch must resolve branch/global and tracked/untracked identity winners before projection",
);

const orderingAndProjection =
  /into_identity_ordered_snapshots\s*\(\)/.test(entity) &&
  /into_identity_ordered_primary_keys\s*\(\)/.test(entity) &&
  /limit/.test(reader) &&
  /entity_pks/.test(reader);
check(
  "typed identity order/projection/LIMIT",
  orderingAndProjection,
  orderingAndProjection ? "terminal projections and identity filters remain present" : "ordering/projection/LIMIT evidence is incomplete",
);

const corruptionAndDuplicates =
  /decode_state_key\s*\(/.test(scanView) &&
  /decode_untracked_key|decode_untracked_value/.test(reader) &&
  /(duplicate|conflict|ambiguous|corrupt)/i.test(scanView + reader);
check(
  "duplicate/corruption fail-closed",
  corruptionAndDuplicates,
  "both typed domains must decode under one view and reject duplicate/conflicting identity authority",
);

const oneView =
  countMatches(operation, /ForkTreeReadFacade::new\s*\(/g) === 1 &&
  countMatches(operation, /\.branch\s*\(/g) === 1 &&
  /CoherentView/.test(reader);
check(
  "one CoherentView/StorageRead acquisition",
  oneView,
  `operation facade.new=${countMatches(operation, /ForkTreeReadFacade::new\s*\(/g)}, branch=${countMatches(operation, /\.branch\s*\(/g)}, coherent-view=${/CoherentView/.test(reader) ? "present" : "missing"}`,
);

const noRawProjectionRead =
  !/ForkTreeReadFacade/.test(entity) &&
  !/\b(?:begin_read|begin_scan|get_many|state_range|load_object_bytes)\s*\(/.test(entity) &&
  countMatches(snapshotProjection, /\.scan_batch\s*\(/g) === 1 &&
  countMatches(primaryKeyProjection, /\.scan_batch\s*\(/g) === 1;
check(
  "no second projection read/fallback/cache",
  noRawProjectionRead,
  `snapshot scans=${countMatches(snapshotProjection, /\.scan_batch\s*\(/g)}, primary-key scans=${countMatches(primaryKeyProjection, /\.scan_batch\s*\(/g)}`,
);

const providerUsesCanonicalRequest =
  /LiveStateFilter::default\s*\(\)/.test(provider) &&
  !/scan_direct_entity_|ForkTreeReadFacade::new/.test(provider);
check(
  "ordinary SQL binds canonical default request",
  providerUsesCanonicalRequest,
  providerUsesCanonicalRequest ? "provider retains the ordinary default filter" : "provider has a direct/alternate entity reader",
);

console.log(`canonical mixed-scan contract root=${root}`);
for (const result of checks) {
  console.log(`${result.pass ? "PASS" : "FAIL"} ${result.name}: ${result.detail}`);
}
const failures = checks.filter((result) => !result.pass);
console.log(`checks=${checks.length} failures=${failures.length}`);
if (failures.length) {
  console.error("RED canonical mixed-scan contract: ordinary untracked=None is not proven as one complete overlay");
  process.exitCode = 1;
} else {
  console.log("GREEN canonical mixed-scan contract: one coherent complete overlay and explicit domain modes");
}
