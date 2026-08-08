#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(SCRIPT_DIR, "..");
const DATA = join(
  ROOT,
  "packages/engine-benchmarks/tests/forktree_stage2_olap_acceptance",
);
const SELECTED = ["pk_point", "pk_range", "column_projection", "group_by", "simple_join"];
const SPI = "AcceptancePhysicalLayout/v1";
const OWNER = "forktree-stage2-production";
const CELL_TIMEOUT_MS = 20 * 60 * 1000;

function parseCsv(path) {
  const [header, ...lines] = readFileSync(path, "utf8").trim().split(/\r?\n/);
  const keys = header.split(",");
  return lines.map((line) =>
    Object.fromEntries(line.split(",").map((value, index) => [keys[index], value])),
  );
}

function parseKeyValueRows(path) {
  return readFileSync(path, "utf8")
    .trim()
    .split(/\r?\n/)
    .map((line) => Object.fromEntries(line.split(",").map((part) => part.split("=", 2))));
}

export function loadBaseline() {
  return {
    queries: parseCsv(join(DATA, "comparator_query_medians.csv")),
    runs: parseCsv(join(DATA, "comparator_runs.csv")),
    digests: parseKeyValueRows(join(DATA, "result_digests.csv")),
  };
}

function number(row, field) {
  const value = Number(row[field]);
  if (!Number.isFinite(value)) throw new Error(`missing numeric ${field}`);
  return value;
}

function baselineQuery(baseline, rows, engine, backend, query) {
  const found = baseline.queries.find(
    (row) =>
      Number(row.rows) === rows &&
      row.engine === engine &&
      row.backend === backend &&
      row.query === query,
  );
  if (!found) throw new Error(`missing baseline ${rows}/${engine}/${backend}/${query}`);
  return found;
}

function baselineRun(baseline, rows, backend) {
  const found = baseline.runs.find(
    (row) => Number(row.rows) === rows && row.engine === "lix" && row.backend === backend,
  );
  if (!found) throw new Error(`missing run baseline ${rows}/${backend}`);
  return found;
}

function expectedDigest(baseline, rows, query) {
  const found = baseline.digests.find(
    (row) => Number(row.rows) === rows && row.query === query,
  );
  if (!found) throw new Error(`missing digest ${rows}/${query}`);
  return found;
}

function atMost(observed, baseline, fraction, label, failures) {
  const limit = baseline * fraction;
  if (observed > limit) failures.push({ metric: label, observed, limit });
}

function managerAcceptance(override, identity, backend, rows, failure, aggregateImprovement) {
  if (!override || backend !== "slatedb" || failure.metric !== "physical_read_objects") return false;
  if (aggregateImprovement < 20) return false;
  return override.acceptances?.some(
    (entry) =>
      entry.acceptedBy &&
      entry.candidateHead === identity.candidate_head &&
      entry.backend === backend &&
      Number(entry.rows) === rows &&
      entry.query === failure.query &&
      entry.metric === failure.metric &&
      Number(entry.observed) === failure.observed &&
      Number(entry.limit) === failure.limit &&
      Number(entry.aggregateTradeoffPct) === aggregateImprovement &&
      /^[0-9a-f]{64}$/.test(entry.reportSha256),
  );
}

export function evaluateControl(records, baseline, backend, rows, override = null) {
  const failures = [];
  const identity = records.find((record) => record.kind === "identity");
  if (!identity || identity.spi !== SPI || identity.owner !== OWNER) {
    failures.push({ metric: "owner_identity", observed: identity ?? null, limit: `${SPI}/${OWNER}` });
  }
  const currentRun = baselineRun(baseline, rows, backend);
  const storage = records.find((record) => record.kind === "storage");
  if (!storage) {
    failures.push({ metric: "storage_record", observed: null, limit: "present" });
  } else {
    atMost(Number(storage.settled_disk_bytes), number(currentRun, "reopen_disk_bytes"), 1.05, "settled_disk_bytes", failures);
    atMost(Number(storage.max_rss_bytes), number(currentRun, "max_rss_kib") * 1024, 1.05, "max_rss_bytes", failures);
    if (Number(storage.write_objects) !== 0 || Number(storage.write_bytes) !== 0) {
      failures.push({ metric: "query_phase_writes", observed: [storage.write_objects, storage.write_bytes], limit: [0, 0] });
    }
  }

  const queryRecords = [];
  for (const query of SELECTED) {
    const record = records.find((candidate) => candidate.kind === "query" && candidate.query === query);
    const reopen = records.find((candidate) => candidate.kind === "reopen" && candidate.query === query);
    const digest = expectedDigest(baseline, rows, query);
    if (!record) {
      failures.push({ query, metric: "query_record", observed: null, limit: "present" });
      continue;
    }
    queryRecords.push(record);
    if (record.digest !== digest.blake3 || Number(record.result_rows) !== Number(digest.result_rows)) {
      failures.push({ query, metric: "result_digest", observed: [record.digest, record.result_rows], limit: [digest.blake3, digest.result_rows] });
    }
    if (!reopen || reopen.digest !== digest.blake3 || reopen.exact_results !== true) {
      failures.push({ query, metric: "cold_reopen", observed: reopen ?? null, limit: digest.blake3 });
    }
    if (Number(record.coherent_storage_reads) !== 1) {
      failures.push({ query, metric: "coherent_storage_reads", observed: record.coherent_storage_reads, limit: 1 });
    }
    if (record.authenticated_block_batching !== true || Number(record.authenticated_blocks) < 1) {
      failures.push({ query, metric: "authenticated_block_batching", observed: record.authenticated_block_batching, limit: true });
    }
    if (query === "column_projection" && record.projection_before_row_allocation !== true) {
      failures.push({ query, metric: "projection_before_row_allocation", observed: record.projection_before_row_allocation, limit: true });
    }
    if (Number(record.write_objects) !== 0 || Number(record.write_bytes) !== 0) {
      failures.push({ query, metric: "query_writes", observed: [record.write_objects, record.write_bytes], limit: [0, 0] });
    }

    const current = baselineQuery(baseline, rows, "lix", backend, query);
    const model = baselineQuery(baseline, rows, "forktree-model", backend, query);
    const wallFraction = ["pk_range", "column_projection"].includes(query) ? 0.90 : 1.05;
    atMost(Number(record.wall_us), number(current, "wall_us_median"), wallFraction, `${query}:wall_us`, failures);
    atMost(Number(record.cpu_us), number(current, "cpu_us_median"), 1.05, `${query}:cpu_us`, failures);
    atMost(Number(record.alloc_bytes), number(current, "alloc_bytes_median"), 1.05, `${query}:alloc_bytes`, failures);

    if (backend === "slatedb") {
      const before = failures.length;
      atMost(Number(record.physical_read_objects), number(current, "physical_read_objects"), 1.05, "physical_read_objects", failures);
      for (let index = before; index < failures.length; index += 1) failures[index].query = query;
      atMost(Number(record.physical_read_bytes), number(current, "physical_read_bytes"), 1.05, `${query}:physical_read_bytes`, failures);
    } else {
      atMost(Number(record.backend_calls), number(model, "get_calls"), 1.05, `${query}:backend_calls`, failures);
      atMost(Number(record.physical_read_bytes), number(model, "get_value_bytes"), 1.05, `${query}:physical_read_bytes`, failures);
    }
  }

  const currentWall = SELECTED.reduce(
    (sum, query) => sum + number(baselineQuery(baseline, rows, "lix", backend, query), "wall_us_median"),
    0,
  );
  const candidateWall = queryRecords.reduce((sum, record) => sum + Number(record.wall_us), 0);
  const aggregateImprovement = Number((100 * (1 - candidateWall / currentWall)).toFixed(6));
  const unaccepted = failures.filter(
    (failure) => !managerAcceptance(override, identity ?? {}, backend, rows, failure, aggregateImprovement),
  );
  return { pass: unaccepted.length === 0, failures: unaccepted, waived: failures.length - unaccepted.length, aggregateImprovement };
}

export function evaluateCorruption(records, backend, rows, fault) {
  const record = records.find((candidate) => candidate.kind === "corruption");
  const failures = [];
  if (!record || record.backend !== backend || Number(record.rows) !== rows || record.fault !== fault) {
    failures.push({ metric: "corruption_record", observed: record ?? null, limit: `${backend}/${rows}/${fault}` });
  } else {
    if (record.fail_closed !== true || record.error_class !== "corruption") failures.push({ metric: "fail_closed", observed: record, limit: "corruption" });
    if (Number(record.write_objects) !== 0 || Number(record.write_bytes) !== 0) failures.push({ metric: "partial_publication", observed: [record.write_objects, record.write_bytes], limit: [0, 0] });
    if (Number(record.disk_before) !== Number(record.disk_after)) failures.push({ metric: "corruption_disk_mutation", observed: [record.disk_before, record.disk_after], limit: "equal" });
  }
  return { pass: failures.length === 0, failures };
}

function parseJsonLines(stdout) {
  return stdout
    .split(/\r?\n/)
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
}

function runCell(binary, evidence, args) {
  const name = args.join("-");
  const result = spawnSync(binary, args, { encoding: "utf8", timeout: CELL_TIMEOUT_MS });
  writeFileSync(join(evidence, `${name}.stdout.log`), result.stdout ?? "");
  writeFileSync(join(evidence, `${name}.stderr.log`), result.stderr ?? "");
  if (result.error || result.status !== 0) throw new Error(`${name} failed: ${result.error ?? `exit ${result.status}`}`);
  return parseJsonLines(result.stdout);
}

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) options[argv[index].replace(/^--/, "")] = argv[index + 1];
  return options;
}

export function orderedPlan() {
  return [
    ["control", "rocksdb", "10000"],
    ["corrupt", "rocksdb", "10000", "malformed_block"],
    ["corrupt", "rocksdb", "10000", "substituted_block"],
    ["control", "slatedb", "10000"],
    ["corrupt", "slatedb", "10000", "malformed_block"],
    ["corrupt", "slatedb", "10000", "substituted_block"],
    ["control", "rocksdb", "50000"],
    ["control", "slatedb", "50000"],
    ["control", "rocksdb", "500000"],
    ["control", "slatedb", "500000"],
  ];
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  if (!options.binary || !options.evidence) throw new Error("usage: gate --binary PATH --evidence DIR [--manager-override FILE]");
  const baseline = loadBaseline();
  const override = options["manager-override"] ? JSON.parse(readFileSync(options["manager-override"], "utf8")) : null;
  const evidence = resolve(options.evidence);
  mkdirSync(evidence, { recursive: true });
  const results = [];
  for (const args of orderedPlan()) {
    const records = runCell(resolve(options.binary), evidence, args);
    const evaluation = args[0] === "control"
      ? evaluateControl(records, baseline, args[1], Number(args[2]), override)
      : evaluateCorruption(records, args[1], Number(args[2]), args[3]);
    results.push({ args, evaluation });
    if (!evaluation.pass) {
      writeFileSync(join(evidence, "GATE.json"), `${JSON.stringify({ pass: false, results }, null, 2)}\n`);
      process.exitCode = 1;
      return;
    }
  }
  writeFileSync(join(evidence, "GATE.json"), `${JSON.stringify({ pass: true, results }, null, 2)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
