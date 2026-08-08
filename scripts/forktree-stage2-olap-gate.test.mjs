import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  evaluateControl,
  evaluateCorruption,
  loadBaseline,
  orderedPlan,
} from "./forktree-stage2-olap-gate.mjs";

const selected = ["pk_point", "pk_range", "column_projection", "group_by", "simple_join"];

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

test("frozen comparator tables and production-only bridge retain exact identity", () => {
  const data = "packages/engine-benchmarks/tests/forktree_stage2_olap_acceptance";
  assert.equal(sha256(`${data}/comparator_query_medians.csv`), "2018e22a677e427d693cf66d903a6f11de09eb4104e2106cbfcb4acfc2485a2e");
  assert.equal(sha256(`${data}/comparator_runs.csv`), "347217cd9e54f04ec05ac599b84712edca1d809636f0ec456d50815ab4771dc5");
  assert.equal(sha256(`${data}/result_digests.csv`), "98c15486d8f14fcdf9afa2ed803c6fcde5a87a9299381b525a9acd51c4b027d4");
  const bridge = readFileSync("packages/engine-benchmarks/benches/forktree_stage2_olap_acceptance.rs", "utf8");
  assert.match(bridge, /AcceptancePhysicalLayout/);
  assert.match(bridge, /Stage2ProductionPhysicalLayout/);
  assert.doesNotMatch(bridge, /duckdb|forktree_replacement|fallback|compatibility/i);
});

function records(backend = "slatedb", rows = 50_000) {
  const baseline = loadBaseline();
  const queries = selected.map((query) => {
    const current = baseline.queries.find(
      (row) => Number(row.rows) === rows && row.engine === "lix" && row.backend === backend && row.query === query,
    );
    const model = baseline.queries.find(
      (row) => Number(row.rows) === rows && row.engine === "forktree-model" && row.backend === backend && row.query === query,
    );
    const digest = baseline.digests.find((row) => Number(row.rows) === rows && row.query === query);
    return {
      kind: "query",
      query,
      digest: digest.blake3,
      result_rows: Number(digest.result_rows),
      wall_us: Number(current.wall_us_median) * 0.5,
      cpu_us: Number(current.cpu_us_median) * 0.5,
      alloc_bytes: Number(current.alloc_bytes_median) * 0.5,
      coherent_storage_reads: 1,
      authenticated_block_batching: true,
      authenticated_blocks: 1,
      projection_before_row_allocation: true,
      write_objects: 0,
      write_bytes: 0,
      backend_calls: Number(model.get_calls),
      physical_read_objects: Number(current.physical_read_objects),
      physical_read_bytes: backend === "slatedb" ? Number(current.physical_read_bytes) * 0.5 : Number(model.get_value_bytes),
    };
  });
  const run = baseline.runs.find(
    (row) => Number(row.rows) === rows && row.engine === "lix" && row.backend === backend,
  );
  return [
    { kind: "identity", spi: "AcceptancePhysicalLayout/v1", owner: "forktree-stage2-production", candidate_head: "a".repeat(40) },
    ...queries,
    ...queries.map((query) => ({ kind: "reopen", query: query.query, digest: query.digest, exact_results: true })),
    { kind: "storage", settled_disk_bytes: Number(run.reopen_disk_bytes) * 0.5, max_rss_bytes: Number(run.max_rss_kib) * 512, write_objects: 0, write_bytes: 0 },
  ];
}

test("ordered plan never reaches 50K before both 10K adapters and corruption", () => {
  assert.deepEqual(orderedPlan().slice(0, 6).map((step) => step.join("/")), [
    "control/rocksdb/10000",
    "corrupt/rocksdb/10000/malformed_block",
    "corrupt/rocksdb/10000/substituted_block",
    "control/slatedb/10000",
    "corrupt/slatedb/10000/malformed_block",
    "corrupt/slatedb/10000/substituted_block",
  ]);
});

test("known Slate six-versus-five residual is a hard failure", () => {
  const candidate = records();
  candidate.find((row) => row.kind === "query" && row.query === "pk_range").physical_read_objects = 6;
  const result = evaluateControl(candidate, loadBaseline(), "slatedb", 50_000);
  assert.equal(result.pass, false);
  assert(result.failures.some((failure) => failure.metric === "physical_read_objects"));
});

test("exact manager artifact can waive only overwhelming Slate object tradeoff", () => {
  const candidate = records();
  candidate.find((row) => row.kind === "query" && row.query === "pk_range").physical_read_objects = 6;
  const preliminary = evaluateControl(candidate, loadBaseline(), "slatedb", 50_000);
  const failure = preliminary.failures.find((entry) => entry.metric === "physical_read_objects");
  const override = {
    acceptances: [{
      acceptedBy: "engineering-manager",
      candidateHead: "a".repeat(40),
      backend: "slatedb",
      rows: 50_000,
      query: "pk_range",
      metric: failure.metric,
      observed: failure.observed,
      limit: failure.limit,
      aggregateTradeoffPct: preliminary.aggregateImprovement,
      reportSha256: "b".repeat(64),
    }],
  };
  assert.equal(evaluateControl(candidate, loadBaseline(), "slatedb", 50_000, override).pass, true);
});

test("manager artifact cannot waive non-object regressions", () => {
  const candidate = records();
  candidate.find((row) => row.kind === "query" && row.query === "pk_range").alloc_bytes *= 3;
  const result = evaluateControl(candidate, loadBaseline(), "slatedb", 50_000, {
    acceptances: [{ acceptedBy: "engineering-manager", candidateHead: "a".repeat(40) }],
  });
  assert.equal(result.pass, false);
  assert(result.failures.some((failure) => failure.metric === "pk_range:alloc_bytes"));
});

test("digest mismatch and corruption partial publication fail closed", () => {
  const candidate = records("rocksdb", 10_000);
  candidate.find((row) => row.kind === "query").digest = "0".repeat(64);
  assert.equal(evaluateControl(candidate, loadBaseline(), "rocksdb", 10_000).pass, false);
  assert.equal(
    evaluateCorruption([{ kind: "corruption", backend: "rocksdb", rows: 10_000, fault: "malformed_block", fail_closed: true, error_class: "corruption", write_objects: 1, write_bytes: 1, disk_before: 2, disk_after: 3 }], "rocksdb", 10_000, "malformed_block").pass,
    false,
  );
});
