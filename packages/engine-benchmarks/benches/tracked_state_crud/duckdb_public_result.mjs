// Fixture-shaped DuckDB control for the 1M public-result CRUD profile.
//
// Run after installing the Node API without modifying the workspace:
//   npm install --prefix /tmp/lix-duckdb --no-save @duckdb/node-api
//   NODE_PATH=/tmp/lix-duckdb/node_modules node duckdb_public_result.mjs
//
// It mirrors the generated 1M `json_pointer` scale rows: a zero-padded path
// plus a JSON object. The timed section orders the rows and materializes
// owned path strings and parsed JSON values, matching the public result shape
// measured by the Lix SQL-session benchmark.

import { createRequire } from "node:module";

// `NODE_PATH` lets this remain a standalone benchmark control instead of
// adding a JavaScript dependency to Lix's Rust workspace.
const require = createRequire(import.meta.url);
const { DuckDBInstance } = require("@duckdb/node-api");
const { version: duckdbNodeApiVersion } = require("@duckdb/node-api/package.json");

const rowCount = Number.parseInt(process.env.LIX_DUCKDB_ROW_COUNT ?? "1000000", 10);
const sampleCount = Number.parseInt(process.env.LIX_DUCKDB_SAMPLES ?? "5", 10);

if (!Number.isSafeInteger(rowCount) || rowCount <= 0) {
  throw new Error("LIX_DUCKDB_ROW_COUNT must be a positive safe integer");
}
if (!Number.isSafeInteger(sampleCount) || sampleCount <= 0) {
  throw new Error("LIX_DUCKDB_SAMPLES must be a positive safe integer");
}

const db = await DuckDBInstance.create(":memory:");
const connection = await db.connect();
await connection.run("PRAGMA threads=1");
await connection.run(`
  CREATE TABLE json_pointer AS
  SELECT
    printf('/~lix-scale/%09d', range) AS path,
    '{"ordinal":' || range::VARCHAR || ',"lane":"scale"}' AS value_json
  FROM range(${rowCount})
`);

const samplesMs = [];
for (let sample = 0; sample < sampleCount; sample += 1) {
  const started = process.hrtime.bigint();
  const reader = await connection.runAndReadAll(
    "SELECT path, value_json FROM json_pointer ORDER BY path",
  );
  const rows = [];
  for (const row of reader.getRows()) {
    rows.push([String(row[0]), JSON.parse(String(row[1]))]);
  }
  if (rows.length !== rowCount || rows.at(-1)[1].ordinal !== rowCount - 1) {
    throw new Error("DuckDB public-result control returned unexpected rows");
  }
  samplesMs.push(Number(process.hrtime.bigint() - started) / 1e6);
}

samplesMs.sort((left, right) => left - right);
console.log(
  JSON.stringify({
    duckdb_node_api_version: duckdbNodeApiVersion,
    node_version: process.version,
    row_count: rowCount,
    samples_ms: samplesMs,
    median_ms: samplesMs[Math.floor(samplesMs.length / 2)],
  }),
);
