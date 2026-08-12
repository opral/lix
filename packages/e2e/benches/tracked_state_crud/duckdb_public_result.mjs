// Fixture-shaped DuckDB control for the 1M public-result CRUD profile.
//
// Run after installing the Node API without modifying the workspace:
//   npm install --prefix /tmp/lix-duckdb --no-save @duckdb/node-api
//   NODE_PATH=/tmp/lix-duckdb/node_modules node duckdb_public_result.mjs
//
// The `olap_*` controls create the same typed rows as Lix and materialize
// owned JavaScript values at the same public-result boundary as ExecuteResult.
// `LIX_DUCKDB_OLAP_STATE=pristine|sparse|moderate` selects the matched
// post-publication mutation state before the untimed validation and warmup.

import { createRequire } from "node:module";

// `NODE_PATH` lets this remain a standalone benchmark control instead of
// adding a JavaScript dependency to Lix's Rust workspace.
const require = createRequire(import.meta.url);
const { DuckDBInstance } = require("@duckdb/node-api");
const { version: duckdbNodeApiVersion } = require("@duckdb/node-api/package.json");

const rowCount = Number.parseInt(process.env.LIX_DUCKDB_ROW_COUNT ?? "1000000", 10);
const sampleCount = Number.parseInt(process.env.LIX_DUCKDB_SAMPLES ?? "5", 10);
const shape = process.env.LIX_DUCKDB_SHAPE ?? "full_result";
const olapShape = shape.startsWith("olap_");
const mutationState = process.env.LIX_DUCKDB_OLAP_STATE ?? "pristine";
const mutationStride = {
  pristine: undefined,
  sparse: 1000,
  moderate: 10,
}[mutationState];

if (!Number.isSafeInteger(rowCount) || rowCount <= 0) {
  throw new Error("LIX_DUCKDB_ROW_COUNT must be a positive safe integer");
}
if (!Number.isSafeInteger(sampleCount) || sampleCount <= 0) {
  throw new Error("LIX_DUCKDB_SAMPLES must be a positive safe integer");
}
if (olapShape && rowCount < 20) {
  throw new Error("typed OLAP controls require at least 20 rows");
}
if (!(mutationState in { pristine: true, sparse: true, moderate: true })) {
  throw new Error("LIX_DUCKDB_OLAP_STATE must be pristine, sparse, or moderate");
}
if (!olapShape && mutationState !== "pristine") {
  throw new Error("LIX_DUCKDB_OLAP_STATE is available only for olap_* shapes");
}

const query = {
  full_result: "SELECT path, value_json FROM json_pointer ORDER BY path",
  general_filter_sort:
    "SELECT path, value_json FROM json_pointer WHERE path IS NOT NULL ORDER BY value_json, path",
  general_aggregate:
    "SELECT COUNT(*) AS rows, MIN(path) AS first_path, MAX(path) AS last_path FROM json_pointer WHERE path IS NOT NULL",
  olap_scan:
    "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) SELECT id, ordinal, lane, score, active FROM source WHERE ordinal >= 0",
  olap_filter:
    "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) SELECT ordinal, lane, score FROM source WHERE active = TRUE AND lane IN ('lane-07', 'lane-19') ORDER BY ordinal",
  olap_sort:
    "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) SELECT id, ordinal, score FROM source WHERE active = TRUE ORDER BY score DESC, ordinal ASC LIMIT 10000",
  olap_group:
    "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) SELECT lane, COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, AVG(score) AS score_avg, MIN(score) AS score_min, MAX(score) AS score_max FROM source WHERE active = TRUE GROUP BY lane ORDER BY lane",
  olap_aggregate:
    "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) SELECT COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, AVG(score) AS score_avg, MIN(ordinal) AS min_ordinal, MAX(ordinal) AS max_ordinal FROM source WHERE active = TRUE",
}[shape];
if (query === undefined) {
  throw new Error(
    "unknown LIX_DUCKDB_SHAPE",
  );
}

const db = await DuckDBInstance.create(":memory:");
const connection = await db.connect();
await connection.run("PRAGMA threads=1");
if (olapShape) {
  await connection.run(`
    CREATE TABLE olap_row AS
    SELECT
      printf('/~lix-olap/%09d', ordinal) AS id,
      ordinal::BIGINT AS ordinal,
      printf('lane-%02d', ordinal % 32) AS lane,
      (ordinal % 10000)::DOUBLE / 8.0 AS score,
      ordinal % 3 <> 0 AS active
    FROM range(${rowCount}) AS generated(ordinal)
  `);
  if (mutationStride !== undefined) {
    const insertedRows = mutationCount(rowCount, 2, mutationStride);
    await connection.run(`
      UPDATE olap_row
      SET lane = printf('lane-%02d', (ordinal + 11) % 32),
          score = score + 2048.0,
          active = NOT active
      WHERE ordinal < ${rowCount} AND ordinal % ${mutationStride} = 1
    `);
    await connection.run(`
      DELETE FROM olap_row
      WHERE ordinal < ${rowCount} AND ordinal % ${mutationStride} = 2
    `);
    await connection.run(`
      INSERT INTO olap_row
      SELECT
        printf('/~lix-olap/%09d', ordinal) AS id,
        ordinal::BIGINT AS ordinal,
        printf('lane-%02d', ordinal % 32) AS lane,
        (ordinal % 10000)::DOUBLE / 8.0 AS score,
        ordinal % 3 <> 0 AS active
      FROM range(${rowCount}, ${rowCount + insertedRows}) AS generated(ordinal)
    `);
  }
} else {
  await connection.run(`
    CREATE TABLE json_pointer AS
    SELECT
      printf('/~lix-scale/%09d', range) AS path,
      '{"ordinal":' || range::VARCHAR || ',"lane":"scale"}' AS value_json
    FROM range(${rowCount})
  `);
}

const expected = olapShape ? olapExpected(rowCount, mutationState) : undefined;

async function materialize(validate) {
  const reader = await connection.runAndReadAll(query);
  const rows = [];
  for (const row of reader.getRows()) {
    rows.push(materializeRow(shape, row));
  }
  if (validate) {
    assertRows(shape, rows, rowCount, expected);
  } else {
    assertRowCount(shape, rows, rowCount, expected);
  }
  return rows;
}

if (olapShape) {
  await materialize(true);
  await materialize(false);
}

const samplesMs = [];
for (let sample = 0; sample < sampleCount; sample += 1) {
  const started = process.hrtime.bigint();
  await materialize(!olapShape);
  samplesMs.push(Number(process.hrtime.bigint() - started) / 1e6);
}

samplesMs.sort((left, right) => left - right);
console.log(
  JSON.stringify({
    duckdb_node_api_version: duckdbNodeApiVersion,
    node_version: process.version,
    shape,
    olap_state: mutationState,
    row_count: rowCount,
    samples_ms: samplesMs,
    median_ms: samplesMs[Math.floor(samplesMs.length / 2)],
  }),
);

function materializeRow(selectedShape, row) {
  switch (selectedShape) {
    case "general_aggregate":
      return [Number(row[0]), String(row[1]), String(row[2])];
    case "full_result":
    case "general_filter_sort":
      return [String(row[0]), JSON.parse(String(row[1]))];
    case "olap_scan":
      return [String(row[0]), Number(row[1]), String(row[2]), Number(row[3]), Boolean(row[4])];
    case "olap_filter":
      return [Number(row[0]), String(row[1]), Number(row[2])];
    case "olap_sort":
      return [String(row[0]), Number(row[1]), Number(row[2])];
    case "olap_group":
      return [String(row[0]), Number(row[1]), Number(row[2]), Number(row[3]), Number(row[4]), Number(row[5])];
    case "olap_aggregate":
      return [Number(row[0]), Number(row[1]), Number(row[2]), Number(row[3]), Number(row[4])];
    default:
      throw new Error(`unhandled shape ${selectedShape}`);
  }
}

function mutationCount(count, residue, stride) {
  return count <= residue ? 0 : Math.floor((count - 1 - residue) / stride) + 1;
}

function olapRow(ordinal, initialCount, state) {
  const stride = { pristine: undefined, sparse: 1000, moderate: 10 }[state];
  if (stride !== undefined) {
    const insertedRows = mutationCount(initialCount, 2, stride);
    if (ordinal >= initialCount) {
      if (ordinal >= initialCount + insertedRows) return undefined;
    } else {
      if (ordinal % stride === 2) return undefined;
    }
  } else if (ordinal >= initialCount) {
    return undefined;
  }
  const row = {
    id: `/~lix-olap/${String(ordinal).padStart(9, "0")}`,
    ordinal,
    lane: ordinal % 32,
    score: (ordinal % 10000) / 8,
    active: ordinal % 3 !== 0,
  };
  if (stride !== undefined && ordinal < initialCount && ordinal % stride === 1) {
    row.lane = (row.lane + 11) % 32;
    row.score += 2048;
    row.active = !row.active;
  }
  return row;
}

function olapRows(count, state) {
  const stride = { pristine: undefined, sparse: 1000, moderate: 10 }[state];
  const insertedRows = stride === undefined ? 0 : mutationCount(count, 2, stride);
  const rows = [];
  for (let ordinal = 0; ordinal < count + insertedRows; ordinal += 1) {
    const row = olapRow(ordinal, count, state);
    if (row !== undefined) rows.push(row);
  }
  return rows;
}

function olapExpected(count, state) {
  const groups = Array.from({ length: 32 }, () => ({
    rows: 0,
    ordinalSum: 0,
    scoreSum: 0,
    scoreMin: Number.POSITIVE_INFINITY,
    scoreMax: Number.NEGATIVE_INFINITY,
  }));
  const result = {
    activeRows: 0,
    activeOrdinalSum: 0,
    activeScoreSum: 0,
    activeMinOrdinal: undefined,
    activeMaxOrdinal: undefined,
    filteredRows: 0,
    filteredFirstOrdinal: undefined,
    filteredLastOrdinal: undefined,
    groups,
    state,
    visibleRows: count,
  };
  for (const row of olapRows(count, state)) {
    if (!row.active) continue;
    const { ordinal, lane, score } = row;
    result.activeRows += 1;
    result.activeOrdinalSum += ordinal;
    result.activeScoreSum += score;
    result.activeMinOrdinal ??= ordinal;
    result.activeMaxOrdinal = ordinal;
    const group = groups[lane];
    group.rows += 1;
    group.ordinalSum += ordinal;
    group.scoreSum += score;
    group.scoreMin = Math.min(group.scoreMin, score);
    group.scoreMax = Math.max(group.scoreMax, score);
    if (lane === 7 || lane === 19) {
      result.filteredRows += 1;
      result.filteredFirstOrdinal ??= ordinal;
      result.filteredLastOrdinal = ordinal;
    }
  }
  return result;
}

function assertRows(selectedShape, rows, count, olap) {
  if (selectedShape === "general_aggregate") {
    if (rows.length !== 1 || rows[0][0] !== count) throw new Error("unexpected general aggregate");
    return;
  }
  if (selectedShape === "full_result" || selectedShape === "general_filter_sort") {
    if (rows.length !== count || rows.at(-1)[1].ordinal !== count - 1) throw new Error("unexpected JSON rows");
    return;
  }
  if (selectedShape === "olap_scan") {
    const expectedRows = olapRows(count, olap.state);
    if (rows.length !== expectedRows.length) throw new Error("unexpected OLAP scan cardinality");
    const expectedByOrdinal = new Map(expectedRows.map((row) => [row.ordinal, row]));
    const seen = new Set();
    for (const [id, ordinal, lane, score, active] of rows) {
      const expectedRow = expectedByOrdinal.get(ordinal);
      if (expectedRow === undefined || seen.has(ordinal) || id !== expectedRow.id || lane !== `lane-${String(expectedRow.lane).padStart(2, "0")}` || score !== expectedRow.score || active !== expectedRow.active) {
        throw new Error("unexpected OLAP scan row");
      }
      seen.add(ordinal);
    }
    if (seen.size !== expectedRows.length) throw new Error("OLAP scan omitted rows");
    return;
  }
  if (selectedShape === "olap_filter") {
    const expectedRows = olapRows(count, olap.state).filter((row) => row.active && (row.lane === 7 || row.lane === 19));
    if (rows.length !== expectedRows.length) throw new Error("unexpected OLAP filter cardinality");
    for (let index = 0; index < rows.length; index += 1) {
      const row = expectedRows[index];
      if (rows[index][0] !== row.ordinal || rows[index][1] !== `lane-${String(row.lane).padStart(2, "0")}` || rows[index][2] !== row.score) {
        throw new Error("unexpected OLAP filter result");
      }
    }
    return;
  }
  if (selectedShape === "olap_sort") {
    const expectedRows = olapRows(count, olap.state)
      .filter((row) => row.active)
      .sort((left, right) => right.score - left.score || left.ordinal - right.ordinal)
      .slice(0, 10000);
    if (rows.length !== expectedRows.length) throw new Error("unexpected OLAP sort cardinality");
    for (let index = 0; index < rows.length; index += 1) {
      const [id, ordinal, score] = rows[index];
      const expectedRow = expectedRows[index];
      if (id !== expectedRow.id || ordinal !== expectedRow.ordinal || score !== expectedRow.score) {
        throw new Error("unexpected OLAP sort row");
      }
    }
    return;
  }
  if (selectedShape === "olap_group") {
    if (rows.length !== 32) throw new Error("unexpected OLAP group cardinality");
    rows.forEach((row, lane) => {
      const group = olap.groups[lane];
      if (row[0] !== `lane-${String(lane).padStart(2, "0")}` || row[1] !== group.rows || row[2] !== group.ordinalSum || !close(row[3], group.scoreSum / group.rows) || row[4] !== group.scoreMin || row[5] !== group.scoreMax) {
        throw new Error(`unexpected OLAP group ${lane}`);
      }
    });
    return;
  }
  if (selectedShape === "olap_aggregate") {
    const row = rows[0];
    if (rows.length !== 1 || row[0] !== olap.activeRows || row[1] !== olap.activeOrdinalSum || !close(row[2], olap.activeScoreSum / olap.activeRows) || row[3] !== olap.activeMinOrdinal || row[4] !== olap.activeMaxOrdinal) {
      throw new Error("unexpected OLAP aggregate");
    }
  }
}

function assertRowCount(selectedShape, rows, count, olap) {
  const expected = {
    full_result: count,
    general_filter_sort: count,
    general_aggregate: 1,
    olap_scan: olap?.visibleRows,
    olap_filter: olap?.filteredRows,
    olap_sort: Math.min(10000, olap?.activeRows ?? 0),
    olap_group: 32,
    olap_aggregate: 1,
  }[selectedShape];
  if (rows.length !== expected) throw new Error(`unexpected ${selectedShape} cardinality`);
}

function close(actual, expected) {
  return Math.abs(actual - expected) <= 1e-10 * Math.max(Math.abs(expected), 1);
}
