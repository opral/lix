import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";
import { openLix } from "@lix-js/sdk";
import { createBetterSqlite3Backend } from "@lix-js/better-sqlite3-backend";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");

const CONFIG = {
  repoPath: process.env.VSCODE_REPLAY_REPO_PATH ?? join(REPO_ROOT, "artifact", "vscode-docs"),
  lixPath:
    process.env.VSCODE_REPLAY_OUTPUT_PATH ??
    join(__dirname, "..", "results", "vscode-docs-first-100.lix"),
};

async function main() {
  const gitWorktreeBytes = await directorySize(CONFIG.repoPath, {
    ignoreNames: new Set([".git"]),
  });
  const gitMetadataBytes = await directorySize(join(CONFIG.repoPath, ".git"));
  const gitTotalBytes = gitWorktreeBytes + gitMetadataBytes;

  let lixFileBytes = 0;
  try {
    lixFileBytes = (await stat(CONFIG.lixPath)).size;
  } catch {
    throw new Error(
      `lix file does not exist: ${CONFIG.lixPath}. Run replay first (pnpm --filter vscode-docs-replay run replay -- --commits 100).`,
    );
  }

  const backend = await createBetterSqlite3Backend({ filename: CONFIG.lixPath });
  const lix = await openLix({ backend });

  try {
    const pageSize = await queryScalarNumber(lix, "PRAGMA page_size", "page_size");
    const pageCount = await queryScalarNumber(lix, "PRAGMA page_count", "page_count");

    const tableNames = await queryColumnText(
      lix,
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
      "table names",
    );

    const tableRows = [];
    for (const tableName of tableNames) {
      const rowCount = await queryScalarNumber(
        lix,
        `SELECT COUNT(*) FROM ${quoteIdentifier(tableName)}`,
        `row count for ${tableName}`,
      );
      tableRows.push({ tableName, rowCount });
    }
    tableRows.sort((a, b) => b.rowCount - a.rowCount);

    let dbstatByObject = [];
    try {
      const dbstatResult = await lix.execute(
        "SELECT name, SUM(pgsize) AS bytes, COUNT(*) AS pages FROM dbstat GROUP BY name ORDER BY bytes DESC LIMIT 20",
        [],
      );
      dbstatByObject = (dbstatResult.rows ?? []).map((row) => ({
        name: scalarToString(row?.[0], "dbstat.name"),
        bytes: scalarToNumber(row?.[1], "dbstat.bytes"),
        pages: scalarToNumber(row?.[2], "dbstat.pages"),
      }));
    } catch {
      dbstatByObject = [];
    }

    let topChangeSchemas = [];
    if (tableNames.includes("lix_internal_change")) {
      const changeSchemaResult = await lix.execute(
        "SELECT schema_key, COUNT(*) AS row_count FROM lix_internal_change GROUP BY schema_key ORDER BY row_count DESC LIMIT 20",
        [],
      );
      topChangeSchemas = (changeSchemaResult.rows ?? []).map((row) => ({
        schemaKey: scalarToString(row?.[0], "schema_key"),
        rowCount: scalarToNumber(row?.[1], "row_count"),
      }));
    }

    console.log("[analyze] Storage comparison");
    console.log(`  git worktree: ${formatBytes(gitWorktreeBytes)} (${gitWorktreeBytes} bytes)`);
    console.log(`  git .git    : ${formatBytes(gitMetadataBytes)} (${gitMetadataBytes} bytes)`);
    console.log(`  git total   : ${formatBytes(gitTotalBytes)} (${gitTotalBytes} bytes)`);
    console.log(`  lix file    : ${formatBytes(lixFileBytes)} (${lixFileBytes} bytes)`);
    console.log(
      `  lix vs git  : ${(lixFileBytes / Math.max(gitTotalBytes, 1)).toFixed(4)}x of git total`,
    );

    console.log("\n[analyze] Lix database footprint");
    console.log(`  page_size : ${pageSize}`);
    console.log(`  page_count: ${pageCount}`);
    console.log(`  estimated : ${formatBytes(pageSize * pageCount)} (${pageSize * pageCount} bytes)`);

    console.log("\n[analyze] Top tables by row count");
    for (const entry of tableRows.slice(0, 20)) {
      console.log(`  ${entry.tableName}: ${entry.rowCount}`);
    }

    if (dbstatByObject.length > 0) {
      console.log("\n[analyze] Top sqlite objects by bytes (dbstat)");
      for (const entry of dbstatByObject) {
        console.log(`  ${entry.name}: ${formatBytes(entry.bytes)} (${entry.bytes} bytes, pages=${entry.pages})`);
      }
    } else {
      console.log("\n[analyze] dbstat unavailable; per-object byte sizes could not be computed");
    }

    if (topChangeSchemas.length > 0) {
      console.log("\n[analyze] Top schema_key counts in lix_internal_change");
      for (const entry of topChangeSchemas) {
        console.log(`  ${entry.schemaKey}: ${entry.rowCount}`);
      }
    }
  } finally {
    await lix.close();
  }
}

async function queryColumnText(lix, sql, context) {
  const result = await lix.execute(sql, []);
  return (result.rows ?? []).map((row, index) =>
    scalarToString(row?.[0], `${context}[${index}]`),
  );
}

async function queryScalarNumber(lix, sql, context) {
  const result = await lix.execute(sql, []);
  return scalarToNumber(result.rows?.[0]?.[0], context);
}

function scalarToNumber(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`missing scalar for ${context}`);
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    return Number(value);
  }
  if (typeof value === "object") {
    if (value.kind === "Integer" || value.kind === "Real" || value.kind === "Text") {
      return Number(value.value);
    }
  }
  throw new Error(`unsupported scalar value for ${context}: ${JSON.stringify(value)}`);
}

function scalarToString(value, context) {
  if (value === null || value === undefined) {
    throw new Error(`missing scalar for ${context}`);
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "object") {
    if (value.kind === "Text" || value.kind === "Integer" || value.kind === "Real") {
      return String(value.value);
    }
  }
  throw new Error(`unsupported text scalar for ${context}: ${JSON.stringify(value)}`);
}

function quoteIdentifier(input) {
  return `"${String(input).replace(/"/g, '""')}"`;
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return `${bytes}`;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(unit === 0 ? 0 : 2)} ${units[unit]}`;
}

async function directorySize(path, options = {}) {
  const ignoreNames = options.ignoreNames ?? new Set();
  let total = 0;
  const queue = [path];

  while (queue.length > 0) {
    const current = queue.pop();
    let stats;
    try {
      stats = await stat(current);
    } catch {
      continue;
    }

    if (stats.isFile()) {
      total += stats.size;
      continue;
    }

    if (!stats.isDirectory()) {
      continue;
    }

    let entries;
    try {
      entries = await readdir(current, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      if (ignoreNames.has(entry.name)) {
        continue;
      }
      queue.push(join(current, entry.name));
    }
  }

  return total;
}

main().catch((error) => {
  console.error("analyze failed");
  console.error(error);
  process.exitCode = 1;
});
