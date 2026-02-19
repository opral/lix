import { stat } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";
import { openLix } from "js-sdk";
import { createBetterSqlite3Backend } from "@lix-js/better-sqlite3-backend";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..", "..");

const CONFIG = {
  lixPath:
    process.env.VSCODE_REPLAY_OUTPUT_PATH ??
    join(__dirname, "..", "results", "vscode-docs-first-100.lix"),
};

async function main() {
  try {
    await stat(CONFIG.lixPath);
  } catch {
    throw new Error(
      `lix file does not exist: ${CONFIG.lixPath}. Run replay first (pnpm --filter vscode-docs-replay run replay -- --commits 100).`,
    );
  }

  const backend = await createBetterSqlite3Backend({ filename: CONFIG.lixPath });
  const lix = await openLix({ backend });

  try {
    const result = await lix.execute(
      `
      WITH plugin_keys AS (
        SELECT key
        FROM lix_internal_plugin
      ),
      path_files AS (
        SELECT file_id, COALESCE(NULLIF(extension, ''), '(noext)') AS extension
        FROM lix_internal_file_path_cache
      ),
      plugin_file_changes AS (
        SELECT c.file_id, c.plugin_key, COUNT(*) AS change_rows
        FROM lix_internal_change c
        JOIN plugin_keys pk ON pk.key = c.plugin_key
        GROUP BY c.file_id, c.plugin_key
      ),
      plugin_extension_stats AS (
        SELECT
          pfc.plugin_key AS plugin_key,
          pf.extension AS extension,
          COUNT(DISTINCT pf.file_id) AS file_count,
          SUM(pfc.change_rows) AS change_rows
        FROM plugin_file_changes pfc
        JOIN path_files pf ON pf.file_id = pfc.file_id
        GROUP BY pfc.plugin_key, pf.extension
      ),
      files_with_any_plugin AS (
        SELECT DISTINCT file_id
        FROM plugin_file_changes
      ),
      no_plugin_stats AS (
        SELECT
          'no_plugin' AS plugin_key,
          pf.extension AS extension,
          COUNT(DISTINCT pf.file_id) AS file_count,
          0 AS change_rows
        FROM path_files pf
        LEFT JOIN files_with_any_plugin f ON f.file_id = pf.file_id
        WHERE f.file_id IS NULL
        GROUP BY pf.extension
      )
      SELECT plugin_key, extension, file_count, change_rows
      FROM plugin_extension_stats
      UNION ALL
      SELECT plugin_key, extension, file_count, change_rows
      FROM no_plugin_stats
      `,
      [],
    );

    const rows = (result.rows ?? []).map((row, index) => ({
      pluginKey: scalarToString(row?.[0], `rows[${index}].plugin_key`),
      extension: scalarToString(row?.[1], `rows[${index}].extension`),
      fileCount: scalarToNumber(row?.[2], `rows[${index}].file_count`),
      changeRows: scalarToNumber(row?.[3], `rows[${index}].change_rows`),
    }));

    const grouped = new Map();
    for (const row of rows) {
      if (!grouped.has(row.pluginKey)) {
        grouped.set(row.pluginKey, []);
      }
      grouped.get(row.pluginKey).push(row);
    }

    const pluginKeys = Array.from(grouped.keys()).sort((a, b) => {
      if (a === "no_plugin" && b !== "no_plugin") {
        return 1;
      }
      if (a !== "no_plugin" && b === "no_plugin") {
        return -1;
      }
      return a.localeCompare(b);
    });

    console.log(
      "Plugin handling by file type (from lix_internal_change joined with lix_internal_file_path_cache):\n",
    );

    if (grouped.size === 0) {
      console.log("- no data");
      return;
    }

    for (const pluginKey of pluginKeys) {
      const entries = grouped.get(pluginKey);
      entries.sort((a, b) => {
        if (b.fileCount !== a.fileCount) {
          return b.fileCount - a.fileCount;
        }
        return a.extension.localeCompare(b.extension);
      });

      console.log(`- ${pluginKey}`);
      const includeChangeRows = pluginKey !== "no_plugin" && entries.length === 1;

      for (const entry of entries) {
        const fileCount = formatFileCount(entry.fileCount);
        if (includeChangeRows) {
          const changeRows = formatNumber(entry.changeRows);
          console.log(
            `  - ${entry.extension}: ${fileCount} (${changeRows} change rows)`,
          );
        } else {
          console.log(`  - ${entry.extension}: ${fileCount}`);
        }
      }
    }
  } finally {
    await lix.close();
  }
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

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function formatFileCount(value) {
  const count = formatNumber(value);
  return `${count} ${value === 1 ? "file" : "files"}`;
}

main().catch((error) => {
  console.error("analyze-file-types failed");
  console.error(error);
  process.exitCode = 1;
});
