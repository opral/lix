export function createReplayState() {
  return {
    pathToFileId: new Map(),
    knownFileIds: new Set(),
  };
}

export function prepareCommitChanges(state, changes, blobByOid) {
  const deleteIds = new Set();
  const insertsById = new Map();
  const updatesById = new Map();
  const statusCounts = new Map();
  let blobBytes = 0;

  for (const change of changes) {
    const status = normalizeStatus(change.status);
    statusCounts.set(status, (statusCounts.get(status) ?? 0) + 1);

    if (status === "D") {
      const deleted = resolveDeletePath(state, change);
      if (deleted) {
        deleteIds.add(deleted.id);
        insertsById.delete(deleted.id);
        updatesById.delete(deleted.id);
      }
      continue;
    }

    if (!change.newPath) {
      continue;
    }

    const writeTarget = resolveWriteTarget(state, change, status);
    const bytes = blobByOid.get(change.newOid);
    if (!bytes) {
      throw new Error(
        `missing blob for ${change.newOid} while applying ${status} ${change.newPath}`,
      );
    }

    blobBytes += bytes.byteLength;
    const write = {
      id: writeTarget.id,
      path: toLixPath(change.newPath),
      previousPath: writeTarget.previousPath,
      data: bytes,
    };

    if (deleteIds.has(write.id)) {
      deleteIds.delete(write.id);
    }

    if (writeTarget.isInsert) {
      insertsById.set(write.id, write);
      updatesById.delete(write.id);
      state.knownFileIds.add(write.id);
      continue;
    }

    if (insertsById.has(write.id)) {
      insertsById.set(write.id, write);
      continue;
    }

    updatesById.set(write.id, write);
  }

  return {
    deletes: [...deleteIds],
    inserts: [...insertsById.values()],
    updates: [...updatesById.values()],
    statusCounts,
    blobBytes,
  };
}

export function buildReplayCommitStatements(batch, options = {}) {
  const maxInsertRows = positiveOrDefault(options.maxInsertRows, 200);
  const maxInsertSqlChars = positiveOrDefault(options.maxInsertSqlChars, 1_500_000);

  if (batch.deletes.length === 0 && batch.inserts.length === 0 && batch.updates.length === 0) {
    return [];
  }

  const statements = [];

  for (const deleteChunk of chunkArray(batch.deletes, 500)) {
    if (deleteChunk.length === 0) {
      continue;
    }
    statements.push(
      replayStatement(
        `DELETE FROM lix_file WHERE id IN (${deleteChunk.map(() => "?").join(", ")})`,
        deleteChunk,
      ),
    );
  }

  appendInsertStatements(statements, batch.inserts, {
    maxInsertRows,
    maxInsertSqlChars,
  });

  appendUpdateStatements(statements, batch.updates, {
    maxInsertRows,
    maxInsertSqlChars,
  });

  return statements;
}

export function buildReplayCommitSqlScript(statements, options = {}) {
  const maxScriptChars = positiveOrUndefined(options.maxScriptChars);

  if (!Array.isArray(statements)) {
    throw new Error("buildReplayCommitSqlScript expects an array of statements");
  }

  if (statements.length === 0) {
    return "";
  }

  const lines = ["BEGIN;"];
  let estimatedChars = 13; // "BEGIN;\nCOMMIT;"

  for (let index = 0; index < statements.length; index++) {
    const statement = statements[index];
    const sql = String(statement?.sql ?? "").trim();

    if (sql.length === 0) {
      throw new Error(`statement ${index} has empty sql`);
    }

    const normalized = sql.endsWith(";") ? sql : `${sql};`;
    lines.push(normalized);
    estimatedChars += normalized.length + 1;

    if (maxScriptChars !== undefined && estimatedChars > maxScriptChars) {
      throw new Error(
        `commit SQL script estimated at ${estimatedChars} chars exceeds limit ${maxScriptChars}`,
      );
    }
  }

  lines.push("COMMIT;");
  return lines.join("\n");
}

function appendInsertStatements(statements, rows, options) {
  if (rows.length === 0) {
    return;
  }

  const maxRowsPerStatement = positiveOrDefault(options.maxInsertRows, 200);

  for (const rowChunk of chunkArray(rows, maxRowsPerStatement)) {
    if (rowChunk.length === 0) {
      continue;
    }
    const params = [];
    const valuesSql = rowChunk
      .map((row) => {
        params.push(row.id, row.path, row.data);
        return "(?, ?, ?)";
      })
      .join(", ");

    statements.push(
      replayStatement(`INSERT INTO lix_file (id, path, data) VALUES ${valuesSql}`, params),
    );
  }
}

function appendUpdateStatements(statements, rows, options) {
  if (rows.length === 0) {
    return;
  }
  const _ = options;
  for (const row of rows) {
    statements.push(
      replayStatement("UPDATE lix_file SET path = ?, data = ? WHERE id = ?", [
        row.path,
        row.data,
        row.id,
      ]),
    );
  }
}

function replayStatement(sql, params = []) {
  return {
    sql,
    params,
  };
}

function normalizeStatus(value) {
  if (!value || typeof value !== "string") {
    return "M";
  }
  return value[0].toUpperCase();
}

function resolveDeletePath(state, change) {
  if (!change.oldPath) {
    return null;
  }
  const id = state.pathToFileId.get(change.oldPath);
  if (!id) {
    return null;
  }
  state.pathToFileId.delete(change.oldPath);
  state.knownFileIds.delete(id);
  return {
    id,
  };
}

function resolveWriteTarget(state, change, status) {
  if (!change.newPath) {
    throw new Error("resolveWriteTarget requires newPath");
  }

  if (status === "R" && change.oldPath) {
    const fromPath = change.oldPath;
    const existingId = state.pathToFileId.get(fromPath);
    if (existingId) {
      state.pathToFileId.delete(fromPath);
      state.pathToFileId.set(change.newPath, existingId);
      return {
        id: existingId,
        isInsert: false,
        previousPath: toLixPath(fromPath),
      };
    }
  }

  const current = state.pathToFileId.get(change.newPath);
  if (current) {
    return {
      id: current,
      isInsert: false,
      previousPath: toLixPath(change.newPath),
    };
  }

  const generated = stableFileId(change.newPath);
  state.pathToFileId.set(change.newPath, generated);
  return {
    id: generated,
    isInsert: !state.knownFileIds.has(generated),
    previousPath: toLixPath(change.newPath),
  };
}

function stableFileId(path) {
  return toLixPath(path);
}

function toLixPath(path) {
  const normalized = String(path).replace(/\\/g, "/");
  const withoutLeadingSlash = normalized.startsWith("/")
    ? normalized.slice(1)
    : normalized;
  const encoded = withoutLeadingSlash
    .split("/")
    .map((segment) => encodePathSegment(segment))
    .join("/");
  return `/${encoded}`;
}

function encodePathSegment(segment) {
  const bytes = new TextEncoder().encode(segment);
  let encoded = "";
  for (const byte of bytes) {
    const isAlphaNum =
      (byte >= 0x30 && byte <= 0x39) ||
      (byte >= 0x41 && byte <= 0x5a) ||
      (byte >= 0x61 && byte <= 0x7a);
    const isSafe =
      byte === 0x2e || // .
      byte === 0x5f || // _
      byte === 0x7e || // ~
      byte === 0x2d; // -
    if (isAlphaNum || isSafe) {
      encoded += String.fromCharCode(byte);
    } else {
      encoded += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return encoded;
}

function chunkArray(values, size) {
  if (values.length === 0) {
    return [];
  }
  const chunks = [];
  for (let i = 0; i < values.length; i += size) {
    chunks.push(values.slice(i, i + size));
  }
  return chunks;
}

function positiveOrDefault(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function positiveOrUndefined(value) {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`expected positive integer, got '${value}'`);
  }
  return parsed;
}
