import { expect, test } from "vitest";
import { openLix, Value } from "./open-lix.js";

function crc32(input: Uint8Array): number {
  let crc = 0xffffffff;
  for (let index = 0; index < input.length; index += 1) {
    crc ^= input[index]!;
    for (let bit = 0; bit < 8; bit += 1) {
      const mask = -(crc & 1);
      crc = (crc >>> 1) ^ (0xedb88320 & mask);
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function createStoredZip(entries: Array<{ name: string; data: Uint8Array }>): Uint8Array {
  const encoder = new TextEncoder();
  const localChunks: Uint8Array[] = [];
  const centralChunks: Uint8Array[] = [];
  let localOffset = 0;

  for (const entry of entries) {
    const name = encoder.encode(entry.name);
    const data = entry.data;
    const crc = crc32(data);

    const localHeader = new Uint8Array(30 + name.byteLength);
    const localView = new DataView(localHeader.buffer);
    localView.setUint32(0, 0x04034b50, true);
    localView.setUint16(4, 20, true);
    localView.setUint16(6, 0, true);
    localView.setUint16(8, 0, true);
    localView.setUint16(10, 0, true);
    localView.setUint16(12, 0, true);
    localView.setUint32(14, crc, true);
    localView.setUint32(18, data.byteLength, true);
    localView.setUint32(22, data.byteLength, true);
    localView.setUint16(26, name.byteLength, true);
    localView.setUint16(28, 0, true);
    localHeader.set(name, 30);

    const centralHeader = new Uint8Array(46 + name.byteLength);
    const centralView = new DataView(centralHeader.buffer);
    centralView.setUint32(0, 0x02014b50, true);
    centralView.setUint16(4, 20, true);
    centralView.setUint16(6, 20, true);
    centralView.setUint16(8, 0, true);
    centralView.setUint16(10, 0, true);
    centralView.setUint16(12, 0, true);
    centralView.setUint16(14, 0, true);
    centralView.setUint32(16, crc, true);
    centralView.setUint32(20, data.byteLength, true);
    centralView.setUint32(24, data.byteLength, true);
    centralView.setUint16(28, name.byteLength, true);
    centralView.setUint16(30, 0, true);
    centralView.setUint16(32, 0, true);
    centralView.setUint16(34, 0, true);
    centralView.setUint16(36, 0, true);
    centralView.setUint32(38, 0, true);
    centralView.setUint32(42, localOffset, true);
    centralHeader.set(name, 46);

    localChunks.push(localHeader, data);
    centralChunks.push(centralHeader);
    localOffset += localHeader.byteLength + data.byteLength;
  }

  const central = concatChunks(centralChunks);
  const eocd = new Uint8Array(22);
  const eocdView = new DataView(eocd.buffer);
  eocdView.setUint32(0, 0x06054b50, true);
  eocdView.setUint16(4, 0, true);
  eocdView.setUint16(6, 0, true);
  eocdView.setUint16(8, entries.length, true);
  eocdView.setUint16(10, entries.length, true);
  eocdView.setUint32(12, central.byteLength, true);
  eocdView.setUint32(16, localOffset, true);
  eocdView.setUint16(20, 0, true);

  return concatChunks([...localChunks, central, eocd]);
}

test("openLix executes SQL against default in-memory sqlite backend", async () => {
  const lix = await openLix();
  const result = await lix.execute("SELECT 1 + 1", []);

  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({ kind: "Integer", value: 2 });
  await lix.close();
});

test("openLix disallows querying internal tables", async () => {
  const lix = await openLix();
  await expect(lix.execute("SELECT * FROM lix_internal_state_vtable", [])).rejects.toThrow(
    "lix_internal_* tables are not allowed",
  );
  await lix.close();
});

test("createVersion + switchVersion use the JS API surface", async () => {
  const lix = await openLix();

  const created = await lix.createVersion({ name: "bench-branch" });
  expect(created.id.length).toBeGreaterThan(0);
  expect(created.name).toBe("bench-branch");

  await lix.switchVersion(created.id);

  const active = await lix.execute(
    "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
  );
  expect(active.rows.length).toBe(1);
  expect(active.rows[0][0]).toEqual({ kind: "Text", value: created.id });
  await lix.close();
});

test("createVersion forwards inheritsFromVersionId and hidden options", async () => {
  const lix = await openLix();

  const created = await lix.createVersion({
    id: "branch-options",
    name: "Branch Options",
    inheritsFromVersionId: "global",
    hidden: true,
  });
  expect(created).toEqual({
    id: "branch-options",
    name: "Branch Options",
    inheritsFromVersionId: "global",
  });

  const row = await lix.execute(
    "SELECT id, name, inherits_from_version_id, hidden \
     FROM lix_version \
     WHERE id = ? \
     LIMIT 1",
    ["branch-options"],
  );
  expect(row.rows.length).toBe(1);
  expect(row.rows[0][0]).toEqual({ kind: "Text", value: "branch-options" });
  expect(row.rows[0][1]).toEqual({ kind: "Text", value: "Branch Options" });
  expect(row.rows[0][2]).toEqual({ kind: "Text", value: "global" });
  expect(row.rows[0][3]).toEqual({ kind: "Text", value: "true" });

  await lix.close();
});

test("createCheckpoint returns checkpoint metadata and rotates working pointer", async () => {
  const lix = await openLix();
  await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
    "js-create-checkpoint",
    "v1",
  ]);

  const checkpoint = await lix.createCheckpoint();
  expect(checkpoint.id.length).toBeGreaterThan(0);
  expect(checkpoint.changeSetId.length).toBeGreaterThan(0);

  const version = await lix.execute(
    "SELECT av.version_id, v.commit_id, v.working_commit_id \
     FROM lix_active_version av \
     JOIN lix_version v ON v.id = av.version_id \
     ORDER BY av.id LIMIT 1",
    [],
  );
  expect(version.rows.length).toBe(1);
  expect(version.rows[0][1]).toEqual({ kind: "Text", value: checkpoint.id });
  expect(version.rows[0][2]).not.toEqual({ kind: "Text", value: checkpoint.id });

  await lix.close();
});

test("executeTransaction applies multiple statements in one call", async () => {
  const lix = await openLix();

  await lix.executeTransaction([
    {
      sql: "INSERT INTO lix_key_value (key, value) VALUES (?, ?)",
      params: ["tx-batch-a", "value-a"],
    },
    {
      sql: "INSERT INTO lix_key_value (key, value) VALUES (?, ?)",
      params: ["tx-batch-b", "value-b"],
    },
  ]);

  const values = await lix.execute(
    "SELECT key, value FROM lix_key_value WHERE key IN (?1, ?2) ORDER BY key",
    ["tx-batch-a", "tx-batch-b"],
  );
  expect(values.rows.length).toBe(2);

  await lix.close();
});

test("beginTransaction commits and rollbacks explicitly", async () => {
  const lix = await openLix();

  const tx = await lix.beginTransaction({ writerKey: "js-sdk-begin-tx" });
  await tx.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
    "tx-explicit-commit",
    "yes",
  ]);
  await tx.commit();

  const committed = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["tx-explicit-commit"],
  );
  expect(committed.rows.length).toBe(1);

  const tx2 = await lix.beginTransaction();
  await tx2.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
    "tx-explicit-rollback",
    "no",
  ]);
  await tx2.rollback();

  const rolledBack = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["tx-explicit-rollback"],
  );
  expect(rolledBack.rows.length).toBe(0);

  await lix.close();
});

test("beginTransaction calls are serialized per lix instance", async () => {
  const lix = await openLix();
  const tx1 = await lix.beginTransaction();

  const tx2Promise = lix.beginTransaction();
  const firstRace = await Promise.race([
    tx2Promise.then(() => "resolved"),
    new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 30)),
  ]);
  expect(firstRace).toBe("timeout");

  await tx1.commit();
  const tx2 = await tx2Promise;
  await tx2.commit();

  await lix.close();
});

test("non-transaction execute waits while a transaction is open", async () => {
  const lix = await openLix();
  const tx = await lix.beginTransaction();
  await tx.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
    "tx-open-visible-only-after-commit",
    "pending",
  ]);

  const executePromise = lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["outside-execute-waits", "ok"],
  );
  const race = await Promise.race([
    executePromise.then(() => "resolved"),
    new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 30)),
  ]);
  expect(race).toBe("timeout");

  await tx.commit();
  await executePromise;

  const rows = await lix.execute(
    "SELECT key FROM lix_key_value WHERE key IN (?1, ?2) ORDER BY key",
    ["outside-execute-waits", "tx-open-visible-only-after-commit"],
  );
  expect(rows.rows.length).toBe(2);

  await lix.close();
});

test("transaction helper commits on success and rolls back on error", async () => {
  const lix = await openLix();

  await lix.transaction(async (tx) => {
    await tx.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
      "tx-helper-commit",
      "ok",
    ]);
  });

  await expect(
    lix.transaction(async (tx) => {
      await tx.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
        "tx-helper-rollback",
        "no",
      ]);
      throw new Error("boom");
    }),
  ).rejects.toThrow("boom");

  const committed = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["tx-helper-commit"],
  );
  expect(committed.rows.length).toBe(1);

  const rolledBack = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["tx-helper-rollback"],
  );
  expect(rolledBack.rows.length).toBe(0);

  await lix.close();
});

test("execute serializes object params for structured JSON columns", async () => {
  const lix = await openLix();

  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    [
      "open-lix-json-param-value",
      {
        enabled: true,
      },
    ],
  );

  const inserted = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["open-lix-json-param-value"],
  );
  expect(inserted.rows.length).toBe(1);
  expect(inserted.rows[0][0]).toEqual({
    kind: "Text",
    value: JSON.stringify({ enabled: true }),
  });

  await lix.close();
});

test("execute preserves typed kind/value params without JSON stringification", async () => {
  const lix = await openLix();

  await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
    "open-lix-typed-param-value",
    { kind: "Text", value: "typed-text-value" },
  ]);

  const inserted = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["open-lix-typed-param-value"],
  );
  expect(inserted.rows.length).toBe(1);
  expect(inserted.rows[0][0]).toEqual({
    kind: "Text",
    value: "typed-text-value",
  });

  await lix.close();
});

test("execute preserves Value instances as typed params", async () => {
  const lix = await openLix();

  await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
    "open-lix-value-instance",
    Value.text("typed-from-instance"),
  ]);

  const inserted = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
    ["open-lix-value-instance"],
  );
  expect(inserted.rows.length).toBe(1);
  expect(inserted.rows[0][0]).toEqual({
    kind: "Text",
    value: "typed-from-instance",
  });

  await lix.close();
});

test("execute options.writerKey is reflected in state commit stream changes", async () => {
  const lix = await openLix();
  const events = lix.stateCommitStream({
    schemaKeys: ["lix_key_value"],
    writerKeys: ["writer-js-sdk-execute"],
  });

  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["writer-key-execute", "ok"],
    { writerKey: "writer-js-sdk-execute" },
  );

  const batch = await waitForBatch(events);
  expect(batch).toBeDefined();
  expect(batch!.changes.length).toBeGreaterThan(0);
  expect(
    batch!.changes.some(
      (change: { writerKey: string | null; entityId: string }) =>
        change.writerKey === "writer-js-sdk-execute" &&
        change.entityId === "writer-key-execute",
    ),
  ).toBe(true);

  events.close();
  await lix.close();
});

test("executeTransaction options.writerKey is reflected in state commit stream changes", async () => {
  const lix = await openLix();
  const events = lix.stateCommitStream({
    schemaKeys: ["lix_key_value"],
    writerKeys: ["writer-js-sdk-tx"],
  });

  await lix.executeTransaction(
    [
      {
        sql: "INSERT INTO lix_key_value (key, value) VALUES (?, ?)",
        params: ["writer-key-tx", "ok"],
      },
    ],
    { writerKey: "writer-js-sdk-tx" },
  );

  const batch = await waitForBatch(events);
  expect(batch).toBeDefined();
  expect(
    batch!.changes.some(
      (change: { writerKey: string | null; entityId: string }) =>
        change.writerKey === "writer-js-sdk-tx" &&
        change.entityId === "writer-key-tx",
    ),
  ).toBe(true);

  events.close();
  await lix.close();
});

test("installPlugin stores plugin metadata", async () => {
  const lix = await openLix();

  const manifestJson = JSON.stringify({
    key: "plugin_json",
    runtime: "wasm-component-v1",
    api_version: "0.1.0",
    match: { path_glob: "*.json" },
    entry: "plugin.wasm",
    schemas: ["schema/plugin_json_schema.json"],
  });
  const wasmBytes = new Uint8Array([
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
  ]);
  const schemaJson = JSON.stringify({
    "x-lix-key": "plugin_json_schema",
    "x-lix-version": "1",
    type: "object",
    properties: { value: { type: "string" } },
    required: ["value"],
    additionalProperties: false,
  });
  const archiveBytes = createStoredZip([
    { name: "manifest.json", data: new TextEncoder().encode(manifestJson) },
    { name: "plugin.wasm", data: wasmBytes },
    {
      name: "schema/plugin_json_schema.json",
      data: new TextEncoder().encode(schemaJson),
    },
  ]);

  await lix.installPlugin({ archiveBytes });
  await expect(lix.installPlugin({ archiveBytes })).resolves.toBeUndefined();
  await lix.close();
});

test("exportSnapshot returns sqlite bytes", async () => {
  const lix = await openLix();
  await lix.execute("INSERT INTO lix_file (id, path, data) VALUES ('f1', '/a.txt', x'01')", []);
  const snapshot = await lix.exportSnapshot();
  expect(snapshot).toBeInstanceOf(Uint8Array);
  expect(snapshot.byteLength).toBeGreaterThan(0);
  await lix.close();
});

test("openLix seeds keyValues at startup", async () => {
  const lix = await openLix({
    keyValues: [
      {
        key: "lix_deterministic_mode",
        value: { enabled: true },
        lixcol_version_id: "global",
      },
    ],
  });
  const result = await lix.execute(
    "SELECT value FROM lix_key_value \
     WHERE key = 'lix_deterministic_mode' LIMIT 1",
    [],
  );
  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({
    kind: "Text",
    value: JSON.stringify({ enabled: true }),
  });
  await lix.close();
});

test("close is idempotent and blocks further API calls", async () => {
  const lix = await openLix();
  await lix.close();
  await lix.close();

  await expect(lix.execute("SELECT 1", [])).rejects.toThrow("lix is closed");
  await expect(lix.createVersion()).rejects.toThrow("lix is closed");
  await expect(lix.switchVersion("v1")).rejects.toThrow("lix is closed");
});

test("stateCommitStream emits filtered commit batches", async () => {
  const lix = await openLix();
  const events = lix.stateCommitStream({ schemaKeys: ["lix_key_value"] });

  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["state-commit-events-js", "ok"],
  );

  const secondBatchTrigger = await lix.execute(
    "SELECT value FROM lix_key_value WHERE key = ?1",
    ["state-commit-events-js"],
  );
  expect(secondBatchTrigger.rows.length).toBe(1);

  const batch = await waitForBatch(events);
  expect(batch).toBeDefined();
  expect(batch!.changes.length).toBeGreaterThan(0);
  expect(
    batch!.changes.some(
      (change: { schemaKey: string; entityId: string }) =>
        change.schemaKey === "lix_key_value" &&
        change.entityId === "state-commit-events-js",
    ),
  ).toBe(true);

  events.close();
  await lix.close();
});

test("observe emits initial and follow-up query results", async () => {
  const lix = await openLix();
  const events = lix.observe({
    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
    params: ["observe-js"],
  });

  const initial = await events.next();
  expect(initial).toBeDefined();
  expect(initial!.sequence).toBe(0);
  expect(initial!.rows.rows).toEqual([]);
  expect(initial!.stateCommitSequence).toBeNull();

  const nextPromise = events.next();
  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["observe-js", "ok"],
  );

  const followUp = await nextPromise;
  expect(followUp).toBeDefined();
  expect(followUp!.sequence).toBe(1);
  expect(followUp!.stateCommitSequence).not.toBeNull();
  expect(followUp!.rows.rows.length).toBe(1);

  events.close();
  await expect(events.next()).resolves.toBeUndefined();
  await lix.close();
});

test("observe on _by_version view emits follow-up results", async () => {
  const lix = await openLix();
  const events = lix.observe({
    sql: "SELECT key FROM lix_key_value_by_version WHERE key = ?1",
    params: ["observe-by-version"],
  });

  const initial = await events.next();
  expect(initial).toBeDefined();
  expect(initial!.sequence).toBe(0);
  expect(initial!.rows.rows).toEqual([]);

  const nextPromise = events.next();
  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["observe-by-version", "ok"],
  );

  const followUp = await withTimeout(nextPromise, 1500);
  expect(followUp).not.toBe(TIMEOUT);
  if (followUp === TIMEOUT || followUp === undefined) {
    throw new Error("observe follow-up did not arrive for _by_version query");
  }
  expect(followUp.sequence).toBe(1);

  events.close();
  await lix.close();
});

test("observe next resolves when closed while waiting", async () => {
  const lix = await openLix();
  const events = lix.observe({
    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
    params: ["observe-close"],
  });

  const initial = await events.next();
  expect(initial).toBeDefined();
  expect(initial!.sequence).toBe(0);

  const pendingNext = events.next();
  events.close();

  const result = await withTimeout(pendingNext, 1500);
  expect(result).not.toBe(TIMEOUT);
  expect(result).toBeUndefined();

  await lix.close();
});

test("observe stream remains usable after query error", async () => {
  const lix = await openLix();
  await lix.execute("CREATE TABLE observe_recover (value TEXT)", []);
  await lix.execute("INSERT INTO observe_recover (value) VALUES ('ok-0')", []);

  const events = lix.observe({
    sql: "SELECT value FROM observe_recover ORDER BY value",
  });

  const initial = await events.next();
  expect(initial).toBeDefined();
  expect(initial!.rows.rows.length).toBe(1);

  await lix.execute("DROP TABLE observe_recover", []);
  const failingNext = events.next();
  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["observe-recover-trigger-1", "x"],
  );

  await expect(failingNext).rejects.toThrow(
    /observe_recover|no such table|does not exist/i,
  );

  await lix.execute("CREATE TABLE observe_recover (value TEXT)", []);
  await lix.execute("INSERT INTO observe_recover (value) VALUES ('ok-1')", []);

  const recoveredNext = events.next();
  await lix.execute(
    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
    ["observe-recover-trigger-2", "x"],
  );

  const recovered = await withTimeout(recoveredNext, 1500);
  expect(recovered).not.toBe(TIMEOUT);
  if (recovered === TIMEOUT || recovered === undefined) {
    throw new Error("observe did not recover after transient query error");
  }
  expect(recovered.rows.rows.length).toBe(1);

  events.close();
  await lix.close();
});

const TIMEOUT = Symbol("timeout");

async function waitForBatch(events: { tryNext(): unknown }): Promise<any | undefined> {
  const timeoutMs = 1000;
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const next = events.tryNext();
    if (next !== undefined) return next;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return undefined;
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
): Promise<T | typeof TIMEOUT> {
  const timeoutPromise = new Promise<typeof TIMEOUT>((resolve) => {
    setTimeout(() => resolve(TIMEOUT), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]);
}
