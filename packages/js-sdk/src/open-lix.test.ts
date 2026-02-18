import { expect, test } from "vitest";
import { openLix } from "./open-lix.js";

test("openLix executes SQL against default in-memory sqlite backend", async () => {
  const lix = await openLix();
  const result = await lix.execute("SELECT 1 + 1", []);

  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({ kind: "Integer", value: 2 });
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

test("installPlugin stores plugin metadata", async () => {
  const lix = await openLix();

  const manifestJson = JSON.stringify({
    key: "plugin_json",
    runtime: "wasm-component-v1",
    api_version: "0.1.0",
    detect_changes_glob: "*.json",
  });
  const wasmBytes = new Uint8Array([
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
  ]);

  await lix.installPlugin({ manifestJson, wasmBytes });

  const result = await lix.execute(
    "SELECT key FROM lix_internal_plugin WHERE key = 'plugin_json'",
  );
  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({ kind: "Text", value: "plugin_json" });
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

test("stateCommitEvents emits filtered commit events", async () => {
  const lix = await openLix();
  const events = lix.stateCommitEvents({ schemaKeys: ["lix_key_value"] });

  await lix.execute(
    "INSERT INTO lix_internal_state_vtable (\
     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
     ) VALUES (\
     'state-commit-events-js', 'lix_key_value', 'lix', 'global', 'lix',\
     '{\"key\":\"state-commit-events-js\",\"value\":\"ok\"}', '1'\
     )",
    [],
  );

  const batch = await waitForBatch(events);
  expect(batch).toBeDefined();
  expect(batch!.changes.length).toBeGreaterThan(0);
  expect(
    batch!.changes.some(
      (change) =>
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
    "INSERT INTO lix_internal_state_vtable (\
     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
     ) VALUES (\
     'observe-js', 'lix_key_value', 'lix', 'global', 'lix',\
     '{\"key\":\"observe-js\",\"value\":\"ok\"}', '1'\
     )",
    [],
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
    "INSERT INTO lix_internal_state_vtable (\
     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
     ) VALUES (\
     'observe-by-version', 'lix_key_value', 'lix', 'global', 'lix',\
     '{\"key\":\"observe-by-version\",\"value\":\"ok\"}', '1'\
     )",
    [],
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
