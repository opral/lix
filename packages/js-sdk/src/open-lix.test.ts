import { expect, test } from "vitest";
import { openLix } from "./open-lix.js";

test("openLix executes SQL against default in-memory sqlite backend", async () => {
  const lix = await openLix();
  const result = await lix.execute("SELECT 1 + 1", []);

  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({ kind: "Integer", value: 2 });
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
});

test("installPlugin stores plugin metadata", async () => {
  const lix = await openLix();

  const manifestJson = JSON.stringify({
    key: "plugin_json",
    runtime: "wasm-component-v1",
    api_version: "0.1.0",
    detect_changes_glob: "*.json",
  });
  const wasmBytes = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

  await lix.installPlugin({ manifestJson, wasmBytes });

  const result = await lix.execute(
    "SELECT key FROM lix_internal_plugin WHERE key = 'plugin_json'",
  );
  expect(result.rows.length).toBe(1);
  expect(result.rows[0][0]).toEqual({ kind: "Text", value: "plugin_json" });
});
