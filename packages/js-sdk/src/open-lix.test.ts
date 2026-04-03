import { expect, test } from "vitest";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import { initLix, openLix } from "./open-lix.js";

async function createInitializedLix(args?: {
	keyValues?: Parameters<typeof initLix>[0]["keyValues"];
}) {
	const backend = await createWasmSqliteBackend();
	await initLix({
		backend,
		keyValues: args?.keyValues,
	});
	return await openLix({ backend });
}

function statementRows(
	result: { statements: Array<{ rows: unknown[][] }> },
	statementIndex = 0,
): unknown[][] {
	return result.statements[statementIndex]?.rows ?? [];
}

test("openLix rejects when backend is not initialized", async () => {
	const backend = await createWasmSqliteBackend();
	await expect(openLix({ backend })).rejects.toThrow(/NOT_INITIALIZED/i);
});

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

function createStoredZip(
	entries: Array<{ name: string; data: Uint8Array }>,
): Uint8Array {
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
	const rows = statementRows(result);

	expect(rows.length).toBe(1);
	expect(rows[0]?.[0]).toBe(2);
	await lix.close();
});

test("openLix allows querying internal tables for debugging", async () => {
	const lix = await createInitializedLix();
	const result = await lix.execute("SELECT id FROM lix_internal_snapshot LIMIT 1", []);
	expect(statementRows(result).length).toBeGreaterThan(0);
	await lix.close();
});

test("createVersion + switchVersion use the JS API surface", async () => {
	const lix = await createInitializedLix();

	const created = await lix.createVersion({ name: "bench-branch" });
	expect(created.id.length).toBeGreaterThan(0);
	expect(created.name).toBe("bench-branch");

	await lix.switchVersion(created.id);

	expect(await lix.activeVersionId()).toBe(created.id);
	await lix.close();
});

test("openChildSession snapshots the caller active version and isolates later switches", async () => {
	const lix = await createInitializedLix();
	const version = await lix.createVersion({ name: "session-branch" });
	await lix.switchVersion(version.id);

	const worker = await lix.openChildSession();
	await worker.switchVersion("global");

	expect(await lix.activeVersionId()).toBe(version.id);
	expect(await worker.activeVersionId()).toBe("global");

	await worker.close();
	await lix.close();
});

test("openChildSession accepts an explicit activeVersionId override", async () => {
	const lix = await createInitializedLix();
	const version = await lix.createVersion({ name: "override-branch" });

	const worker = await lix.openChildSession({
		activeVersionId: version.id,
	});
	expect(await worker.activeVersionId()).toBe(version.id);

	await worker.close();
	await lix.close();
});

test("openChildSession snapshots active accounts and allows explicit overrides", async () => {
	const lix = await createInitializedLix();
	const seeded = await lix.openChildSession({
		activeAccountIds: ["acct-parent"],
	});
	const worker = await seeded.openChildSession();
	const overrideWorker = await seeded.openChildSession({
		activeAccountIds: ["acct-override"],
	});

	expect(await lix.activeAccountIds()).toEqual([]);
	expect(await seeded.activeAccountIds()).toEqual(["acct-parent"]);
	expect(await worker.activeAccountIds()).toEqual(["acct-parent"]);
	expect(await overrideWorker.activeAccountIds()).toEqual(["acct-override"]);

	await overrideWorker.close();
	await worker.close();
	await seeded.close();
	await lix.close();
});

test("openLix reopens on the workspace-backed active version", async () => {
	const backend = await createWasmSqliteBackend();
	await initLix({ backend });

	const lix = await openLix({ backend });
	const version = await lix.createVersion({ name: "workspace-reopen" });
	await lix.switchVersion(version.id);
	await lix.close();

	const reopened = await openLix({ backend });
	expect(await reopened.activeVersionId()).toBe(version.id);

	await reopened.close();
});

test("createVersion forwards hidden option", async () => {
	const lix = await createInitializedLix();

	const created = await lix.createVersion({
		id: "branch-options",
		name: "Branch Options",
		hidden: true,
	});
	expect(created).toEqual({
		id: "branch-options",
		name: "Branch Options",
	});

	const row = await lix.execute(
		"SELECT id, name, hidden \
     FROM lix_version \
     WHERE id = ? \
     LIMIT 1",
		["branch-options"],
	);
	const rowValues = statementRows(row);
	expect(rowValues.length).toBe(1);
	expect(rowValues[0]?.[0]).toBe("branch-options");
	expect(rowValues[0]?.[1]).toBe("Branch Options");
	expect(rowValues[0]?.[2]).toBe(true);

	await lix.close();
});

test("createCheckpoint returns checkpoint metadata and rotates working pointer", async () => {
	const lix = await createInitializedLix();
	await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
		"js-create-checkpoint",
		"v1",
	]);

	const checkpoint = await lix.createCheckpoint();
	expect(checkpoint.id.length).toBeGreaterThan(0);
	expect(checkpoint.changeSetId.length).toBeGreaterThan(0);

	const activeVersionId = await lix.activeVersionId();
	const version = await lix.execute(
		"SELECT commit_id \
     FROM lix_version \
     WHERE id = ?1 \
     LIMIT 1",
		[activeVersionId],
	);
	const versionRows = statementRows(version);
	expect(versionRows.length).toBe(1);
	expect(versionRows[0]?.[0]).toBe(checkpoint.id);

	await lix.close();
});

test("execute applies multiple statements in one call", async () => {
	const lix = await createInitializedLix();

	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('tx-batch-a', 'value-a'); \
	 INSERT INTO lix_key_value (key, value) VALUES ('tx-batch-b', 'value-b');",
	);

	const values = await lix.execute(
		"SELECT key, value FROM lix_key_value WHERE key IN (?1, ?2) ORDER BY key",
		["tx-batch-a", "tx-batch-b"],
	);
	expect(statementRows(values).length).toBe(2);

	await lix.close();
});

test("execute accepts explicit BEGIN/COMMIT wrappers", async () => {
	const lix = await createInitializedLix();

	await lix.execute(
		"BEGIN; INSERT INTO lix_key_value (key, value) VALUES (?1, ?2); COMMIT;",
		["js-sdk-begin-commit", "ok"],
	);

	const value = await lix.execute(
		"SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
		["js-sdk-begin-commit"],
	);
	const valueRows = statementRows(value);
	expect(valueRows.length).toBe(1);
	expect(valueRows[0]?.[0]).toBe("ok");

	await lix.close();
});

test("execute rejects unsupported transaction control variants", async () => {
	const lix = await createInitializedLix();

	await expect(lix.execute("BEGIN IMMEDIATE;", [])).rejects.toThrow(
		/plain BEGIN|transaction control|not supported/i,
	);

	await lix.close();
});

test("execute rejects object params that are not runtime values", async () => {
	const lix = await createInitializedLix();

	await expect(
		lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
			"open-lix-json-param-value",
			{
				enabled: true,
			} as unknown as never,
		]),
	).rejects.toThrow(/runtime scalar values|Uint8Array/i);

	await lix.close();
});

test("execute persists raw scalar params", async () => {
	const lix = await createInitializedLix();

	await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
		"open-lix-typed-param-value",
		"typed-text-value",
	]);

	const inserted = await lix.execute(
		"SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
		["open-lix-typed-param-value"],
	);
	const insertedRows = statementRows(inserted);
	expect(insertedRows.length).toBe(1);
	expect(insertedRows[0]?.[0]).toBe("typed-text-value");

	await lix.close();
});

test("installPlugin stores plugin metadata", async () => {
	const lix = await createInitializedLix();

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

test("export_image returns sqlite bytes", async () => {
	const lix = await createInitializedLix();
	await lix.execute(
		"INSERT INTO lix_file (id, path, data) VALUES ('f1', '/a.txt', x'01')",
		[],
	);
	const image = await lix.export_image();
	expect(image).toBeInstanceOf(Uint8Array);
	expect(image.byteLength).toBeGreaterThan(0);
	await lix.close();
});

test("lix_file.data roundtrips as Uint8Array runtime value", async () => {
	const lix = await createInitializedLix();
	await lix.execute(
		"INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3)",
		["blob-roundtrip", "/blob.bin", new Uint8Array([1, 2, 3])],
	);
	const result = await lix.execute(
		"SELECT data FROM lix_file WHERE id = ?1 LIMIT 1",
		["blob-roundtrip"],
	);
	const resultRows = statementRows(result);
	expect(resultRows.length).toBe(1);
	expect(resultRows[0]?.[0]).toEqual(new Uint8Array([1, 2, 3]));
	await lix.close();
});

test("openLix seeds global keyValues at startup", async () => {
	const lix = await openLix({
		keyValues: [
			{
				key: "lix_deterministic_mode",
				value: { enabled: true },
				lixcol_global: true,
			},
		],
	});
	const result = await lix.execute(
		"SELECT COUNT(*) FROM lix_state_by_version \
     WHERE schema_key = 'lix_key_value' \
       AND entity_id = ?1 \
       AND version_id = 'global' \
       AND snapshot_content IS NOT NULL",
		["lix_deterministic_mode"],
	);
	const resultRows = statementRows(result);
	expect(resultRows.length).toBe(1);
	expect(resultRows[0]?.[0]).toBe(1);
	await lix.close();
});

test("openLix defaults boot keyValues to the active version", async () => {
	const lix = await openLix({
		keyValues: [
			{
				key: "boot-default-active",
				value: "active-value",
			},
		],
	});
	const activeVersionId = await lix.activeVersionId();

	const result = await lix.execute(
		"SELECT COUNT(*) FROM lix_key_value_by_version \
     WHERE key = ?1 \
       AND lixcol_version_id = ?2 \
       AND lixcol_global = false",
		["boot-default-active", activeVersionId],
	);
	const resultRows = statementRows(result);
	expect(resultRows.length).toBe(1);
	expect(resultRows[0]?.[0]).toBe(1);
	await lix.close();
});

test("initLix rejects legacy boot keyValue version fields", async () => {
	const backend = await createWasmSqliteBackend();
	await expect(
		initLix({
			backend,
			keyValues: [
				{
					key: "legacy-boot",
					value: "x",
					lixcol_version_id: "global",
				} as any,
			],
		}),
	).rejects.toThrow("lixcol_global");
});

test("close is idempotent and blocks further API calls", async () => {
	const lix = await createInitializedLix();
	await lix.close();
	await lix.close();

	await expect(lix.execute("SELECT 1", [])).rejects.toThrow("lix is closed");
	await expect(lix.createVersion()).rejects.toThrow("lix is closed");
	await expect(lix.switchVersion("v1")).rejects.toThrow("lix is closed");
});

test("observe emits initial and follow-up query results", async () => {
	const lix = await createInitializedLix();
	const events = lix.observe({
		sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
		params: ["observe-js"],
	});

	const initial = await events.next();
	expect(initial).toBeDefined();
	expect(initial!.sequence).toBe(0);
	expect(initial!.rows.rows).toEqual([]);

	const nextPromise = events.next();
	await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
		"observe-js",
		"ok",
	]);

	const followUp = await nextPromise;
	expect(followUp).toBeDefined();
	expect(followUp!.sequence).toBe(1);
	expect(followUp!.rows.rows.length).toBe(1);

	events.close();
	await expect(events.next()).resolves.toBeUndefined();
	await lix.close();
});

test("observe on _by_version view emits follow-up results", async () => {
	const lix = await createInitializedLix();
	const events = lix.observe({
		sql: "SELECT key FROM lix_key_value_by_version WHERE key = ?1",
		params: ["observe-by-version"],
	});

	const initial = await events.next();
	expect(initial).toBeDefined();
	expect(initial!.sequence).toBe(0);
	expect(initial!.rows.rows).toEqual([]);

	const nextPromise = events.next();
	await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
		"observe-by-version",
		"ok",
	]);

	const followUp = await withTimeout(nextPromise, 1500);
	expect(followUp).not.toBe(TIMEOUT);
	if (followUp === TIMEOUT || followUp === undefined) {
		throw new Error("observe follow-up did not arrive for _by_version query");
	}
	expect(followUp.sequence).toBe(1);

	events.close();
	await lix.close();
});

test("observe on lix_state emits follow-up when switching active version", async () => {
	const lix = await createInitializedLix();
	const branch = await lix.createVersion({ name: "observe-switch-state" });
	const entityId = "observe-switch-state-key";

	await lix.execute(
		"INSERT INTO lix_state_by_version \
		 (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
		[
			entityId,
			"lix_key_value",
			"lix",
			"global",
			"lix_sdk",
			JSON.stringify({ key: entityId, value: "global" }),
			"1",
		],
	);
	await lix.execute(
		"INSERT INTO lix_state_by_version \
		 (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
		[
			entityId,
			"lix_key_value",
			"lix",
			branch.id,
			"lix_sdk",
			JSON.stringify({ key: entityId, value: "branch" }),
			"1",
		],
	);
	await lix.switchVersion("global");

	const events = lix.observe({
		sql: "SELECT json_extract(snapshot_content, '$.value') \
		      FROM lix_state \
		      WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
		params: [entityId],
	});

	const initial = await events.next();
	expect(initial).toBeDefined();
	expect(initial!.rows.rows).toEqual([["global"]]);

	const nextPromise = events.next();
	await lix.switchVersion(branch.id);

	const followUp = await withTimeout(nextPromise, 1500);
	expect(followUp).not.toBe(TIMEOUT);
	if (followUp === TIMEOUT || followUp === undefined) {
		throw new Error("observe follow-up did not arrive after switching version");
	}
	expect(followUp.rows.rows).toEqual([["branch"]]);

	events.close();
	await lix.close();
});

test("observe on lix_file emits follow-up when switching active version", async () => {
	const lix = await createInitializedLix();
	const branch = await lix.createVersion({ name: "observe-switch-file" });
	const path = "/observe-switch-file.txt";

	await lix.execute(
		"INSERT INTO lix_file_by_version (path, data, lixcol_version_id) VALUES (?1, ?2, ?3)",
		[path, new Uint8Array([1]), "global"],
	);
	await lix.execute(
		"UPDATE lix_file_by_version SET data = ?1 WHERE path = ?2 AND lixcol_version_id = ?3",
		[new Uint8Array([2]), path, branch.id],
	);
	await lix.switchVersion("global");

	const events = lix.observe({
		sql: "SELECT data FROM lix_file WHERE path = ?1",
		params: [path],
	});

	const initial = await events.next();
	expect(initial).toBeDefined();
	expect(initial!.rows.rows).toEqual([[new Uint8Array([1])]]);

	const nextPromise = events.next();
	await lix.switchVersion(branch.id);

	const followUp = await withTimeout(nextPromise, 1500);
	expect(followUp).not.toBe(TIMEOUT);
	if (followUp === TIMEOUT || followUp === undefined) {
		throw new Error("observe follow-up did not arrive after switching version");
	}
	expect(followUp.rows.rows).toEqual([[new Uint8Array([2])]]);

	events.close();
	await lix.close();
});

test("observe next resolves when closed while waiting", async () => {
	const lix = await createInitializedLix();
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
	const lix = await createInitializedLix();
	const key = "observe-recover-json";
	await lix.execute("INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)", [
		key,
		'{"value":"ok-0"}',
	]);

	const events = lix.observe({
		sql: "SELECT json_extract(value, '$.value') FROM lix_key_value WHERE key = ?1",
		params: [key],
	});

	const initial = await events.next();
	expect(initial).toBeDefined();
	expect(initial!.rows.rows.length).toBe(1);
	expect(initial!.rows.rows[0]![0]).toBe("ok-0");

	const failingNext = events.next().then(
		(value) => ({ ok: true as const, value }),
		(error) => ({ ok: false as const, error }),
	);
	await lix.execute("UPDATE lix_key_value SET value = ?2 WHERE key = ?1", [
		key,
		"{",
	]);

	const failingResult = await failingNext;
	expect(failingResult.ok).toBe(false);
	if (failingResult.ok) {
		throw new Error("observe query unexpectedly succeeded");
	}
	expect(String(failingResult.error)).toContain("LIX_ERROR_");

	await lix.execute("UPDATE lix_key_value SET value = ?2 WHERE key = ?1", [
		key,
		'{"value":"ok-1"}',
	]);

	const recoveredNext = events.next();
	const recovered = await withTimeout(recoveredNext, 1500);
	expect(recovered).not.toBe(TIMEOUT);
	if (recovered === TIMEOUT || recovered === undefined) {
		throw new Error("observe did not recover after transient query error");
	}
	expect(recovered.rows.rows.length).toBe(1);
	expect(recovered.rows.rows[0]![0]).toBe("ok-1");

	events.close();
	await lix.close();
});

const TIMEOUT = Symbol("timeout");

async function withTimeout<T>(
	promise: Promise<T>,
	timeoutMs: number,
): Promise<T | typeof TIMEOUT> {
	const timeoutPromise = new Promise<typeof TIMEOUT>((resolve) => {
		setTimeout(() => resolve(TIMEOUT), timeoutMs);
	});
	return Promise.race([promise, timeoutPromise]);
}
