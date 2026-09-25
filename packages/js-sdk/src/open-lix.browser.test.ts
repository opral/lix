import { expect, test } from "vitest";
import { registerMemoryStorageContract } from "../tests/memory-storage-contract.js";

async function loadPluginTestArchives() {
	return await Promise.all(
		(["plugin_csv", "plugin_markdown"] as const).map(async (key) => {
			const response = await fetch(
				new URL(`../../lix/tests/fixtures/plugin-api/v2/${key}.lixplugin`, import.meta.url),
			);
			if (!response.ok)
				throw new Error(`Could not load frozen plugin archive: ${response.status}`);
			return {
				key,
				fileName: `${key}.lixplugin`,
				archiveBytes: new Uint8Array(await response.arrayBuffer()),
			};
		}),
	);
}

registerMemoryStorageContract({
	name: "browser WASM",
	loadSdk: async () => await import("@lix-js/sdk"),
	loadPluginArchives: loadPluginTestArchives,
	operationTimeoutMs: 30_000,
});

test("exports and restores snapshot streams in browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const source = await openLix();
	await source.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('browser-snapshot', 'complete')",
	);
	const restored = await openLix.fromSnapshot(source.exportSnapshot());
	try {
		const rows = await restored.execute(
			"SELECT value FROM lix_key_value WHERE key = 'browser-snapshot'",
		);
		expect(rows.rows).toHaveLength(1);
	} finally {
		await restored.close();
		await source.close();
	}
});

test("browser snapshot export is chunked and cancelable through the worker", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	let state = 0x8765_4321;
	let payload = "";
	for (let index = 0; index < 160 * 1024; index++) {
		state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
		payload += String.fromCharCode(33 + (state % 90));
	}
	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["browser-large-snapshot", payload],
	);

	const reader = lix.exportSnapshot().getReader();
	let chunks = 0;
	while (true) {
		const result = await reader.read();
		if (result.done) break;
		expect(result.value.byteLength).toBeLessThanOrEqual(64 * 1024);
		chunks += 1;
	}
	expect(chunks).toBeGreaterThan(1);
	const restored = await openLix.fromSnapshot(lix.exportSnapshot());
	try {
		expect(
			(
				await restored.execute(
					"SELECT value FROM lix_key_value WHERE key = 'browser-large-snapshot'",
				)
			).rows,
		).toHaveLength(1);
	} finally {
		await restored.close();
	}

	const canceled = lix.exportSnapshot().getReader();
	expect((await canceled.read()).done).toBe(false);
	await canceled.cancel();
	await lix.close();
});

test("browser close cancels an abandoned started snapshot export", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	const reader = lix.exportSnapshot().getReader();
	expect((await reader.read()).done).toBe(false);
	await lix.close();
});

test("forwards opt-in SQL telemetry from browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	let resolveRequest!: (request: Uint8Array) => void;
	const received = new Promise<Uint8Array>((resolve) => {
		resolveRequest = resolve;
	});
	const lix = await openLix({
		telemetry: {
			onExport(request) {
				if (request.byteLength > 0) resolveRequest(request);
			},
		},
	});
	try {
		await lix.execute("SELECT 'private-value' AS value, 42 AS number");
		expect(await received).toBeInstanceOf(Uint8Array);
	} finally {
		await lix.close();
	}
});

test("loads and executes the engine outside the browser main thread", async () => {
	const wasm = WebAssembly as unknown as Record<
		string,
		(...args: unknown[]) => unknown
	>;
	const methodNames = [
		"compile",
		"compileStreaming",
		"instantiate",
		"instantiateStreaming",
	] as const;
	const originals = new Map<string, (...args: unknown[]) => unknown>();
	let mainThreadCalls = 0;
	for (const name of methodNames) {
		const original = wasm[name];
		if (!original) continue;
		originals.set(name, original);
		wasm[name] = (...args: unknown[]) => {
			mainThreadCalls += 1;
			return original(...args);
		};
	}

	try {
		const { openLix } = await import("@lix-js/sdk");
		const lix = await openLix();
		const result = await lix.execute("SELECT 1 AS value");
		expect(result.rows[0]?.value).toBe(1);
		await lix.close();
		expect(mainThreadCalls).toBe(0);
	} finally {
		for (const [name, original] of originals) wasm[name] = original;
	}
});

test("keeps browser worker sessions independent", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const main = await openLix();
	const mainBranchId = await main.activeBranchId();
	const draft = await main.createBranch({ name: "Browser draft" });
	const review = await main.openAnotherSession({ branchId: draft.id });

	expect(await main.activeBranchId()).toBe(mainBranchId);
	expect(await review.activeBranchId()).toBe(draft.id);
	await main.close();
	expect(
		(await review.execute("SELECT 1 AS value")).rows[0]?.value,
	).toBe(1);
	await review.close();
});

test("checkpoint SQL returns the new active head through browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["checkpoint-test", "working"],
		);
		const before = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
		).rows[0]?.commit_id;

		const checkpoint = await lix.execute(
			"SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
		);
		const checkpointId = checkpoint.rows[0]?.commit_id;

		expect(checkpointId).not.toBe(before);
		expect(
			(
				await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
			).rows[0]?.commit_id,
		).toBe(checkpointId);
	} finally {
		await lix.close();
	}
});

test("checkpoint GC starts without requiring a browser Tokio runtime", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		// The first checkpoint establishes the recovery boundary. Sixteen more
		// non-empty intervals reach the fresh-repository GC threshold
		// (16 intervals * yield 1 * denominator 4 = inventory floor 64).
		for (let sequence = 0; sequence < 17; sequence += 1) {
			await lix.execute(
				`INSERT INTO lix_key_value (key, value)
				 VALUES ($1, $2)
				 ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
				["checkpoint-gc-browser-test", sequence],
			);
			const checkpoint = await lix.execute(
				"SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
			);
			expect(checkpoint.rows[0]?.commit_id).toEqual(expect.any(String));
		}

		expect((await lix.execute("SELECT 1 AS value")).rows[0]?.value).toBe(
			1,
		);
	} finally {
		await lix.close();
	}
});

test("lix_restore creates a new commit from an ancestor through browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const initial = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
		).rows[0]?.commit_id as string;
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["restore-test", "later"],
		);

		const restored = await lix.execute("SELECT commit_id FROM lix_restore($1)", [
			initial,
		]);
		const restoredCommit = restored.rows[0]?.commit_id;

		expect(
			(
				await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
			).rows[0]?.commit_id,
		).toBe(restoredCommit);
		expect(restoredCommit).toEqual(expect.any(String));
		expect(restoredCommit).not.toBe(initial);
		expect(
			(await lix.execute("SELECT * FROM lix_key_value WHERE key = $1", [
				"restore-test",
			])).rows,
		).toHaveLength(0);
	} finally {
		await lix.close();
	}
});

test("executes a globally ordered union plan in browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute("INSERT INTO lix_directory (path) VALUES ($1)", [
			"/docs",
		]);
		await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
			"/README.md",
			new Uint8Array(),
		]);

		const result = await lix.execute(`
			SELECT path, 'directory' AS kind FROM lix_directory
			UNION ALL
			SELECT path, 'file' AS kind FROM lix_file
			ORDER BY path ASC
		`);
		const rows = result.rows
			.map((row) => row as { path: string; kind: string })
			.filter((row) => row.path !== "/.lix" && !row.path.startsWith("/.lix/"));

		expect(rows).toEqual([
			{ path: "/README.md", kind: "file" },
			{ path: "/docs", kind: "directory" },
		]);
	} finally {
		await lix.close();
	}
});

test("WASM child sessions route SQL telemetry independently and nested sessions inherit", async () => {
 const { openLixBinding } = await import("./binding.browser.js");
 const rootSpans: Uint8Array[] = [];
 const childSpans: Uint8Array[] = [];
 const root = await openLixBinding({kind:"memory"}, request => rootSpans.push(request));
 const child = await root.openAnotherSession({}, request => childSpans.push(request));
 const nested = await child.openAnotherSession({});
 try {
  rootSpans.length=0; childSpans.length=0;
  child.setTelemetryParent({traceparent:"00-11111111111111111111111111111111-1111111111111111-01"});
  nested.setTelemetryParent({traceparent:"00-22222222222222222222222222222222-2222222222222222-01"});
  await child.execute("SELECT 41 AS child_value", []);
  await nested.execute("SELECT 42 AS nested_value", []);
  expect(childSpans.length).toBeGreaterThan(0);
  expect(childSpans.every(request => request instanceof Uint8Array)).toBe(true);
  expect(rootSpans).toEqual([]);
  childSpans.length=0;
  await root.execute("SELECT 43 AS root_value", []);
  expect(rootSpans.length).toBeGreaterThan(0);
  expect(childSpans).toEqual([]);
 } finally { await nested.close(); await child.close(); await root.close(); }
});
