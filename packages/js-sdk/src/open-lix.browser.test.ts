import { expect, test } from "vitest";
import { registerMemoryStorageContract } from "../tests/memory-storage-contract.js";

registerMemoryStorageContract({
	name: "browser WASM",
	loadSdk: async () => await import("@lix-js/sdk"),
	operationTimeoutMs: 30_000,
	supportsPluginExecution: false,
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
	let resolveSpan!: (span: { attributes: Record<string, unknown> }) => void;
	const received = new Promise<{ attributes: Record<string, unknown> }>(
		(resolve) => {
			resolveSpan = resolve;
		},
	);
	const lix = await openLix({
		telemetry: {
			onSpan(span) {
				if (
					span.name === "lix.sql.query" &&
					span.attributes["db.query.text"] ===
						"SELECT ? AS value, ? AS number"
				) {
					resolveSpan(span);
				}
			},
		},
	});
	try {
		await lix.execute("SELECT 'private-value' AS value, 42 AS number");
		const span = await received;
		expect(span.attributes["db.query.text"]).toBe(
			"SELECT ? AS value, ? AS number",
		);
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
		expect(result.rows[0]?.get("value")).toBe(1);
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
		(await review.execute("SELECT 1 AS value")).rows[0]?.get("value"),
	).toBe(1);
	await review.close();
});

test("createCheckpoint returns the new active head through browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["checkpoint-test", "working"],
		);
		const before = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
		).rows[0]?.get("commit_id");

		const checkpoint = await lix.createCheckpoint();

		expect(checkpoint.commitId).not.toBe(before);
		expect(
			(
				await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
			).rows[0]?.get("commit_id"),
		).toBe(checkpoint.commitId);
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
			const checkpoint = await lix.createCheckpoint();
			expect(checkpoint.commitId).toEqual(expect.any(String));
		}

		expect((await lix.execute("SELECT 1 AS value")).rows[0]?.get("value")).toBe(
			1,
		);
	} finally {
		await lix.close();
	}
});

test("lix_restore moves the active branch to an ancestor through browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const initial = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
		).rows[0]?.get("commit_id") as string;
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["restore-test", "later"],
		);

		await lix.execute(
			"INSERT INTO lix_restore (commit_id) VALUES ($1) RETURNING commit_id",
			[initial],
		);

		expect(
			(
				await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
			).rows[0]?.get("commit_id"),
		).toBe(initial);
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
			.map((row) => row.toObject() as { path: string; kind: string })
			.filter((row) => !row.path.startsWith("/.lix/"));

		expect(rows).toEqual([
			{ path: "/README.md", kind: "file" },
			{ path: "/docs", kind: "directory" },
		]);
	} finally {
		await lix.close();
	}
});
