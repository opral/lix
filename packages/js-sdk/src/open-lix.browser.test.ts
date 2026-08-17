import { expect, test } from "vitest";
import { registerMemoryStorageContract } from "../tests/memory-storage-contract.js";

registerMemoryStorageContract({
	name: "browser WASM",
	loadSdk: async () => await import("@lix-js/sdk"),
	operationTimeoutMs: 30_000,
	supportsPluginExecution: false,
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

test("OpfsStorage persists a complete local Lix", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const { OpfsStorage } = await import("@lix-js/storage-opfs");
	const storage = new OpfsStorage({
		name: `lix-opfs-test:${crypto.randomUUID()}`,
	});
	const first = await openLix({ storage });
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["durable-opfs", { value: 42 }],
	);
	await first.close();

	const second = await openLix({ storage });
	try {
		expect(
			(
				await second.execute("SELECT value FROM lix_key_value WHERE key = $1", [
					"durable-opfs",
				])
			).rows[0]?.get("value"),
		).toEqual({ value: 42 });
	} finally {
		await second.close();
	}
});

test("OpfsStorage exclusively owns a name across handles", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const { OpfsStorage } = await import("@lix-js/storage-opfs");
	const name = `lix-opfs-owner-test:${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	await expect(openLix({ storage: new OpfsStorage({ name }) })).rejects.toThrow(
		"already open",
	);
	await first.close();
});

test("distinct OPFS repositories can open in parallel workers", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const { OpfsStorage } = await import("@lix-js/storage-opfs");
	const first = openLix({
		storage: new OpfsStorage({ name: `lix-opfs-parallel-a:${crypto.randomUUID()}` }),
	});
	const second = openLix({
		storage: new OpfsStorage({ name: `lix-opfs-parallel-b:${crypto.randomUUID()}` }),
	});
	const [left, right] = await Promise.all([first, second]);
	await Promise.all([left.close(), right.close()]);
});
