import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openLix } from "@lix-js/sdk";
import { expect, test } from "vitest";
import { FilesystemStorage } from "./index.js";
import { openLixBinding } from "./native-binding.js";

const ROWS = 800;

async function seed(execute: (sql: string, params: unknown[]) => Promise<unknown>) {
	await execute(
		"INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
		[
			{
				kind: "text",
				value: JSON.stringify({
					$schema: "https://lix.dev/schema-v1.json",
					key: "native_stream_item",
					columns: [
						{ name: "id", type: "text", nullable: false },
						{ name: "n", type: "int8", nullable: false },
					],
					primary_key: ["id"],
				}),
			},
		],
	);
	for (let start = 0; start < ROWS; start += 200) {
		const values = Array.from(
			{ length: 200 },
			(_, index) => `($${index * 2 + 1}, $${index * 2 + 2})`,
		).join(", ");
		const params = Array.from({ length: 200 }, (_, index) => [
			{ kind: "text", value: `item-${String(start + index).padStart(5, "0")}` },
			{ kind: "integer", value: start + index },
		]).flat();
		await execute(`INSERT INTO native_stream_item (id, n) VALUES ${values}`, params);
	}
}

test("the native binding pulls pages, cancels, and ends at close", async () => {
	const binding = await openLixBinding({ kind: "memory" });
	try {
		await seed((sql, params) => binding.execute(sql, params as never));
		const sql = "SELECT id, n FROM native_stream_item ORDER BY id";
		const buffered = await binding.query(sql, []);
		const stream = await binding.stream(sql, []);
		expect(stream.columns).toEqual(["id", "n"]);
		const rows: unknown[] = [];
		let pages = 0;
		for (let page = await stream.next(); page; page = await stream.next()) {
			pages++;
			rows.push(...page.rows);
		}
		expect(rows).toEqual(buffered.rows);
		expect(pages).toBeGreaterThan(1);
		expect(await stream.next()).toBeNull();

		const cancelled = await binding.stream(sql, []);
		expect(await cancelled.next()).toBeTruthy();
		await cancelled.cancel();
		expect(await cancelled.next()).toBeNull();

		const snapshot = await binding.snapshot();
		await binding.execute("UPDATE native_stream_item SET n = -2 WHERE n = 4", []);
		const pinned = await snapshot.query(
			"SELECT n FROM native_stream_item WHERE id = 'item-00004'",
			[],
		);
		expect(pinned.rows).toEqual([[{ kind: "integer", value: 4 }]]);
		await snapshot.close();
		await expect(snapshot.query("SELECT 1", [])).rejects.toMatchObject({
			code: "LIX_ERROR_SNAPSHOT_CLOSED",
		});

		const open = await binding.stream(sql, []);
		expect(await open.next()).toBeTruthy();
		// An idle stream never blocks a write on the same handle.
		await binding.execute("UPDATE native_stream_item SET n = -1 WHERE n = 3", []);
		await binding.close();
		await expect(open.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	} finally {
		await binding.close();
	}
});

test("lix.stream reads a filesystem repository through the native runtime", async () => {
	const path = mkdtempSync(join(tmpdir(), "lix-native-stream-"));
	const lix = await openLix({ storage: new FilesystemStorage({ path }) });
	try {
		await seed((sql, params) =>
			lix.execute(
				sql,
				(params as { value: unknown }[]).map((param) => param.value as never),
			),
		);
		const seen: number[] = [];
		for await (const row of await lix.stream<{ n: number }>(
			"SELECT n FROM native_stream_item ORDER BY n",
		)) {
			seen.push(row.n);
			if (seen.length >= 100) break;
		}
		expect(seen).toEqual(Array.from({ length: 100 }, (_, n) => n));

		expect(
			await lix.read((snapshot) =>
				snapshot.queryOne("SELECT count(*) AS n FROM native_stream_item"),
			),
		).toEqual({ n: 800 });

		const open = (await lix.stream("SELECT id FROM native_stream_item")).pages();
		expect((await open.next()).done).toBe(false);
		await lix.close();
		await expect(open.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	} finally {
		await lix.close();
		rmSync(path, { recursive: true, force: true });
	}
});

// Minutes of engine work if nothing stops it.
const LONG_READ = "SELECT count(*) AS n FROM generate_series(1, 100000000000)";

async function within<T>(ms: number, pending: Promise<T>): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			pending,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
}

const started = () => new Promise((resolve) => setTimeout(resolve, 200));

test("aborting a native read stops its engine work", async () => {
	const path = mkdtempSync(join(tmpdir(), "lix-native-cancel-"));
	const lix = await openLix({ storage: new FilesystemStorage({ path }) });
	try {
		const controller = new AbortController();
		const reading = lix.query(LONG_READ, [], { signal: controller.signal });
		await started();
		controller.abort(new Error("stop reading"));
		await expect(within(3_000, reading)).rejects.toThrow("stop reading");
		// Reads on one handle run in order: this one only answers promptly if
		// the aborted read no longer runs.
		expect(await within(3_000, lix.queryOne("SELECT 1 AS n"))).toEqual({ n: 1 });

		const snapshot = await lix.snapshot();
		const pinned = snapshot.query(LONG_READ);
		// Settles while close() is awaited; observed below.
		pinned.catch(() => undefined);
		await started();
		await within(3_000, snapshot.close());
		await expect(within(3_000, pinned)).rejects.toMatchObject({
			code: "LIX_ERROR_SNAPSHOT_CLOSED",
		});

		const transaction = new AbortController();
		const running = lix.transaction((tx) => tx.query(LONG_READ), {
			signal: transaction.signal,
		});
		await started();
		transaction.abort(new Error("stop transaction"));
		await expect(within(3_000, running)).rejects.toThrow("stop transaction");
		expect(await within(3_000, lix.queryOne("SELECT 2 AS n"))).toEqual({ n: 2 });
	} finally {
		await lix.close();
		rmSync(path, { recursive: true, force: true });
	}
});

test("a native snapshot answers every read sent right after close()", async () => {
	const binding = await openLixBinding({ kind: "memory" });
	try {
		for (let attempt = 0; attempt < 40; attempt++) {
			const snapshot = await binding.snapshot();
			const closing = snapshot.close();
			// Sent while the snapshot's actor is closing: each must settle.
			const reads = [
				snapshot.query("SELECT 1", []),
				snapshot.stream("SELECT 1", []),
			];
			await within(3_000, closing);
			for (const read of reads) {
				await expect(within(3_000, read)).rejects.toMatchObject({
					code: "LIX_ERROR_SNAPSHOT_CLOSED",
				});
			}
			await expect(within(3_000, snapshot.query("SELECT 1", []))).rejects.toMatchObject({
				code: "LIX_ERROR_SNAPSHOT_CLOSED",
			});
		}
	} finally {
		await binding.close();
	}
});
