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

test("the native binding pulls byte-bounded pages, cancels, and ends at close", async () => {
	const binding = await openLixBinding({ kind: "memory" });
	try {
		await seed((sql, params) => binding.execute(sql, params as never));
		const sql = "SELECT id, n FROM native_stream_item ORDER BY id";
		const buffered = await binding.execute(sql, []);
		const stream = await binding.stream(sql, [], { pageBytes: 1_024 });
		const rows: unknown[] = [];
		let pages = 0;
		for (let page = await stream.next(); page; page = await stream.next()) {
			pages++;
			expect(page.columns).toEqual(buffered.columns);
			rows.push(...page.rows);
		}
		expect(rows).toEqual(buffered.rows);
		expect(pages).toBeGreaterThan(10);
		expect(await stream.next()).toBeNull();

		const cancelled = await binding.stream(sql, [], { pageBytes: 256 });
		expect(await cancelled.next()).toBeTruthy();
		await cancelled.cancel();
		expect(await cancelled.next()).toBeNull();

		const open = await binding.stream(sql, [], { pageBytes: 256 });
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
		for await (const page of lix.stream(
			"SELECT n FROM native_stream_item ORDER BY n",
			[],
			{ pageBytes: 512, rowMode: "array" },
		)) {
			seen.push(...page.rows.map(([n]) => n as number));
			if (seen.length >= 100) break;
		}
		expect(seen.slice(0, 100)).toEqual(Array.from({ length: 100 }, (_, n) => n));

		const open = lix.stream("SELECT id FROM native_stream_item");
		expect((await open.next()).done).toBe(false);
		await lix.close();
		await expect(open.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	} finally {
		await lix.close();
		rmSync(path, { recursive: true, force: true });
	}
});
