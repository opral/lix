import { expect, test } from "vitest";
import { openLix } from "./open-lix.js";
import { json, Value } from "./value.js";

async function seeded(rows = 3) {
	const lix = await openLix();
	await lix.execute(
		`INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))`,
		[
			JSON.stringify({
				$schema: "https://lix.dev/schema-v1.json",
				key: "api_item",
				columns: [
					{ name: "id", type: "int8", nullable: false },
					{ name: "label", type: "text", nullable: false },
				],
				primary_key: ["id"],
			}),
		],
	);
	await lix.executeMany(
		"INSERT INTO api_item (id, label) VALUES ($1, $2)",
		Array.from({ length: rows }, (_, id) => [id, `label ${id}`]),
	);
	return lix;
}

test("query returns rows, never writes, and caps buffered bytes", async () => {
	const lix = await seeded(50);
	try {
		expect(
			await lix.query<{ id: number }>("SELECT id FROM api_item WHERE id < $1 ORDER BY id", [3]),
		).toEqual([{ id: 0 }, { id: 1 }, { id: 2 }]);
		await expect(
			lix.query("INSERT INTO api_item (id, label) VALUES (100, 'x')"),
		).rejects.toMatchObject({ code: "LIX_ERROR_READ_ONLY" });
		await expect(
			lix.query("SELECT id, label FROM api_item", [], { maxBytes: 128 }),
		).rejects.toMatchObject({ code: "LIX_ERROR_RESULT_TOO_LARGE" });
		expect(
			await lix.query("SELECT id, label FROM api_item", [], { maxBytes: Infinity }),
		).toHaveLength(50);
		// Runtime functions are reads.
		expect(await lix.query("SELECT uuidv7() AS id")).toHaveLength(1);
	} finally {
		await lix.close();
	}
});

test("snapshots and transactions apply maxBytes", async () => {
	const lix = await seeded(50);
	const sql = "SELECT id, label FROM api_item";
	const tooLarge = { code: "LIX_ERROR_RESULT_TOO_LARGE" };
	try {
		await lix.read(async (snapshot) => {
			await expect(snapshot.query(sql, [], { maxBytes: 128 })).rejects.toMatchObject(tooLarge);
			expect(await snapshot.query(sql, [], { maxBytes: Infinity })).toHaveLength(50);
		});
		await lix.transaction(async (tx) => {
			await expect(tx.query(sql, [], { maxBytes: 128 })).rejects.toMatchObject(tooLarge);
			expect(await tx.query(sql, [], { maxBytes: Infinity })).toHaveLength(50);
		});
	} finally {
		await lix.close();
	}
});

test("queryOne reports absent and ambiguous results", async () => {
	const lix = await seeded();
	try {
		expect(await lix.queryOne("SELECT label FROM api_item WHERE id = 1")).toEqual({
			label: "label 1",
		});
		expect(await lix.queryOne("SELECT label FROM api_item WHERE id = 99")).toBeUndefined();
		await expect(lix.queryOne("SELECT label FROM api_item")).rejects.toMatchObject({
			code: "LIX_ERROR_NOT_UNIQUE",
		});
	} finally {
		await lix.close();
	}
});

test("execute runs reads and writes and returns rows plus rowsAffected", async () => {
	const lix = await seeded();
	try {
		const read = await lix.execute("SELECT id FROM api_item ORDER BY id");
		expect(read.rows).toEqual([{ id: 0 }, { id: 1 }, { id: 2 }]);
		expect(read.rowsAffected).toBe(0);
		expect(read.commit).toBeNull();
		const write = await lix.execute(
			"UPDATE api_item SET label = 'changed' WHERE id = $1 RETURNING id",
			[1],
		);
		expect(write.rows).toEqual([{ id: 1 }]);
		expect(write.rowsAffected).toBe(1);
		expect(write.commit).not.toBeNull();
		await expect(
			lix.execute("SELECT id, label FROM api_item", [], { maxBytes: 16 }),
		).rejects.toMatchObject({ code: "LIX_ERROR_RESULT_TOO_LARGE" });
	} finally {
		await lix.close();
	}
});

test("executeMany is atomic and rejects statements that return rows", async () => {
	const lix = await seeded();
	try {
		expect(
			await lix.executeMany("INSERT INTO api_item (id, label) VALUES ($1, $2)", [
				[10, "a"],
				[11, "b"],
			]),
		).toEqual({ rowsAffected: 2 });
		await expect(
			lix.executeMany("INSERT INTO api_item (id, label) VALUES ($1, $2)", [
				[20, "ok"],
				[10, "duplicate key"],
			]),
		).rejects.toBeTruthy();
		expect(await lix.queryOne("SELECT id FROM api_item WHERE id = 20")).toBeUndefined();
		await expect(lix.executeMany("SELECT $1", [[1]])).rejects.toMatchObject({
			code: "LIX_INVALID_PARAM",
		});
		expect(
			await lix.executeMany("INSERT INTO api_item (id, label) VALUES ($1, $2)", []),
		).toEqual({ rowsAffected: 0 });
	} finally {
		await lix.close();
	}
});

test("parameter errors name the call and give a fitting hint", async () => {
	const lix = await openLix();
	try {
		// A hole is a missing parameter, not a skipped one.
		// eslint-disable-next-line no-sparse-arrays
		await expect(lix.query("SELECT $1 AS a, $2 AS b", [1, , 3] as never)).rejects.toMatchObject({
			code: "LIX_INVALID_PARAM",
			details: { operation: "query", argument: "params[1]" },
		});
		await expect(lix.executeMany("SELECT $1", [[10n as never]])).rejects.toMatchObject({
			code: "LIX_INVALID_PARAM",
			details: { operation: "executeMany", paramSetIndex: 0 },
			hint: expect.stringContaining("Number(value)"),
		});
		await expect(lix.query("SELECT $1", [new ArrayBuffer(2) as never])).rejects.toMatchObject({
			message: expect.stringContaining("ArrayBuffer"),
			hint: expect.stringContaining("Uint8Array"),
		});
		const map = lix.query("SELECT $1", [new Map() as never]);
		await expect(map).rejects.toMatchObject({ message: "Map is not a valid SQL parameter" });
		await expect(map).rejects.not.toHaveProperty("hint");
		let deep: unknown = 1;
		for (let depth = 0; depth < 200_000; depth++) deep = [deep];
		expect(() => json(deep as never)).toThrow(
			expect.objectContaining({ code: "LIX_INVALID_PARAM" }),
		);
		const { runInNewContext } = await import("node:vm");
		const foreign = runInNewContext("new Uint8Array([7, 8])") as Uint8Array;
		expect(foreign instanceof Uint8Array).toBe(false);
		expect(await lix.queryOne("SELECT $1 AS b", [foreign])).toEqual({
			b: new Uint8Array([7, 8]),
		});
	} finally {
		await lix.close();
	}
});

test("parameters are guarded: JSON needs json(), other objects throw", async () => {
	const lix = await openLix();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["settings", json({ theme: "dark", sizes: [1, 2] })],
		);
		expect(
			await lix.queryOne("SELECT value FROM lix_key_value WHERE key = 'settings'"),
		).toEqual({ value: { theme: "dark", sizes: [1, 2] } });
		for (const bad of [{ a: 1 }, [1, 2], new Date(), undefined, Number.NaN, Infinity, 10n]) {
			await expect(
				lix.query("SELECT $1 AS v", [bad as never]),
			).rejects.toMatchObject({ code: "LIX_INVALID_PARAM" });
		}
		expect(await lix.queryOne("SELECT $1 AS v", [Value.integer(7)])).toEqual({ v: 7 });
		expect(
			await lix.queryOne("SELECT $1 AS b", [new Uint8Array([1, 2])]),
		).toEqual({ b: new Uint8Array([1, 2]) });
	} finally {
		await lix.close();
	}
});

test("duplicate result column names throw on object rows", async () => {
	const lix = await seeded();
	try {
		await expect(
			lix.query("SELECT a.id, b.id FROM api_item a JOIN api_item b ON a.id = b.id"),
		).rejects.toMatchObject({ code: "LIX_ERROR_AMBIGUOUS_COLUMN" });
	} finally {
		await lix.close();
	}
});

test("a snapshot reads one state across writes and closes its streams", async () => {
	const lix = await seeded();
	try {
		const snapshot = await lix.snapshot();
		await lix.execute("INSERT INTO api_item (id, label) VALUES (50, 'later')");
		const count = "SELECT count(*) AS n FROM api_item";
		expect(await snapshot.queryOne(count)).toEqual({ n: 3 });
		expect(await lix.queryOne(count)).toEqual({ n: 4 });
		const rows = await snapshot.stream("SELECT id FROM api_item ORDER BY id");
		const second = await snapshot.stream("SELECT id FROM api_item ORDER BY id DESC");
		expect((await second.pages().next()).value).toEqual([{ id: 2 }, { id: 1 }, { id: 0 }]);
		await snapshot.close();
		await expect(rows.pages().next()).rejects.toMatchObject({
			code: "LIX_ERROR_SNAPSHOT_CLOSED",
		});
		await expect(snapshot.query(count)).rejects.toMatchObject({
			code: "LIX_ERROR_SNAPSHOT_CLOSED",
		});
	} finally {
		await lix.close();
	}
});

test("read() scopes a snapshot to its callback", async () => {
	const lix = await seeded();
	try {
		let kept: { query(sql: string): Promise<unknown> } | undefined;
		const counts = await lix.read(async (snapshot) => {
			kept = snapshot;
			const before = await snapshot.queryOne<{ n: number }>(
				"SELECT count(*) AS n FROM api_item",
			);
			await lix.execute("INSERT INTO api_item (id, label) VALUES (60, 'x')");
			const after = await snapshot.queryOne<{ n: number }>(
				"SELECT count(*) AS n FROM api_item",
			);
			return [before!.n, after!.n];
		});
		expect(counts).toEqual([3, 3]);
		await expect(kept!.query("SELECT 1")).rejects.toMatchObject({
			code: "LIX_ERROR_SNAPSHOT_CLOSED",
		});
	} finally {
		await lix.close();
	}
});

test("transactions read their own writes and executeMany is all-or-nothing", async () => {
	const lix = await seeded();
	try {
		const { value } = await lix.transaction(async (tx) => {
			await tx.execute("INSERT INTO api_item (id, label) VALUES (70, 'tx')");
			const seen = await tx.queryOne("SELECT label FROM api_item WHERE id = 70");
			const streamed: unknown[] = [];
			for await (const row of await tx.stream("SELECT id FROM api_item WHERE id >= 70"))
				streamed.push(row);
			await expect(
				tx.executeMany("INSERT INTO api_item (id, label) VALUES ($1, $2)", [
					[71, "ok"],
					["not a number", "bad"],
				]),
			).rejects.toBeTruthy();
			expect(await tx.queryOne("SELECT id FROM api_item WHERE id = 71")).toBeUndefined();
			return { seen, streamed };
		});
		expect(value).toEqual({ seen: { label: "tx" }, streamed: [{ id: 70 }] });
		expect(await lix.queryOne("SELECT label FROM api_item WHERE id = 70")).toEqual({
			label: "tx",
		});
	} finally {
		await lix.close();
	}
});

test("aborting a transaction rolls it back", async () => {
	const lix = await seeded();
	try {
		const controller = new AbortController();
		await expect(
			lix.transaction(
				async (tx) => {
					await tx.execute("INSERT INTO api_item (id, label) VALUES (80, 'aborted')");
					controller.abort(new Error("stop"));
				},
				{ signal: controller.signal },
			),
		).rejects.toThrow("stop");
		expect(await lix.queryOne("SELECT id FROM api_item WHERE id = 80")).toBeUndefined();
	} finally {
		await lix.close();
	}
});

test("an aborted query rejects with the signal's reason", async () => {
	const lix = await seeded();
	try {
		const controller = new AbortController();
		controller.abort(new Error("cancelled"));
		await expect(
			lix.query("SELECT id FROM api_item", [], { signal: controller.signal }),
		).rejects.toThrow("cancelled");
	} finally {
		await lix.close();
	}
});

test("reads reject multi-statement SQL", async () => {
	const lix = await openLix();
	try {
		for (const read of [
			() => lix.query("SELECT 1; SELECT 2"),
			() => lix.queryOne("SELECT 1; SELECT 2"),
			() => lix.stream("SELECT 1; SELECT 2"),
		]) {
			await expect(read()).rejects.toMatchObject({ name: "LixError" });
		}
	} finally {
		await lix.close();
	}
});

test("concurrent snapshot and stream releases are never refused for backpressure", async () => {
	const lix = await openLix();
	try {
		const reads = await Promise.allSettled(
			Array.from({ length: 40 }, () =>
				lix.read((snapshot) => snapshot.queryOne("SELECT 1 AS n")),
			),
		);
		expect(reads.filter((read) => read.status === "rejected")).toEqual([]);
		const streams = await Promise.allSettled(
			Array.from({ length: 40 }, async () => {
				for await (const row of await lix.stream(
					"SELECT value FROM generate_series(1, 200)",
				)) {
					expect(row).toHaveProperty("value");
					break;
				}
			}),
		);
		expect(streams.filter((stream) => stream.status === "rejected")).toEqual([]);
	} finally {
		await lix.close();
	}
});

test("duplicate output columns reject before anything is written", async () => {
	const lix = await seeded();
	const ambiguous = { code: "LIX_ERROR_AMBIGUOUS_COLUMN" };
	try {
		await expect(
			lix.execute("INSERT INTO api_item (id, label) VALUES (90, 'dup') RETURNING id, id"),
		).rejects.toMatchObject(ambiguous);
		expect(await lix.queryOne("SELECT id FROM api_item WHERE id = 90")).toBeUndefined();
		await expect(
			lix.transaction((tx) =>
				tx.execute("INSERT INTO api_item (id, label) VALUES (91, 'dup') RETURNING id, id"),
			),
		).rejects.toMatchObject(ambiguous);
		expect(await lix.queryOne("SELECT id FROM api_item WHERE id = 91")).toBeUndefined();
		// A read through execute() wrote nothing, so it rejects too.
		await expect(
			lix.execute("SELECT a.id, b.id FROM api_item a JOIN api_item b ON a.id = b.id"),
		).rejects.toMatchObject(ambiguous);
		await expect(lix.query("SELECT id, id FROM api_item")).rejects.toMatchObject(ambiguous);
	} finally {
		await lix.close();
	}
});

test("a fan-out of 100 concurrent reads all answer", async () => {
	const lix = await openLix();
	try {
		const results = await Promise.all(
			Array.from({ length: 100 }, (_, n) => lix.queryOne<{ n: number }>("SELECT $1 AS n", [n])),
		);
		expect(results.map((row) => row?.n)).toEqual(Array.from({ length: 100 }, (_, n) => n));
	} finally {
		await lix.close();
	}
});
