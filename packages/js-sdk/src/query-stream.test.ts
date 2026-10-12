import { expect, test, vi } from "vitest";
import { Lix } from "./lix.js";
import { openLix } from "./open-lix.js";
import type {
	BindingExecuteResult,
	LixBinding,
	QueryStreamBinding,
} from "./binding-types.js";

function page(values: number[]): BindingExecuteResult {
	return {
		columns: [{ name: "n", type: "integer" }],
		rows: values.map((value) => [{ kind: "integer", value }]),
		rowsAffected: 0,
		notices: [],
	};
}

function fakeLix(
	next: QueryStreamBinding["next"],
	columns: string[] = ["n"],
) {
	const cancel = vi.fn(async () => {});
	const stream = vi.fn(async () => ({ columns, next, cancel }));
	const close = vi.fn(async () => {});
	const lix = new Lix({
		stream,
		close,
		openReport: () => undefined,
	} as unknown as LixBinding);
	return { lix, stream, cancel, close };
}

test("stream opens eagerly, then rows and pages share one cursor in order", async () => {
	const pages = [page([1, 2]), page([3]), page([4, 5])];
	const f = fakeLix(vi.fn(async () => pages.shift() ?? null));
	const rows = await f.lix.stream("SELECT n FROM t", [], { maxBytes: 64 });
	expect(f.stream).toHaveBeenCalledWith("SELECT n FROM t", [], { maxBytes: 64 });
	expect(rows.columns).toEqual(["n"]);
	const pageIterator = rows.pages();
	expect(await pageIterator.next()).toEqual({ done: false, value: [{ n: 1 }, { n: 2 }] });
	const seen: unknown[] = [];
	for await (const row of rows) seen.push(row);
	expect(seen).toEqual([{ n: 3 }, { n: 4 }, { n: 5 }]);
	expect(await pageIterator.next()).toEqual({ done: true, value: undefined });
	await f.lix.close();
});

test("break, return() and close() release the binding stream once", async () => {
	const f = fakeLix(vi.fn(async () => page([1])));
	const rows = await f.lix.stream("SELECT n FROM t");
	for await (const row of rows) {
		expect(row).toEqual({ n: 1 });
		break;
	}
	expect(f.cancel).toHaveBeenCalledOnce();
	await rows.close();
	await rows.close();
	await f.lix.close();
	expect(f.cancel).toHaveBeenCalledOnce();
});

test("await using closes the stream", async () => {
	const f = fakeLix(vi.fn(async () => page([1])));
	{
		await using rows = await f.lix.stream("SELECT n FROM t");
		expect(rows.columns).toEqual(["n"]);
	}
	expect(f.cancel).toHaveBeenCalledOnce();
	await f.lix.close();
});

test("an error is latched: later pulls reject with it", async () => {
	const failure = Object.assign(new Error("boom"), { code: "LIX_BOOM" });
	const next = vi.fn().mockResolvedValueOnce(page([1])).mockRejectedValueOnce(failure);
	const f = fakeLix(next);
	const rows = await f.lix.stream("SELECT n FROM t");
	const pages = rows.pages();
	await pages.next();
	await expect(pages.next()).rejects.toBe(failure);
	await expect(pages.next()).rejects.toBe(failure);
	expect(f.cancel).toHaveBeenCalledOnce();
	await f.lix.close();
});

test.each(["abort", "lix-close"])(
	"%s settles a pending pull that the binding never answers",
	async (action) => {
		const next = vi.fn(() => new Promise<never>(() => {}));
		const f = fakeLix(next);
		const controller = new AbortController();
		const rows = await f.lix.stream("SELECT n FROM t", [], {
			signal: controller.signal,
		});
		const pages = rows.pages();
		const pending = pages.next();
		await vi.waitFor(() => expect(next).toHaveBeenCalledOnce());
		if (action === "abort") controller.abort(new Error("stop"));
		else await f.lix.close();
		const expected =
			action === "abort" ? { message: "stop" } : { code: "LIX_ERROR_CLOSED" };
		await expect(pending).rejects.toMatchObject(expected);
		await expect(pages.next()).rejects.toMatchObject(expected);
		expect(f.cancel).toHaveBeenCalledOnce();
		await f.lix.close();
	},
);

test("duplicate column names reject the stream and release it", async () => {
	const f = fakeLix(vi.fn(async () => null), ["id", "id"]);
	await expect(f.lix.stream("SELECT a.id, b.id FROM t")).rejects.toMatchObject({
		code: "LIX_AMBIGUOUS_COLUMN",
	});
	expect(f.cancel).toHaveBeenCalledOnce();
	await f.lix.close();
});

test("invalid options are rejected before reaching the binding", async () => {
	const f = fakeLix(vi.fn(async () => null));
	await expect(f.lix.stream("SELECT 1", [], { maxBytes: 0 })).rejects.toThrowError(
		/options.maxBytes/,
	);
	await expect(f.lix.stream("SELECT 1", [], { maxBytes: 1.5 })).rejects.toThrowError(
		/options.maxBytes/,
	);
	expect(f.stream).not.toHaveBeenCalled();
	await f.lix.close();
});

async function seeded(rows: number) {
	const lix = await openLix();
	await lix.execute(
		`INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))`,
		[
			JSON.stringify({
				$schema: "https://lix.dev/schema-v1.json",
				key: "stream_item",
				columns: [
					{ name: "id", type: "text", nullable: false },
					{ name: "n", type: "int8", nullable: false },
					{ name: "label", type: "text", nullable: false },
				],
				primary_key: ["id"],
			}),
		],
	);
	const sets = Array.from({ length: rows }, (_, n) => [
		`item-${String(n).padStart(6, "0")}`,
		n,
		`label ${n}`,
	]);
	await lix.executeMany(
		"INSERT INTO stream_item (id, n, label) VALUES ($1, $2, $3)",
		sets,
	);
	return lix;
}

test("streams a real query with the same rows as query()", async () => {
	const lix = await seeded(1_000);
	try {
		const sql = "SELECT id, n, label FROM stream_item ORDER BY id";
		const buffered = await lix.query(sql);
		const rows = await lix.stream(sql);
		expect(rows.columns).toEqual(["id", "n", "label"]);
		let pages = 0;
		const streamed: unknown[] = [];
		for await (const page of rows.pages()) {
			pages++;
			expect(page.length).toBeGreaterThan(0);
			streamed.push(...page);
		}
		expect(streamed).toEqual(buffered);
		expect(pages).toBeGreaterThan(1);

		await expect(lix.stream("DELETE FROM stream_item")).rejects.toMatchObject({
			code: "LIX_ERROR_READ_ONLY",
		});
	} finally {
		await lix.close();
	}
});

test("breaking early and closing with an open stream release it", async () => {
	const lix = await seeded(600);
	const sql = "SELECT id FROM stream_item ORDER BY id";
	for await (const row of await lix.stream(sql)) {
		expect(row).toHaveProperty("id");
		break;
	}
	// The broken stream no longer pins anything: writes and new streams work.
	await lix.execute("UPDATE stream_item SET label = 'changed' WHERE n = 1");

	const open = await lix.stream(sql);
	const pages = open.pages();
	expect((await pages.next()).done).toBe(false);
	// An idle open stream does not block writes on the same handle.
	await lix.execute("UPDATE stream_item SET label = 'again' WHERE n = 2");
	await lix.close();
	await expect(pages.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	await expect(lix.stream(sql)).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
});

test("a result over the buffered cap streams completely", async () => {
	const lix = await openLix();
	try {
		const rows = 200_000;
		const sql = `SELECT value FROM generate_series(1, ${rows})`;
		await expect(lix.query(sql, [], { maxBytes: 1024 })).rejects.toMatchObject({
			code: "LIX_RESULT_TOO_LARGE",
		});
		let seen = 0;
		let last = 0;
		for await (const row of await lix.stream<{ value: number }>(sql)) {
			expect(row.value).toBe(last + 1);
			last = row.value;
			seen++;
		}
		expect(seen).toBe(rows);
	} finally {
		await lix.close();
	}
}, 120_000);
