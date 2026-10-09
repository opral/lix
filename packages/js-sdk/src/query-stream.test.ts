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

function fakeLix(next: QueryStreamBinding["next"]) {
	const cancel = vi.fn(async () => {});
	const stream = vi.fn(async () => ({ next, cancel }));
	const close = vi.fn(async () => {});
	const lix = new Lix({
		stream,
		close,
		openReport: () => undefined,
	} as unknown as LixBinding);
	return { lix, stream, cancel, close };
}

test("pages are pulled lazily, in order, and the stream ends at null", async () => {
	const pages = [page([1, 2]), page([3])];
	const f = fakeLix(vi.fn(async () => pages.shift() ?? null));
	const iterator = f.lix.stream("SELECT n FROM t", [], { pageBytes: 64 });
	expect(f.stream).not.toHaveBeenCalled();
	const first = iterator.next();
	const second = iterator.next();
	expect(await first).toEqual({
		done: false,
		value: { columns: [{ name: "n", type: "integer" }], rows: [{ n: 1 }, { n: 2 }] },
	});
	expect(await second).toEqual({
		done: false,
		value: { columns: [{ name: "n", type: "integer" }], rows: [{ n: 3 }] },
	});
	expect(await iterator.next()).toEqual({ done: true, value: undefined });
	expect(f.stream).toHaveBeenCalledWith("SELECT n FROM t", [], { pageBytes: 64 });
	await f.lix.close();
});

test("break cancels the binding stream once", async () => {
	const f = fakeLix(vi.fn(async () => page([1])));
	for await (const result of f.lix.stream("SELECT n FROM t", [], { rowMode: "array" })) {
		expect(result.rows).toEqual([[1]]);
		break;
	}
	expect(f.cancel).toHaveBeenCalledOnce();
	await f.lix.close();
	expect(f.cancel).toHaveBeenCalledOnce();
});

test.each(["abort", "lix-close"])(
	"%s settles a pending pull that the binding never answers",
	async (action) => {
		const next = vi.fn(() => new Promise<never>(() => {}));
		const f = fakeLix(next);
		const controller = new AbortController();
		const iterator = f.lix.stream("SELECT n FROM t", [], {
			signal: controller.signal,
		});
		const pending = iterator.next();
		await vi.waitFor(() => expect(next).toHaveBeenCalledOnce());
		if (action === "abort") controller.abort(new Error("stop"));
		else await f.lix.close();
		const expected =
			action === "abort"
				? { message: "stop" }
				: { code: "LIX_ERROR_CLOSED" };
		await expect(pending).rejects.toMatchObject(expected);
		await expect(iterator.next()).rejects.toMatchObject(expected);
		expect(f.cancel).toHaveBeenCalledOnce();
		await f.lix.close();
	},
);

test("invalid options are rejected before reaching the binding", () => {
	const f = fakeLix(vi.fn(async () => null));
	expect(() => f.lix.stream("SELECT 1", [], { pageBytes: 0 })).toThrowError(
		/options.pageBytes/,
	);
	expect(() =>
		f.lix.stream("SELECT 1", [], { rowMode: "rows" as "array" }),
	).toThrowError(/options.rowMode/);
	expect(f.stream).not.toHaveBeenCalled();
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
	for (let start = 0; start < rows; start += 200) {
		const count = Math.min(200, rows - start);
		const values = Array.from(
			{ length: count },
			(_, index) => `($${index * 3 + 1}, $${index * 3 + 2}, $${index * 3 + 3})`,
		).join(", ");
		const params = Array.from({ length: count }, (_, index) => {
			const n = start + index;
			return [`item-${String(n).padStart(6, "0")}`, n, `label ${n}`];
		}).flat();
		await lix.execute(`INSERT INTO stream_item (id, n, label) VALUES ${values}`, params);
	}
	return lix;
}

test("streams a real query page by page with the same rows as execute", async () => {
	const lix = await seeded(1_000);
	try {
		const sql = "SELECT id, n, label FROM stream_item ORDER BY id";
		const buffered = await lix.execute(sql);
		const rows: unknown[] = [];
		let pages = 0;
		for await (const result of lix.stream(sql, [], { pageBytes: 2_048 })) {
			pages++;
			expect(result.columns).toEqual(buffered.columns);
			expect(result.rows.length).toBeGreaterThan(0);
			rows.push(...result.rows);
		}
		expect(rows).toEqual(buffered.rows);
		expect(pages).toBeGreaterThan(10);

		const arrays: unknown[] = [];
		for await (const result of lix.stream(
			"SELECT n FROM stream_item WHERE n < $1 ORDER BY n",
			[5],
			{ rowMode: "array" },
		)) {
			arrays.push(...result.rows);
		}
		expect(arrays).toEqual([[0], [1], [2], [3], [4]]);

		await expect(
			lix.stream("DELETE FROM stream_item").next(),
		).rejects.toMatchObject({ code: "LIX_ERROR_READ_ONLY" });
	} finally {
		await lix.close();
	}
});

test("breaking early and closing with an open stream release it", async () => {
	const lix = await seeded(600);
	const sql = "SELECT id FROM stream_item ORDER BY id";
	for await (const result of lix.stream(sql, [], { pageBytes: 256 })) {
		expect(result.rows.length).toBeGreaterThan(0);
		break;
	}
	// The broken stream no longer pins anything: writes and new streams work.
	await lix.execute("UPDATE stream_item SET label = 'changed' WHERE n = 1");

	const open = lix.stream(sql, [], { pageBytes: 256 });
	const first = await open.next();
	expect(first.done).toBe(false);
	// An idle open stream does not block writes on the same handle.
	await lix.execute("UPDATE stream_item SET label = 'again' WHERE n = 2");
	await lix.close();
	await expect(open.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	expect(() => lix.stream(sql)).toThrowError(/closed/);
});

test("a result larger than the buffered read budget streams completely", async () => {
	const lix = await openLix();
	try {
		const rows = 1_000_100;
		const sql = `SELECT value FROM generate_series(1, ${rows})`;
		await expect(lix.execute(sql)).rejects.toMatchObject({
			code: "LIX_READ_RESOURCE_EXHAUSTED",
		});
		let seen = 0;
		let last = 0;
		let pages = 0;
		for await (const result of lix.stream(sql, [], { rowMode: "array" })) {
			pages++;
			for (const [value] of result.rows as [number][]) {
				expect(value).toBe(last + 1);
				last = value;
				seen++;
			}
		}
		expect(seen).toBe(rows);
		expect(pages).toBeGreaterThan(1);
	} finally {
		await lix.close();
	}
}, 120_000);
