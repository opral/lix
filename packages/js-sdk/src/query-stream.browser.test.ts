import { expect, test } from "vitest";

test("streams pages in browser WASM and cancels on break and close", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const sql = "SELECT value FROM generate_series(1, 5000)";
		const values: number[] = [];
		let pages = 0;
		for await (const page of lix.stream(sql, [], {
			pageBytes: 4_096,
			rowMode: "array",
		})) {
			pages++;
			expect(page.columns).toEqual([{ name: "value", type: "integer" }]);
			values.push(...page.rows.map(([value]) => value as number));
		}
		expect(values).toEqual(Array.from({ length: 5000 }, (_, index) => index + 1));
		expect(pages).toBeGreaterThan(1);

		for await (const page of lix.stream(sql, [], { pageBytes: 256 })) {
			expect(page.rows.length).toBeGreaterThan(0);
			break;
		}

		const open = lix.stream(sql, [], { pageBytes: 256 });
		expect((await open.next()).done).toBe(false);
		await lix.close();
		await expect(open.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	} finally {
		await lix.close();
	}
});
