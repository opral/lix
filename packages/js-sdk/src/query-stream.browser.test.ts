import { expect, test } from "vitest";

test("streams rows in browser WASM and closes on break and close", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const sql = "SELECT value FROM generate_series(1, 5000)";
		const values: number[] = [];
		let pages = 0;
		const stream = await lix.stream<{ value: number }>(sql);
		expect(stream.columns).toEqual(["value"]);
		for await (const page of stream.pages()) {
			pages++;
			values.push(...page.map((row) => row.value));
		}
		expect(values).toEqual(Array.from({ length: 5000 }, (_, index) => index + 1));
		expect(pages).toBeGreaterThan(1);

		for await (const row of await lix.stream(sql)) {
			expect(row).toHaveProperty("value");
			break;
		}

		await lix.transaction(async (tx) => {
			expect(await tx.queryOne("SELECT count(*) AS n FROM generate_series(1, 10)")).toEqual({ n: 10 });
		});

		const open = await lix.stream(sql);
		const pagesOfOpen = open.pages();
		expect((await pagesOfOpen.next()).done).toBe(false);
		await lix.close();
		await expect(pagesOfOpen.next()).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	} finally {
		await lix.close();
	}
});
