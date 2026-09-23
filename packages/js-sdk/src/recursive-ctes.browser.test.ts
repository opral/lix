import { expect, test } from "vitest";

test("executes a parameterized recursive CTE in browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["recursive-browser-seed", "a"],
		);
		const result = await lix.execute(
			`WITH RECURSIVE walk(depth, value) AS (
				SELECT 0, 'a'::TEXT FROM lix_key_value WHERE key = $1
				UNION ALL
				SELECT depth + 1, value || 'x' FROM walk WHERE depth < 2
			)
			SELECT depth, value FROM walk ORDER BY depth`,
			["recursive-browser-seed"],
		);
		expect(result.rows.map((row) => (row as { depth: number }).depth)).toEqual([
			0,
			1,
			2,
		]);
		expect(result.rows.map((row) => (row as { value: string }).value)).toEqual([
			"a",
			"ax",
			"axx",
		]);
	} finally {
		await lix.close();
	}
});
