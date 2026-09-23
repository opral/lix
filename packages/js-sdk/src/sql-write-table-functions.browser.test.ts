import { expect, test } from "vitest";

test("uses native SQL table functions in browser writes", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const before = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS id")
		).rows[0]?.id as string;
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["wasm-diff-source", "source"],
		);
		const after = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS id")
		).rows[0]?.id as string;

		const diffRows = await lix.execute(
			"SELECT key FROM lix_diff($1, $2, $3) WHERE key = 'wasm-diff-source'",
			["lix_key_value", before, after],
		);
		expect(diffRows.rows[0]?.key).toBe("wasm-diff-source");

		const inserted = await lix.execute(
			`INSERT INTO lix_key_value (key, value)
				 SELECT 'wasm-diff-copy', 'copied'
				 FROM lix_diff('lix_key_value', $1, $2)
				 WHERE key = 'wasm-diff-source'
				 RETURNING key`,
			[before, after],
		);
		expect(inserted.rows[0]?.key).toBe("wasm-diff-copy");

		const nullRowRef = await lix.execute(
			"SELECT lix_row_ref('lix_key_value', NULL, NULL) AS row_ref",
		);
		expect(nullRowRef.rows[0]?.row_ref).toBeNull();
	} finally {
		await lix.close();
	}
});
