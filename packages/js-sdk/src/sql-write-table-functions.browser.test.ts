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

		const changeInserted = await lix.execute(
			`INSERT INTO lix_key_value (key, value)
				 SELECT 'wasm-change-copy-' || id, schema_key
				 FROM lix_change
				 WHERE schema_key = 'lix_key_value'
				 ORDER BY created_at DESC
				 LIMIT 1
				 RETURNING key`,
		);
		expect(String(changeInserted.rows[0]?.key)).toMatch(
			/^wasm-change-copy-/,
		);

		const beforeUpdateCount = Number(
			(
				await lix.execute(
					"SELECT COUNT(*) AS n FROM lix_change WHERE schema_key = 'lix_key_value'",
				)
			).rows[0]?.n,
		);
		const returnedSubquery = await lix.execute(
			`UPDATE lix_key_value SET value = 'updated-in-wasm'
				 WHERE key = 'wasm-diff-source'
				 RETURNING key,
					 (SELECT COUNT(*) FROM lix_change
					  WHERE schema_key = 'lix_key_value') AS visible_changes`,
		);
		expect(returnedSubquery.rows[0]?.key).toBe("wasm-diff-source");
		expect(Number(returnedSubquery.rows[0]?.visible_changes)).toBe(
			beforeUpdateCount,
		);

		const nullRowRef = await lix.execute(
			"SELECT lix_row_ref('lix_key_value', NULL, NULL) AS row_ref",
		);
		expect(nullRowRef.rows[0]?.row_ref).toBeNull();
	} finally {
		await lix.close();
	}
});
