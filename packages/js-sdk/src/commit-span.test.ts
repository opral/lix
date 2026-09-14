import { expect, test } from "vitest";
import { openLix } from "./index.js";
test("writes return the commit span they published", async () => {
	const lix = await openLix();
	const head = async () =>
		String(
			(
				await lix.execute<{ commit_id: string }>(
					"SELECT lix_active_branch_commit_id() AS commit_id",
				)
			).rows[0]?.commit_id,
		);
	const before = await head();
	const written = await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('span-js', 'one')",
	);
	expect(written.commit).toEqual({ before, after: await head() });
	expect(written.commit?.after).not.toBe(before);

	const read = await lix.execute("SELECT 1 AS value");
	expect(read.commit).toBeNull();

	const batchBefore = await head();
	const batch = await lix.executeBatch([
		{ sql: "INSERT INTO lix_key_value (key, value) VALUES ('span-js-b', 'one')" },
		{ sql: "UPDATE lix_key_value SET value = 'two' WHERE key = 'span-js-b' RETURNING key" },
	]);
	expect(batch.commit).toEqual({ before: batchBefore, after: await head() });
	expect(batch.results).toHaveLength(2);
	for (const statement of batch.results) expect(statement).not.toHaveProperty("commit");

	// Inside an explicit transaction the commit is the write.
	const transaction = await lix.beginTransaction();
	const staged = await transaction.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('span-js-tx', 'one')",
	);
	expect(staged.rowsAffected).toBe(1);
	expect(staged).not.toHaveProperty("commit");
	const receipt = await transaction.commit();
	expect(receipt.commit).toEqual({ before: batch.commit?.after, after: await head() });
	const readOnly = await lix.beginTransaction();
	await readOnly.execute("SELECT 1");
	expect(await readOnly.commit()).toEqual({ commit: null });
	await lix.close();
});
