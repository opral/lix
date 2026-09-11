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
	expect(read.commit).toBeUndefined();

	const batchBefore = await head();
	const batch = await lix.executeBatch([
		{ sql: "INSERT INTO lix_key_value (key, value) VALUES ('span-js-b', 'one')" },
		{ sql: "UPDATE lix_key_value SET value = 'two' WHERE key = 'span-js-b' RETURNING key" },
	]);
	expect(batch[0]?.commit).toEqual({ before: batchBefore, after: await head() });
	expect(batch[1]?.commit).toEqual(batch[0]?.commit);

	// Inside an explicit transaction the commit is the write.
	const transaction = await lix.beginTransaction();
	const staged = await transaction.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('span-js-tx', 'one')",
	);
	expect(staged.rowsAffected).toBe(1);
	expect(staged.commit).toBeUndefined();
	await transaction.commit();
	await lix.close();
});
