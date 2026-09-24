import { expect, test } from "vitest";
import { openLix } from "./index.js";

test("a described checkpoint has one durable commit comment and retry is idempotent", async () => {
	const lix = await openLix();
	try {
		await lix.execute("INSERT INTO lix_key_value (key, value) VALUES ($1, $2)", [
			"checkpoint-description-test",
			"ready",
		]);
		const description = "Validated imports before storage; old imports remain readable.";
		const { commitId } = await lix.createCheckpoint({ description });
		await lix.describeCheckpoint({ commitId, description });
		const corrected = "Validated imports before storage; old imports remain readable and retry safely.";
		await lix.describeCheckpoint({ commitId, description: corrected });

		const result = await lix.execute<{ body: { blocks: Array<{ children: Array<{ text: string }> }> } }>(
			`SELECT note.body
			 FROM lix_conversation AS thread
			 JOIN lix_comment AS note
			   ON note.conversation_id = thread.id
			  AND note.lixcol_global = thread.lixcol_global
			 WHERE thread.target = lix_row_ref('lix_commit', NULL, $1)
			   AND thread.lixcol_global = true`,
			[commitId],
		);
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.body.blocks[0]?.children[0]?.text).toBe(corrected);
	} finally {
		await lix.close();
	}
});

test("blank checkpoint descriptions fail before a checkpoint is created", async () => {
	const lix = await openLix();
	try {
		await expect(lix.createCheckpoint({ description: "  " })).rejects.toThrow(
			"Checkpoint description must not be blank",
		);
		const result = await lix.execute<{ count: number }>(
			"SELECT COUNT(*) AS count FROM lix_log() WHERE is_checkpoint",
		);
		expect(result.rows[0]?.count).toBe(0);
	} finally {
		await lix.close();
	}
});
