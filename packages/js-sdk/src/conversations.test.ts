import { expect, test } from "vitest";
import { openLix, type ResultColumn } from "./index.js";

type ConversationRow = {
	id: string;
	title: string | null;
	resolved: boolean;
};

const body = JSON.stringify({ _type: "zettel_doc", blocks: [] });

test("lix_conversation.resolved is a typed boolean that defaults to false", async () => {
	const lix = await openLix();
	try {
		const open = "01950000-0000-7000-8000-000000000a01";
		const done = "01950000-0000-7000-8000-000000000a02";
		await lix.execute("INSERT INTO lix_conversation (id, title) VALUES ($1, 'Open')", [open]);
		const transaction = await lix.beginTransaction();
		await transaction.execute("INSERT INTO lix_conversation (id, title) VALUES ($1, 'Done')", [
			done,
		]);
		await transaction.execute("UPDATE lix_conversation SET resolved = true WHERE id = $1", [done]);
		await transaction.execute(
			"INSERT INTO lix_comment (id, conversation_id, body) VALUES ($1, $2, $3::jsonb)",
			["01950000-0000-7000-8000-000000000b01", done, body],
		);
		await transaction.commit();

		const result = await lix.execute<ConversationRow>(
			"SELECT id, title, resolved FROM lix_conversation ORDER BY id",
		);
		expect(result.columns.find((column: ResultColumn) => column.name === "resolved")).toEqual({
			name: "resolved",
			type: "boolean",
		});
		expect(result.rows).toEqual([
			{ id: open, title: "Open", resolved: false },
			{ id: done, title: "Done", resolved: true },
		]);
		const resolved: boolean = result.rows[1]!.resolved;
		expect(resolved).toBe(true);

		await lix.execute("UPDATE lix_conversation SET resolved = $2 WHERE id = $1", [done, false]);
		const reopened = await lix.execute<{ open: number }>(
			"SELECT COUNT(*) AS open FROM lix_conversation WHERE resolved = false",
		);
		expect(reopened.rows).toEqual([{ open: 2 }]);

		const discovered = await lix.execute(
			`SELECT data_type, is_nullable, column_default
			 FROM information_schema.columns
			 WHERE table_name = 'lix_conversation' AND column_name = 'resolved'`,
		);
		expect(discovered.rows).toEqual([
			{ data_type: "BOOLEAN", is_nullable: "NO", column_default: "FALSE" },
		]);
	} finally {
		await lix.close();
	}
});
