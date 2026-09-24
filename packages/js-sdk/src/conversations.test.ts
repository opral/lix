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

test("lix_conversation.target is a ROW_REF that detaches when its target is deleted", async () => {
	const lix = await openLix();
	try {
		const fileId = "01950000-0000-7000-8000-000000000f01";
		const conversation = "01950000-0000-7000-8000-000000000a03";
		await lix.execute(
			"INSERT INTO lix_file (id, path, content) VALUES ($1, '/target.txt', CAST('x' AS BYTEA))",
			[fileId],
		);
		const inserted = await lix.execute<{ target: string }>(
			"INSERT INTO lix_conversation (id, target) VALUES ($1, lix_row_ref('lix_file', NULL, $2)) RETURNING target",
			[conversation, fileId],
		);
		expect(inserted.columns).toEqual([{ name: "target", type: "row_ref" }]);
		const target = inserted.rows[0]!.target;
		expect(target).toMatch(/^lix_row_ref:v2:/);

		// A reference returned by one query is a plain string parameter for the next.
		const found = await lix.execute<{ id: string }>(
			"SELECT id FROM lix_conversation WHERE target = $1",
			[target],
		);
		expect(found.rows).toEqual([{ id: conversation }]);
		const discovered = await lix.execute(
			`SELECT data_type FROM information_schema.columns
			 WHERE table_name = 'lix_conversation' AND column_name = 'target'`,
		);
		expect(discovered.rows).toEqual([{ data_type: "ROW_REF" }]);

		await lix.execute("DELETE FROM lix_file WHERE id = $1", [fileId]);
		const detached = await lix.execute<{ id: string; target: string }>(
			`SELECT c.id, c.target FROM lix_conversation c
			 LEFT JOIN lix_file f ON c.target = lix_row_ref('lix_file', NULL, f.id)
			 WHERE c.target IS NOT NULL AND f.id IS NULL`,
		);
		expect(detached.rows).toEqual([{ id: conversation, target }]);

		await lix.execute(
			"UPDATE lix_conversation SET resolved = true, title = 'Detached' WHERE id = $1",
			[conversation],
		);
		await expect(
			lix.execute(
				"UPDATE lix_conversation SET target = lix_row_ref('lix_file', NULL, $2) WHERE id = $1",
				[conversation, "01950000-0000-7000-8000-000000000f02"],
			),
		).rejects.toThrow();
	} finally {
		await lix.close();
	}
});
