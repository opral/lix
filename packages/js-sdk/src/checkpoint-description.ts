/** A plain-text checkpoint note in the minimal valid Zettel document shape. */
export function checkpointDescriptionDocument(description: string) {
	const text = description.trim();
	if (!text) throw new TypeError("Checkpoint description must not be blank");
	return {
		_type: "zettel_doc" as const,
		blocks: [
			{
				_type: "zettel_block" as const,
				_key: "checkpoint-paragraph",
				style: "normal" as const,
				markDefs: [],
				children: [
					{
						_type: "zettel_span" as const,
						_key: "checkpoint-text",
						text,
						marks: [],
					},
				],
			},
		],
	};
}

/** The same commit ID identifies its one opening conversation and comment. */
export function checkpointDescriptionStatements(commitId: string, description: string) {
	const body = JSON.stringify(checkpointDescriptionDocument(description));
	return [
		{
			sql: `INSERT INTO lix_conversation (id, target, title, lixcol_global)
				VALUES ($1, lix_row_ref('lix_commit', NULL, $1), 'Checkpoint description', true)
				ON CONFLICT (id) DO NOTHING`,
			params: [commitId],
		},
		{
			sql: `INSERT INTO lix_comment (id, conversation_id, body, lixcol_global)
				VALUES ($1, $1, $2::jsonb, true)
				ON CONFLICT (id) DO UPDATE SET body = excluded.body`,
			params: [commitId, body],
		},
	] as const;
}
