import { expect, test } from "vitest";
import { ebEntity, qb } from "../src/index.js";

const db = qb({
	execute: async () => ({ rows: [] }),
});

test("hasLabel compiles to the label assignment state-address tuple for entity tables", () => {
	const compiled = db
		.selectFrom("lix_commit")
		.where(ebEntity("lix_commit").hasLabel({ name: "checkpoint" }))
		.select("id")
		.compile();

	expect(compiled.sql).toContain("from lix_label_assignment");
	expect(compiled.sql).toContain(
		"lix_label_assignment.target_entity_pk = lix_commit.lixcol_entity_pk",
	);
	expect(compiled.sql).toContain(
		"lix_label_assignment.target_schema_key = lix_commit.lixcol_schema_key",
	);
	expect(compiled.sql).toContain(
		"lix_label_assignment.target_file_id is lix_commit.lixcol_file_id",
	);
	expect(compiled.sql).toContain("lix_label.name = ?");
	expect(compiled.sql).not.toContain("lix_entity_label");
	expect(compiled.parameters).toEqual(["checkpoint"]);
});

test("hasLabel compiles to the label assignment state-address tuple for canonical state tables", () => {
	const compiled = db
		.selectFrom("lix_state")
		.where(ebEntity("lix_state").hasLabel({ id: "label-a" }))
		.select("entity_pk")
		.compile();

	expect(compiled.sql).toContain(
		"lix_label_assignment.target_entity_pk = lix_state.entity_pk",
	);
	expect(compiled.sql).toContain(
		"lix_label_assignment.target_schema_key = lix_state.schema_key",
	);
	expect(compiled.sql).toContain(
		"lix_label_assignment.target_file_id is lix_state.file_id",
	);
	expect(compiled.sql).toContain("lix_label.id = ?");
	expect(compiled.parameters).toEqual(["label-a"]);
});
