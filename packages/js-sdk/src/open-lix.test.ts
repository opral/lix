import { expect, test } from "vitest";
import {
	openLix,
	Value,
	type ExecuteResult,
	type Lix,
} from "./index.js";

test("openLix exposes the rs-sdk e2e flow", async () => {
	const lix = await openLix();
	const mainVersionId = await lix.activeVersionId();

	await registerCrmTaskSchema(lix);

	await lix.execute(
		"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
		["task-1", "Draft JS SDK flow", false],
	);

	expect(await taskDone(lix, "task-1")).toBe(false);

	const draft = await lix.createVersion({
		id: "draft-version",
		name: "Draft",
	});

	await lix.switchVersion({ versionId: draft.versionId });

	await lix.execute("UPDATE crm_task SET done = $1 WHERE id = $2", [
		true,
		"task-1",
	]);

	expect(await taskDone(lix, "task-1")).toBe(true);

	await lix.switchVersion({ versionId: mainVersionId });

	expect(await taskDone(lix, "task-1")).toBe(false);

	const merge = await lix.mergeVersion({
		sourceVersionId: draft.versionId,
	});

	expect(merge.outcome).toBe("mergeCommitted");
	expect(merge.targetVersionId).toBe(mainVersionId);
	expect(merge.appliedChangeCount).toBeGreaterThan(0);
	expect(await taskDone(lix, "task-1")).toBe(true);

	await lix.close();
	await expect(lix.activeVersionId()).rejects.toThrow("lix is closed");
});

async function registerCrmTaskSchema(lix: Lix) {
	const schema = {
		$schema: "https://json-schema.org/draft/2020-12/schema",
		"x-lix-key": "crm_task",
		"x-lix-version": "1",
		"x-lix-primary-key": ["/id"],
		type: "object",
		required: ["id", "title", "done"],
		properties: {
			id: { type: "string" },
			title: { type: "string" },
			done: { type: "boolean" },
		},
		additionalProperties: false,
	} as const;

	await lix.execute(
		"INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
		[JSON.stringify(schema)],
	);
}

async function taskDone(lix: Lix, taskId: string): Promise<boolean> {
	const result = await lix.execute(
		"SELECT done FROM crm_task WHERE id = $1",
		[taskId],
	);
	const rows = expectRows(result);
	expect(rows.rows).toHaveLength(1);
	const done = rows.rows[0]?.[0];
	expect(done).toBeInstanceOf(Value);
	expect(done?.asBoolean()).not.toBeUndefined();
	return done!.asBoolean()!;
}

function expectRows(result: ExecuteResult) {
	expect(result.kind).toBe("rows");
	if (result.kind !== "rows") {
		throw new Error("expected rows");
	}
	return result.rows;
}
