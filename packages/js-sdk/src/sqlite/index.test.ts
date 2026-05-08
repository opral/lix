import { expect, test } from "vitest";
import { openLix, Value, type ExecuteResult, type Lix } from "../index.js";

const hasBetterSqlite3 = await import("better-sqlite3").then(
	() => true,
	() => false,
);

test.runIf(hasBetterSqlite3)(
	"createBetterSqlite3Backend can back a Lix session",
	async () => {
		const { createBetterSqlite3Backend } = await import("./index.js");
		const backend = createBetterSqlite3Backend({ path: ":memory:" });
		const lix = await openLix({ backend });

		await registerCrmTaskSchema(lix);
		await lix.execute(
			"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
			["sqlite-task", "Ship better-sqlite3 backend", false],
		);

		expect(await taskTitle(lix, "sqlite-task")).toBe(
			"Ship better-sqlite3 backend",
		);
		await lix.close();
	},
);

test.runIf(hasBetterSqlite3)(
	"committed writes survive close and reopen",
	async () => {
		const { createBetterSqlite3Backend } = await import("./index.js");
		const file = tempLixPath();
		const first = await openLix({
			backend: createBetterSqlite3Backend({ path: file }),
		});

		await registerCrmTaskSchema(first);
		await first.execute(
			"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
			["persistent-task", "Persist before close", false],
		);
		await first.close();

		const second = await openLix({
			backend: createBetterSqlite3Backend({ path: file }),
		});

		expect(await taskTitle(second, "persistent-task")).toBe(
			"Persist before close",
		);
		await second.close();
	},
);

test.runIf(hasBetterSqlite3)(
	"createBetterSqlite3Backend rejects a second handle for the same file",
	async () => {
		const { createBetterSqlite3Backend } = await import("./index.js");
		const file = tempLixPath();
		const firstBackend = createBetterSqlite3Backend({ path: file });
		const first = await openLix({ backend: firstBackend });

		expect(() => createBetterSqlite3Backend({ path: file })).toThrow(
			/already has an open handle/,
		);

		await first.close();
		const second = await openLix({
			backend: createBetterSqlite3Backend({ path: file }),
		});
		await second.close();
	},
);

async function registerCrmTaskSchema(lix: Lix) {
	const schema = {
		$schema: "https://json-schema.org/draft/2020-12/schema",
		"x-lix-key": "crm_task",
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

async function taskTitle(lix: Lix, taskId: string): Promise<string> {
	const result = await lix.execute(
		"SELECT title FROM crm_task WHERE id = $1",
		[taskId],
	);
	const rows = expectRows(result);
	expect(rows.rows).toHaveLength(1);
	const title = rows.rows[0]?.get("title");
	expect(typeof title).toBe("string");
	return title as string;
}

function tempLixPath(): string {
	return `/tmp/lix-sqlite-test-${Date.now()}-${Math.random()
		.toString(16)
		.slice(2)}.lix`;
}

function expectRows(result: ExecuteResult) {
	return result;
}
