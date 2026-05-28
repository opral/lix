import { expect, test } from "vitest";
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import init, {
	resolveEngineWasmModuleOrPath,
	runBackendConformance,
	type BackendConformanceReport,
} from "../engine-wasm/index.js";
import {
	openLix,
	Value,
	type ExecuteResult,
	type Lix,
} from "../index.js";
import { SqliteBackend } from "./index.js";

const hasBetterSqlite3 = await import("better-sqlite3").then(
	() => true,
	() => false,
);

let wasmReady: Promise<void> | undefined;

test.runIf(hasBetterSqlite3)(
	"SqliteBackend passes backend conformance",
	async () => {
		await ensureWasmReady();
		const report = runBackendConformance({
			createFixture() {
				const path = tempLixPath();
				return {
					open() {
						return new SqliteBackend({ path });
					},
				};
			},
			config: {
				ephemeral: false,
			},
		}) as BackendConformanceReport;

		const failures = report.tests.filter((test) => test.status !== "passed");
		expect(failures).toEqual([]);
	},
);

test.runIf(hasBetterSqlite3)(
	"SqliteBackend can open an rs-sdk-created SQLite backend file",
	async () => {
		const Database = (await import("better-sqlite3")).default;
		const file = tempLixPath();

		runCargoInteropFixture("create_sqlite_fixture", [file]);

		const db = new Database(file);
		try {
			expect(db.pragma("user_version", { simple: true })).toBe(1);
			const table = db
				.prepare(
					"SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'lix_entries'",
				)
				.get();
			expect(table).toMatchObject({ name: "lix_entries" });

			const key = Buffer.from([0, 1, 2, 255]);
			const value = Buffer.from("js-compat-value");
			db.prepare(
				"INSERT INTO lix_entries (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
			).run(key, value);

			const row = db
				.prepare("SELECT value FROM lix_entries WHERE key = ?")
				.get(key);
			expect(Buffer.isBuffer(row.value)).toBe(true);
			expect(row.value.equals(value)).toBe(true);
		} finally {
			db.close();
		}

		const lix = await openLix({
			backend: new SqliteBackend({ path: file }),
		});
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('js-sdk-key', 'js-sdk-value')",
			[],
		);
		await lix.close();
	},
	120_000,
);

test.runIf(hasBetterSqlite3)(
	"SQLite backend matrix: Rust writes and JS reads",
	async () => {
		const file = tempLixPath();

		runCargoInteropFixture("write_sqlite_key_value", [
			file,
			"rust-wrote-key",
			"rust-wrote-value",
		]);

		const lix = await openLix({
			backend: new SqliteBackend({ path: file }),
		});
		const result = await lix.execute(
			"SELECT value FROM lix_key_value WHERE key = 'rust-wrote-key'",
			[],
		);
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.get("value")).toBe("rust-wrote-value");
		await lix.close();
	},
	20_000,
);

test.runIf(hasBetterSqlite3)(
	"SQLite backend matrix: JS writes and Rust reads",
	async () => {
		const file = tempLixPath();
		const lix = await openLix({
			backend: new SqliteBackend({ path: file }),
		});

		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('js-wrote-key', 'js-wrote-value')",
			[],
		);
		await lix.close();

		runCargoInteropFixture("verify_sqlite_key_value", [
			file,
			"js-wrote-key",
			"js-wrote-value",
		]);
	},
	20_000,
);

test.runIf(hasBetterSqlite3)(
	"SqliteBackend can back a Lix session",
	async () => {
		const backend = new SqliteBackend({ path: ":memory:" });
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
		const file = tempLixPath();
		const first = await openLix({
			backend: new SqliteBackend({ path: file }),
		});

		await registerCrmTaskSchema(first);
		await first.execute(
			"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
			["persistent-task", "Persist before close", false],
		);
		await first.close();

		const second = await openLix({
			backend: new SqliteBackend({ path: file }),
		});

		expect(await taskTitle(second, "persistent-task")).toBe(
			"Persist before close",
		);
		await second.close();
	},
);

test.runIf(hasBetterSqlite3)(
	"SqliteBackend rejects a second handle for the same file",
	async () => {
		const file = tempLixPath();
		const firstBackend = new SqliteBackend({ path: file });
		const first = await openLix({ backend: firstBackend });

		expect(() => new SqliteBackend({ path: file })).toThrow(
			/already has an open handle/,
		);

		await first.close();
		const second = await openLix({
			backend: new SqliteBackend({ path: file }),
		});
		await second.close();
	},
);

test.runIf(hasBetterSqlite3)(
	"scanEntries applies resume cursor before limiting",
	() => {
		const backend = new SqliteBackend({ path: ":memory:" });
		try {
			const write = backend.beginWriteTransaction();
			write.writeKvBatch({
				ops: [0, 1, 2, 3, 4].map((byte) => ({
					kind: "put",
					key: new Uint8Array([byte]),
					value: new Uint8Array([byte + 10]),
				})),
			});
			write.commit();

			const read = backend.beginReadTransaction();
			try {
				const keys: number[] = [];
				let after: Uint8Array | null | undefined = null;
				do {
					const page = read.scanEntries({
						range: {
							lower: { kind: "unbounded" },
							upper: { kind: "unbounded" },
						},
						after,
						limit: 1,
					});
					keys.push(...page.keys.map((key) => key[0]!));
					after = page.resumeAfter;
				} while (after);

				expect(keys).toEqual([0, 1, 2, 3, 4]);
			} finally {
				read.rollback();
			}
		} finally {
			backend.close();
		}
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
	const result = await lix.execute("SELECT title FROM crm_task WHERE id = $1", [
		taskId,
	]);
	const rows = expectRows(result);
	expect(rows.rows).toHaveLength(1);
	const title = rows.rows[0]?.get("title");
	expect(typeof title).toBe("string");
	return title as string;
}

function tempLixPath(): string {
	const dir = join(tmpdir(), "lix-js-sdk-sqlite-tests");
	mkdirSync(dir, { recursive: true });
	return join(
		dir,
		`lix-sqlite-test-${Date.now()}-${Math.random().toString(16).slice(2)}.lix`,
	);
}

function workspaceRoot(): string {
	return resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
}

function runCargoInteropFixture(name: string, args: string[]): void {
	execFileSync(
		"cargo",
		[
			"run",
			"--quiet",
			"-p",
			"lix_rs_sdk",
			"--features",
			"sqlite",
			"--example",
			name,
			"--",
			...args,
		],
		{ cwd: workspaceRoot(), stdio: "pipe" },
	);
}

async function ensureWasmReady(): Promise<void> {
	wasmReady ??= resolveEngineWasmModuleOrPath()
		.then((module_or_path) => init({ module_or_path }))
		.then(() => undefined);
	await wasmReady;
}

function expectRows(result: ExecuteResult) {
	return result;
}
