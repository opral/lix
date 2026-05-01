import { expect, test } from "vitest";
import {
	openLix,
	Value,
	type ExecuteResult,
	type KvPair,
	type KvScanRange,
	type LixBackend,
	type LixBackendTransaction,
	type Lix,
	type TransactionBeginMode,
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

test("openLix accepts an explicit backend", async () => {
	const backend = createMemoryBackend();

	const first = await openLix({ backend });
	await registerCrmTaskSchema(first);
	await first.execute(
		"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
		["backend-task", "Stored through explicit backend", false],
	);
	await first.close();

	const second = await openLix({ backend });
	expect(await taskDone(second, "backend-task")).toBe(false);
	await second.close();
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

type StoredKvPair = {
	namespace: string;
	key: Uint8Array;
	value: Uint8Array;
};

function createMemoryBackend(): LixBackend {
	let rows: StoredKvPair[] = [];

	return {
		beginTransaction(_mode: TransactionBeginMode): LixBackendTransaction {
			let transactionRows = rows.map(cloneStoredPair);
			let closed = false;

			const ensureOpen = () => {
				if (closed) {
					throw new Error("transaction is closed");
				}
			};

			return {
				kvGet(namespace, key) {
					ensureOpen();
					const row = transactionRows.find(
						(row) =>
							row.namespace === namespace && compareBytes(row.key, key) === 0,
					);
					return row ? new Uint8Array(row.value) : null;
				},
				kvScan(namespace, range, limit) {
					ensureOpen();
					const matches = transactionRows
						.filter(
							(row) =>
								row.namespace === namespace && keyMatchesRange(row.key, range),
						)
						.sort((left, right) => compareBytes(left.key, right.key))
						.slice(0, limit ?? undefined);
					return matches.map(
						(row): KvPair => ({
							key: new Uint8Array(row.key),
							value: new Uint8Array(row.value),
						}),
					);
				},
				kvPut(namespace, key, value) {
					ensureOpen();
					transactionRows = transactionRows.filter(
						(row) =>
							row.namespace !== namespace || compareBytes(row.key, key) !== 0,
					);
					transactionRows.push({
						namespace,
						key: new Uint8Array(key),
						value: new Uint8Array(value),
					});
				},
				kvDelete(namespace, key) {
					ensureOpen();
					transactionRows = transactionRows.filter(
						(row) =>
							row.namespace !== namespace || compareBytes(row.key, key) !== 0,
					);
				},
				commit() {
					ensureOpen();
					rows = transactionRows.map(cloneStoredPair);
					closed = true;
				},
				rollback() {
					ensureOpen();
					closed = true;
				},
			};
		},
	};
}

function cloneStoredPair(row: StoredKvPair): StoredKvPair {
	return {
		namespace: row.namespace,
		key: new Uint8Array(row.key),
		value: new Uint8Array(row.value),
	};
}

function keyMatchesRange(key: Uint8Array, range: KvScanRange): boolean {
	if (range.kind === "prefix") {
		if (key.length < range.prefix.length) return false;
		return range.prefix.every((byte, index) => key[index] === byte);
	}
	return (
		compareBytes(key, range.start) >= 0 && compareBytes(key, range.end) < 0
	);
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
	const length = Math.min(left.length, right.length);
	for (let index = 0; index < length; index++) {
		const delta = left[index]! - right[index]!;
		if (delta !== 0) return delta;
	}
	return left.length - right.length;
}
