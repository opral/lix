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
	isLixError,
} from "./index.js";

test("openLix exposes the rs-sdk e2e flow", async () => {
	const lix = await openLix();
	const mainVersionId = await lix.activeVersionId();

	await registerCrmTaskSchema(lix);

	await lix.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"task-1",
			"Draft JS SDK flow",
			false,
			JSON.stringify({ priority: "high", tags: ["sdk", "json"] }),
		],
	);

	const projected = await lix.execute(
		"SELECT title, meta FROM crm_task WHERE id = $1",
		["task-1"],
	);
	const projectedRow = projected.rows[0]!;
	expect(projectedRow.get("title")).toBe("Draft JS SDK flow");
	expect(projectedRow.value("title")).toBeInstanceOf(Value);
	expect(projectedRow.get("meta")).toEqual({
		priority: "high",
		tags: ["sdk", "json"],
	});
	expect(projectedRow.value("meta").kind).toBe("json");
	expect(projectedRow.value("meta").asJson()).toEqual({
		priority: "high",
		tags: ["sdk", "json"],
	});
	expect(projectedRow.toObject()).toEqual({
		title: "Draft JS SDK flow",
		meta: { priority: "high", tags: ["sdk", "json"] },
	});
	expect(projectedRow.toValueMap().title).toBeInstanceOf(Value);
	expect(() => projectedRow.get("missing")).toThrow(
		/Available columns: title, meta/,
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
	await lix.close();
	await expect(lix.activeVersionId()).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	await expect(lix.execute("SELECT 1")).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
});

test("openLix accepts an explicit backend", async () => {
	const backend = createMemoryBackend();

	const first = await openLix({ backend });
	await registerCrmTaskSchema(first);
	await first.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"backend-task",
			"Stored through explicit backend",
			false,
			JSON.stringify({ priority: "normal" }),
		],
	);
	await first.close();

	const second = await openLix({ backend });
	expect(await taskDone(second, "backend-task")).toBe(false);
	await second.close();
});

test("lix.close delegates backend close through the engine bridge", async () => {
	let closeCount = 0;
	const backend = {
		...createMemoryBackend(),
		close() {
			closeCount += 1;
		},
	};

	const lix = await openLix({ backend });
	await lix.close();
	await lix.close();

	expect(closeCount).toBe(1);
});

test("engine errors expose structured hints", async () => {
	const lix = await openLix();

	try {
		await lix.execute("SELECT entity_id FROM lix_state_history");
		throw new Error("expected history query to fail");
	} catch (error) {
		expect(isLixError(error)).toBe(true);
		if (!isLixError(error)) throw error;
		expect(error.code).toBe("LIX_HISTORY_FILTER_REQUIRED");
		expect(error.hint).toContain("lix_active_version_commit_id()");
	}

	await lix.close();
});

async function registerCrmTaskSchema(lix: Lix) {
	const schema = {
		$schema: "https://json-schema.org/draft/2020-12/schema",
		"x-lix-key": "crm_task",
		"x-lix-version": "1",
		"x-lix-primary-key": ["/id"],
		type: "object",
		required: ["id", "title", "done", "meta"],
		properties: {
			id: { type: "string" },
			title: { type: "string" },
			done: { type: "boolean" },
			meta: { type: "object" },
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
	const done = rows.rows[0]?.get("done");
	expect(typeof done).toBe("boolean");
	return done as boolean;
}

function expectRows(result: ExecuteResult) {
	return result;
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
