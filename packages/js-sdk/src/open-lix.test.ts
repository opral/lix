import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";
import {
	openLix,
	Value,
	type BackendKvEntryPage,
	type BackendKvExistsBatch,
	type BackendKvGetRequest,
	type BackendKvKeyPage,
	type BackendKvScanRange,
	type BackendKvScanRequest,
	type BackendKvValueBatch,
	type BackendKvValuePage,
	type BackendKvWriteBatch,
	type BackendKvWriteStats,
	type ExecuteResult,
	type LixBackend,
	type LixBackendReadTransaction,
	type LixBackendWriteTransaction,
	type LixError,
	type Lix,
	isLixError,
} from "./index.js";

const execFileAsync = promisify(execFile);
const jsSdkRoot = fileURLToPath(new URL("..", import.meta.url));

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

	const mainHead = await lix.execute("SELECT lix_active_version_commit_id()");
	const mainHeadCommitId = mainHead.rows[0]!.get("lix_active_version_commit_id()");
	expect(typeof mainHeadCommitId).toBe("string");

	const draft = await lix.createVersion({
		id: "draft-version",
		name: "Draft",
	});
	expect(draft).toMatchObject({
		id: "draft-version",
		name: "Draft",
		hidden: false,
		commitId: mainHeadCommitId,
	});

	await lix.switchVersion({ versionId: draft.id });

	await lix.execute("UPDATE crm_task SET done = $1 WHERE id = $2", [
		true,
		"task-1",
	]);

	expect(await taskDone(lix, "task-1")).toBe(true);

	await lix.switchVersion({ versionId: mainVersionId });

	expect(await taskDone(lix, "task-1")).toBe(false);

	const preview = await lix.mergeVersionPreview({
		sourceVersionId: draft.id,
	});
	expect(preview.outcome).toBe("fastForward");
	expect(preview.targetVersionId).toBe(mainVersionId);
	expect(preview.sourceVersionId).toBe(draft.id);
	expect(preview.changeStats).toEqual({
		total: 1,
		added: 0,
		modified: 1,
		removed: 0,
	});
	expect(preview.conflicts).toEqual([]);
	expect(await taskDone(lix, "task-1")).toBe(false);

	const merge = await lix.mergeVersion({
		sourceVersionId: draft.id,
	});

	expect(merge.outcome).toBe("fastForward");
	expect(merge.targetVersionId).toBe(mainVersionId);
	expect(merge.changeStats).toEqual({
		total: 1,
		added: 0,
		modified: 1,
		removed: 0,
	});
	expect(merge.createdMergeCommitId).toBeNull();
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

test("custom backend applies ordered deleteRange write ops", () => {
	const backend = createMemoryBackend();
	const tx = backend.beginWriteTransaction();

	tx.writeKvBatch({
		groups: [
			{
				namespace: "n",
				ops: [
					{ kind: "put", key: new Uint8Array([1]), value: new Uint8Array([10]) },
					{ kind: "put", key: new Uint8Array([2]), value: new Uint8Array([20]) },
					{
						kind: "deleteRange",
						range: { kind: "prefix", prefix: new Uint8Array([1]) },
					},
					{ kind: "put", key: new Uint8Array([1]), value: new Uint8Array([11]) },
				],
			},
		],
	});
	expect(tx.commit()).toBeUndefined();

	const read = backend.beginReadTransaction();
	const values = read.getValues({
		groups: [
			{
				namespace: "n",
				keys: [new Uint8Array([1]), new Uint8Array([2])],
			},
		],
	});

	expect(values.groups[0]?.values).toEqual([
		new Uint8Array([11]),
		new Uint8Array([20]),
	]);
	expect(read.rollback()).toBeUndefined();
});

test("execute supports UNION ALL without trapping wasm", async () => {
	const lix = await openLix();

	const result = await lix.execute("SELECT 1 UNION ALL SELECT 2");

	expect(result.rows.map((row) => row.get("Int64(1)"))).toEqual([1, 2]);
	await lix.close();
});

test("beginTransaction commits multiple statements together", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["tx-task-1", "First", false, JSON.stringify({ batch: 1 })],
	);
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["tx-task-2", "Second", true, JSON.stringify({ batch: 1 })],
	);

	const staged = await tx.execute(
		"SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
		["tx-task-1", "tx-task-2"],
	);
	expect(staged.rows.map((row) => row.get("id"))).toEqual([
		"tx-task-1",
		"tx-task-2",
	]);

	await tx.commit();

	const committed = await lix.execute(
		"SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
		["tx-task-1", "tx-task-2"],
	);
	expect(committed.rows.map((row) => row.get("id"))).toEqual([
		"tx-task-1",
		"tx-task-2",
	]);
	await expect(tx.execute("SELECT 1")).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});

	await lix.close();
});

test("beginTransaction rollback discards writes and closes handle", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["rolled-back-task", "Rollback", false, JSON.stringify({ batch: 1 })],
	);
	await tx.rollback();

	const result = await lix.execute("SELECT id FROM crm_task WHERE id = $1", [
		"rolled-back-task",
	]);
	expect(result.rows).toHaveLength(0);
	await expect(tx.rollback()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});

	await lix.close();
});

test("beginTransaction blocks session writes on the same handle", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["tx-only-task", "Inside tx", false, JSON.stringify({ batch: 1 })],
	);

	await expect(
		lix.execute(
			"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
			["outside-task", "Outside tx", false, JSON.stringify({ batch: 1 })],
		),
	).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});

	await tx.commit();

	const committed = await lix.execute(
		"SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
		["outside-task", "tx-only-task"],
	);
	expect(committed.rows.map((row) => row.get("id"))).toEqual([
		"tx-only-task",
	]);

	await lix.close();
});

test("beginTransaction blocks session reads on the same handle", async () => {
	const lix = await openLix();
	const tx = await lix.beginTransaction();

	await expect(lix.execute("SELECT 1 AS ok")).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});

	const result = await tx.execute("SELECT 1 AS ok");
	expect(result.rows[0]?.get("ok")).toBe(1);

	await tx.rollback();
	await lix.close();
});

test("unsupported UNION DISTINCT returns a JS error without trapping wasm", async () => {
	const { stdout } = await execFileAsync(
		process.execPath,
		[
			"--input-type=module",
			"-e",
			`
				import { openLix } from './dist/index.js';
				const lix = await openLix();
				try {
					await lix.execute('SELECT 1 UNION SELECT 1');
					console.log('unexpected-success');
				} catch (error) {
					console.log(error.code, error.message);
				} finally {
					await lix.close().catch(() => {});
				}
			`,
		],
		{ cwd: jsSdkRoot },
	);

	expect(stdout).toContain("LIX_UNSUPPORTED_SQL_RUNTIME_PLAN");
	expect(stdout).toContain("CoalescePartitionsExec");
});

test("INSERT SELECT UNION ALL executes without trapping wasm", async () => {
	const { stdout } = await execFileAsync(
		process.execPath,
		[
			"--input-type=module",
			"-e",
			`
				import { openLix } from './dist/index.js';
				const lix = await openLix();
				try {
					const result = await lix.execute("INSERT INTO lix_directory (path) SELECT '/u1/' UNION ALL SELECT '/u2/'");
					console.log(result.rowsAffected);
				} finally {
					await lix.close().catch(() => {});
				}
			`,
		],
		{ cwd: jsSdkRoot },
	);

	expect(stdout.trim()).toBe("2");
});

test("createVersion can start from an explicit commit id", async () => {
	const lix = await openLix();

	await registerCrmTaskSchema(lix);
	const baseHead = await lix.execute("SELECT lix_active_version_commit_id()");
	const fromCommitId = baseHead.rows[0]!.get("lix_active_version_commit_id()");
	expect(typeof fromCommitId).toBe("string");

	await lix.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"after-base",
			"Written after base",
			false,
			JSON.stringify({ priority: "normal" }),
		],
	);

	const version = await lix.createVersion({
		id: "from-explicit-commit",
		name: "From explicit commit",
		fromCommitId: fromCommitId as string,
	});
	expect(version).toMatchObject({
		id: "from-explicit-commit",
		name: "From explicit commit",
		hidden: false,
		commitId: fromCommitId,
	});
	await lix.switchVersion({ versionId: version.id });

	const projected = await lix.execute(
		"SELECT id FROM crm_task WHERE id = $1",
		["after-base"],
	);
	expect(projected.rows).toHaveLength(0);

	await lix.close();
});

test("merge conflicts expose structured details", async () => {
	const lix = await openLix();
	const mainVersionId = await lix.activeVersionId();
	await registerCrmTaskSchema(lix);
	await lix.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"conflict-task",
			"Base",
			false,
			JSON.stringify({ priority: "normal" }),
		],
	);
	const draft = await lix.createVersion({
		id: "conflict-draft",
		name: "Conflict draft",
	});

	await lix.switchVersion({ versionId: draft.id });
	await lix.execute("UPDATE crm_task SET title = $1 WHERE id = $2", [
		"Draft",
		"conflict-task",
	]);

	await lix.switchVersion({ versionId: mainVersionId });
	await lix.execute("UPDATE crm_task SET title = $1 WHERE id = $2", [
		"Main",
		"conflict-task",
	]);

	try {
		await lix.mergeVersion({ sourceVersionId: draft.id });
		throw new Error("expected merge conflict");
	} catch (error) {
		expect(isLixError(error)).toBe(true);
		if (!isLixError(error)) throw error;
		expect(error.code).toBe("LIX_MERGE_CONFLICT");
		expect(error.message).toContain("tracked-state conflict");
		expect(error.details).toBeDefined();
		expect((error as LixError & { data?: unknown }).data).toBeUndefined();
		expect(
			"description" in (error as LixError & { description?: unknown }),
		).toBe(false);
		const details = error.details as {
			conflicts?: Array<{
				schemaKey?: string;
				entityId?: string[];
				target?: unknown;
				source?: unknown;
			}>;
		};
		expect(details.conflicts).toHaveLength(1);
		expect(details.conflicts?.[0]).toMatchObject({
			schemaKey: "crm_task",
			entityId: ["conflict-task"],
		});
		expect(details.conflicts?.[0]?.target).toBeDefined();
		expect(details.conflicts?.[0]?.source).toBeDefined();
	}

	await lix.close();
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

test("execute rejects invalid runtime arguments before wasm", async () => {
	const lix = await openLix();
	const unsafeLix = lix as unknown as {
		execute(sql: unknown, params?: unknown): Promise<ExecuteResult>;
	};

	await expect(unsafeLix.execute(123, [])).rejects.toMatchObject({
		name: "LixError",
		code: "LIX_INVALID_ARGUMENT",
		message: "lix.execute() expected sql to be a string",
		details: {
			operation: "execute",
			argument: "sql",
			expected: "string",
			actual: "number",
		},
	});

	await expect(unsafeLix.execute("SELECT 1", 123)).rejects.toMatchObject({
		name: "LixError",
		code: "LIX_INVALID_ARGUMENT",
		message: "lix.execute() expected params to be an array",
		details: {
			operation: "execute",
			argument: "params",
			expected: "array",
			actual: "number",
		},
	});

	await lix.close();
});

test("execute rejects lossy JavaScript parameter coercions", async () => {
	const lix = await openLix();
	const circular: Record<string, unknown> = {};
	circular.self = circular;

	const invalidCases: Array<{
		name: string;
		value: unknown;
		message: string | RegExp;
		actual?: string;
	}> = [
		{
			name: "Date",
			value: new Date("2026-01-02T03:04:05.000Z"),
			message: /Date is not a valid SQL parameter/,
			actual: "Date",
		},
		{
			name: "Int32Array",
			value: new Int32Array([1, 2, 3]),
			message: /typed array SQL parameters must be Uint8Array/,
			actual: "Int32Array",
		},
		{
			name: "lone surrogate",
			value: "X\uD83DY",
			message: /well-formed UTF-16/,
			actual: "string",
		},
		{
			name: "undefined",
			value: undefined,
			message: /undefined is not a valid SQL parameter/,
			actual: "undefined",
		},
		{
			name: "BigInt",
			value: 10n,
			message: /requires a LixValue, JSON value, or binary value/,
			actual: "bigint",
		},
		{
			name: "NaN",
			value: Number.NaN,
			message: /finite number/,
			actual: "number",
		},
		{
			name: "Infinity",
			value: Number.POSITIVE_INFINITY,
			message: /finite number/,
			actual: "number",
		},
		{
			name: "circular object",
			value: circular,
			message: /circular references/,
			actual: "object",
		},
		{
			name: "Symbol",
			value: Symbol("x"),
			message: /requires a LixValue, JSON value, or binary value/,
			actual: "symbol",
		},
		{
			name: "function",
			value: () => undefined,
			message: /requires a LixValue, JSON value, or binary value/,
			actual: "function",
		},
	];

	for (const testCase of invalidCases) {
		try {
			await lix.execute("SELECT $1 AS v", [testCase.value as never]);
			throw new Error(`expected ${testCase.name} to fail`);
		} catch (error) {
			expect(error, testCase.name).toMatchObject({
				name: "LixError",
				code: "LIX_INVALID_PARAM",
				details: {
					operation: "execute",
					parameter_index: 1,
					argument: "params[0]",
					actual: testCase.actual,
				},
			});
			if (!(error instanceof Error)) throw error;
			expect(error.message, testCase.name).toMatch(testCase.message);
		}
	}

	await lix.close();
});

test("execute rejects extra SQL parameters", async () => {
	const lix = await openLix();

	try {
		await lix.execute("SELECT $1 AS v", [1, 2]);
		throw new Error("expected extra params to fail");
	} catch (error) {
		expect(error).toMatchObject({
			code: "LIX_INVALID_PARAM",
			details: {
				operation: "execute",
				expected_param_count: 1,
				provided_param_count: 2,
				placeholders: ["$1"],
			},
		});
		if (!(error instanceof Error)) throw error;
		expect(error.message).toBe(
			"SQL expected 1 parameter(s), but 2 parameter(s) were provided",
		);
	}

	await lix.close();
});

test("lix_state_history snapshot_content preserves JSON null for binary file rows", async () => {
	const lix = await openLix();

	await lix.execute(
		"INSERT INTO lix_file (id, path, data, hidden) VALUES ($1, $2, $3, false)",
		[
			"history-binary-js-repro",
			"/history/repro.bin",
			new Uint8Array([0x80, 0xff, 0x00]),
		],
	);

	const result = await lix.execute(
		"SELECT schema_key, snapshot_content \
		 FROM lix_state_history \
		 WHERE start_commit_id = lix_active_version_commit_id()",
	);
	const directoryRow = result.rows.find(
		(row) => row.get("schema_key") === "lix_directory_descriptor",
	);

	expect(directoryRow?.get("snapshot_content")).toMatchObject({
		parent_id: null,
	});

	await lix.close();
});

async function registerCrmTaskSchema(lix: Lix) {
	const schema = {
		$schema: "https://json-schema.org/draft/2020-12/schema",
		"x-lix-key": "crm_task",
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

	function createTransaction(): LixBackendWriteTransaction {
			let transactionRows = rows.map(cloneStoredPair);
			let closed = false;

			const ensureOpen = () => {
				if (closed) {
					throw new Error("transaction is closed");
				}
			};

			return {
				getValues(request): BackendKvValueBatch {
					ensureOpen();
					return {
						groups: request.groups.map((group) => ({
							namespace: group.namespace,
							values: group.keys.map((key) => {
								const row = transactionRows.find(
									(row) =>
										row.namespace === group.namespace &&
										compareBytes(row.key, key) === 0,
								);
								return row ? new Uint8Array(row.value) : null;
							}),
						})),
					};
				},
				existsMany(request): BackendKvExistsBatch {
					ensureOpen();
					return {
						groups: request.groups.map((group) => ({
							namespace: group.namespace,
							exists: group.keys.map((key) =>
								transactionRows.some(
									(row) =>
										row.namespace === group.namespace &&
										compareBytes(row.key, key) === 0,
								),
							),
						})),
					};
				},
				scanKeys(request): BackendKvKeyPage {
					ensureOpen();
					const { pairs, resumeAfter } = scanPage(transactionRows, request);
					return {
						keys: pairs.map((row) => new Uint8Array(row.key)),
						resumeAfter,
					};
				},
				scanValues(request): BackendKvValuePage {
					ensureOpen();
					const { pairs, resumeAfter } = scanPage(transactionRows, request);
					return {
						values: pairs.map((row) => new Uint8Array(row.value)),
						resumeAfter,
					};
				},
				scanEntries(request): BackendKvEntryPage {
					ensureOpen();
					const { pairs, resumeAfter } = scanPage(transactionRows, request);
					return {
						keys: pairs.map((row) => new Uint8Array(row.key)),
						values: pairs.map((row) => new Uint8Array(row.value)),
						resumeAfter,
					};
				},
				writeKvBatch(batch): BackendKvWriteStats {
					ensureOpen();
					const stats: BackendKvWriteStats = {
						puts: 0,
						deletes: 0,
						deleteRanges: 0,
						bytesWritten: 0,
					};
					for (const group of batch.groups) {
						for (const op of group.ops) {
							if (op.kind === "put") {
								stats.puts += 1;
								stats.bytesWritten += op.key.length + op.value.length;
								transactionRows = transactionRows.filter(
									(row) =>
										row.namespace !== group.namespace ||
										compareBytes(row.key, op.key) !== 0,
								);
								transactionRows.push({
									namespace: group.namespace,
									key: new Uint8Array(op.key),
									value: new Uint8Array(op.value),
								});
							} else if (op.kind === "delete") {
								stats.deletes += 1;
								stats.bytesWritten += op.key.length;
								transactionRows = transactionRows.filter(
									(row) =>
										row.namespace !== group.namespace ||
										compareBytes(row.key, op.key) !== 0,
								);
							} else {
								stats.deleteRanges += 1;
								stats.bytesWritten += deleteRangeBytes(op.range);
								transactionRows = transactionRows.filter(
									(row) =>
										row.namespace !== group.namespace ||
										!keyMatchesRange(row.key, op.range),
								);
							}
						}
					}
					return stats;
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
	}

	return {
		beginReadTransaction(): LixBackendReadTransaction {
			return createTransaction();
		},
		beginWriteTransaction(): LixBackendWriteTransaction {
			return createTransaction();
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

function scanPage(
	rows: StoredKvPair[],
	request: BackendKvScanRequest,
): { pairs: StoredKvPair[]; resumeAfter: Uint8Array | null } {
	const matches = rows
		.filter(
			(row) =>
				row.namespace === request.namespace &&
				keyMatchesRange(row.key, request.range) &&
				(!request.after || compareBytes(row.key, request.after) > 0),
		)
		.sort((left, right) => compareBytes(left.key, right.key));
	const hasMore = matches.length > request.limit;
	const pairs = matches.slice(0, request.limit);
	return {
		pairs,
		resumeAfter: hasMore ? (pairs.at(-1)?.key ?? null) : null,
	};
}

function keyMatchesRange(key: Uint8Array, range: BackendKvScanRange): boolean {
	if (range.kind === "prefix") {
		if (key.length < range.prefix.length) return false;
		return range.prefix.every((byte, index) => key[index] === byte);
	}
	return (
		compareBytes(key, range.start) >= 0 && compareBytes(key, range.end) < 0
	);
}

function deleteRangeBytes(range: BackendKvScanRange): number {
	if (range.kind === "prefix") {
		return range.prefix.length;
	}
	return range.start.length + range.end.length;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
	const length = Math.min(left.length, right.length);
	for (let index = 0; index < length; index++) {
		const delta = left[index]! - right[index]!;
		if (delta !== 0) return delta;
	}
	return left.length - right.length;
}
