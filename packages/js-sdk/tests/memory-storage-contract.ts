import { describe, expect, test } from "vitest";
import type * as LixSdk from "../src/index.js";

type ContractSdk = typeof LixSdk;
type ContractLix = Awaited<ReturnType<ContractSdk["openLix"]>>;

export type MemoryStorageContractOptions = {
	name: string;
	loadSdk: () => Promise<ContractSdk>;
	operationTimeoutMs?: number;
};

export function registerMemoryStorageContract({
	name,
	loadSdk,
	operationTimeoutMs = 5_000,
}: MemoryStorageContractOptions): void {
	const wait = <T>(promise: Promise<T>, operation: string): Promise<T> =>
		withTimeout(promise, operation, operationTimeoutMs);

	describe(`${name} memory-storage public contract`, () => {
		test("round-trips values, blobs, and JSON and preserves structured errors", async () => {
			const { openLix, Value } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");

			try {
				const scalar = await lix.execute("SELECT $1 AS text, $2 AS flag, $3 AS count", [
					"hello",
					true,
					42,
				]);
				expect(scalar.rows[0]?.toObject()).toEqual({
					text: "hello",
					flag: true,
					count: 42,
				});

				const json = { nested: { ok: true }, items: [1, "two", null] };
				const jsonResult = await lix.execute("SELECT $1 AS value", [json]);
				expect(jsonResult.rows[0]?.get("value")).toEqual(json);
				const returnedJson = jsonResult.rows[0]?.get("value") as typeof json;
				returnedJson.nested.ok = false;
				expect(jsonResult.rows[0]?.get("value")).toEqual(json);

				const bytes = new Uint8Array([0x00, 0x01, 0x7f, 0xff]);
				const blobResult = await lix.execute("SELECT $1 AS value", [bytes]);
				const blob = blobResult.rows[0]?.value("value");
				expect(blob?.kind).toBe("blob");
				expect(blob?.asBytes()).toEqual(bytes);
				const returnedBytes = blob?.asBytes();
				if (!returnedBytes) throw new Error("expected blob bytes");
				returnedBytes[0] = 0xff;
				expect(blob?.asBytes()).toEqual(bytes);

				const explicit = await lix.execute("SELECT $1 AS value", [Value.real(1.5)]);
				expect(explicit.rows[0]?.get("value")).toBe(1.5);

				await expect(
					lix.execute("SELECT $1 AS value", [Number.NaN]),
				).rejects.toMatchObject({
					name: "LixError",
					code: "LIX_INVALID_PARAM",
					details: { operation: "execute", parameter_index: 1 },
				});
				await expect(
					lix.execute("SELECT entity_pk FROM lix_state_history"),
				).rejects.toMatchObject({
					name: "LixError",
					code: "LIX_HISTORY_FILTER_REQUIRED",
					hint: expect.stringContaining("lix_active_branch_commit_id()"),
				});
			} finally {
				await lix.close();
			}
		});

		test("commits and rolls back transactions and survives a rejected close", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");

			const committed = await lix.beginTransaction();
			await committed.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["contract-committed", "yes"],
			);
			await committed.commit();
			expect(
				(
					await lix.execute(
						"SELECT value FROM lix_key_value WHERE key = $1",
						["contract-committed"],
					)
				).rows[0]?.get("value"),
			).toBe("yes");

			const rolledBack = await lix.beginTransaction();
			await rolledBack.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["contract-rolled-back", "no"],
			);
			await expect(lix.close()).rejects.toMatchObject({
				name: "LixError",
				code: "LIX_INVALID_TRANSACTION_STATE",
			});
			expect((await rolledBack.execute("SELECT 1 AS ok")).rows[0]?.get("ok")).toBe(1);
			await rolledBack.rollback();

			const afterRollback = await lix.execute(
				"SELECT COUNT(*) AS count FROM lix_key_value WHERE key = $1",
				["contract-rolled-back"],
			);
			expect(afterRollback.rows[0]?.get("count")).toBe(0);
			await lix.close();
		});

		test("executes ordered atomic batches with per-statement parameters", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");

			try {
				const oneStatement = await lix.executeBatch([
					{ sql: "SELECT $1 AS value", params: ["one statement"] },
				]);
				expect(oneStatement).toHaveLength(1);
				expect(oneStatement[0]?.rows[0]?.get("value")).toBe(
					"one statement",
				);

				const results = await lix.executeBatch([
					{
						sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
						params: ["batch-a", "first"],
					},
					{
						sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
						params: ["batch-b", "second"],
					},
					{
						sql: "SELECT key, value FROM lix_key_value WHERE key IN ($1, $2) ORDER BY key",
						params: ["batch-a", "batch-b"],
					},
				]);
				expect(results).toHaveLength(3);
				expect(results[0]?.rowsAffected).toBe(1);
				expect(results[1]?.rowsAffected).toBe(1);
				expect(
					results[2]?.rows.map((row) => row.toObject()),
				).toEqual([
					{ key: "batch-a", value: "first" },
					{ key: "batch-b", value: "second" },
				]);

				const executeResult = await lix.execute("SELECT $1 AS value", [
					"execute remains unchanged",
				]);
				expect(executeResult.rows[0]?.get("value")).toBe(
					"execute remains unchanged",
				);

				await expect(lix.executeBatch([])).rejects.toMatchObject({
					name: "LixError",
					code: "LIX_INVALID_ARGUMENT",
					details: {
						operation: "executeBatch",
						argument: "statements",
						expected: "non-empty array",
						actual: "empty array",
					},
				});

				await expect(
					lix.executeBatch([
						{
							sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
							params: ["batch-rolled-back", "before failure"],
						},
						{ sql: "SELECT entity_pk FROM lix_state_history" },
						{
							sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
							params: ["batch-not-run", "after failure"],
						},
					]),
				).rejects.toMatchObject({
					name: "LixError",
					code: "LIX_HISTORY_FILTER_REQUIRED",
					details: { statementIndex: 1 },
				});
				const rolledBack = await lix.execute(
					"SELECT key FROM lix_key_value WHERE key IN ($1, $2) ORDER BY key",
					["batch-rolled-back", "batch-not-run"],
				);
				expect(rolledBack.rows).toHaveLength(0);
			} finally {
				await lix.close();
			}
		});

		test("executes coherent reads against an explicit branch without switching", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");

			try {
				const main = await lix.activeBranchId();
				await writeFile(lix, "/read-batch.txt", "main");
				const draft = await lix.createBranch({ name: "Read batch draft" });
				await lix.switchBranch({ branchId: draft.id });
				await writeFile(lix, "/read-batch.txt", "draft");
				await lix.switchBranch({ branchId: main });

				const batch = await lix.executeReadBatch({
					branchId: draft.id,
					statements: [
						{
							sql: "SELECT data FROM lix_file WHERE path = $1",
							params: ["/read-batch.txt"],
						},
						{ sql: "SELECT lix_active_branch_commit_id() AS commit_id" },
					],
				});

				expect(batch.branchId).toBe(draft.id);
				expect(batch.storageMutationRevision).toBeInstanceOf(Uint8Array);
				expect(batch.results).toHaveLength(2);
				const bytes = batch.results[0]?.rows[0]?.value("data").asBytes();
				expect(bytes && new TextDecoder().decode(bytes)).toBe("draft");
				expect(batch.results[1]?.rows[0]?.get("commit_id")).toBe(
					batch.branchCommitId,
				);
				expect(await lix.activeBranchId()).toBe(main);

				await expect(
					lix.executeReadBatch({
						branchId: draft.id,
						statements: [
							{
								sql: "DELETE FROM lix_file WHERE path = $1",
								params: ["/read-batch.txt"],
							},
						],
					}),
				).rejects.toMatchObject({
					name: "LixError",
					code: "LIX_INVALID_PARAM",
					details: { statementIndex: 0 },
				});
				await expect(
					lix.executeReadBatch({ branchId: draft.id, statements: [] }),
				).rejects.toMatchObject({
					code: "LIX_INVALID_ARGUMENT",
					details: { operation: "executeReadBatch", argument: "statements" },
				});
			} finally {
				await lix.close();
			}
		});

		test("creates, switches, previews, and merges branches", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");

			try {
				const main = await lix.activeBranchId();
				await writeFile(lix, "/contract-branch.txt", "main");
				const draft = await lix.createBranch({ name: "Contract draft" });
				await lix.switchBranch({ branchId: draft.id });
				await writeFile(lix, "/contract-branch.txt", "draft");
				expect(await readTextFile(lix, "/contract-branch.txt")).toBe("draft");

				await lix.switchBranch({ branchId: main });
				expect(await readTextFile(lix, "/contract-branch.txt")).toBe("main");
				const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
				expect(preview).toMatchObject({
					outcome: "fastForward",
					targetBranchId: main,
					sourceBranchId: draft.id,
					conflicts: [],
				});
				const merge = await lix.mergeBranch({ sourceBranchId: draft.id });
				expect(merge.outcome).toBe("fastForward");
				expect(merge.createdMergeCommitId).toBeNull();
				expect(await readTextFile(lix, "/contract-branch.txt")).toBe("draft");
			} finally {
				await lix.close();
			}
		});

		test("observe emits its initial snapshot and a committed update", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");
			const events = lix.observe(
				"SELECT key, value FROM lix_key_value WHERE key = $1",
				["contract-observe"],
			);

			const initial = await wait(events.next(), "initial observation");
			expect(initial?.sequence).toBe(0);
			expect(initial?.result.rows).toHaveLength(0);
			await lix.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["contract-observe", "updated"],
			);

			const update = await wait(events.next(), "updated observation");
			expect(update?.sequence).toBe(1);
			expect(update?.result.rows[0]?.toObject()).toEqual({
				key: "contract-observe",
				value: "updated",
			});
			events.close();
			await lix.close();
		});

		test("observe reports only the final committed batch state", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");
			const events = lix.observe(
				"SELECT key, value FROM lix_key_value WHERE key IN ($1, $2) ORDER BY key",
				["batch-observe-a", "batch-observe-b"],
			);

			try {
				const initial = await wait(events.next(), "initial batch observation");
				expect(initial?.result.rows).toHaveLength(0);

				const updatePromise = events.next();
				const batch = lix.executeBatch([
					{
						sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
						params: ["batch-observe-a", "first"],
					},
					{
						sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
						params: ["batch-observe-b", "second"],
					},
				]);
				const update = await wait(updatePromise, "batch observation");
				await wait(batch, "atomic batch");
				expect(update?.sequence).toBe(1);
				expect(update?.result.rows.map((row) => row.toObject())).toEqual([
					{ key: "batch-observe-a", value: "first" },
					{ key: "batch-observe-b", value: "second" },
				]);

				const noIntermediateUpdate = events.next();
				await expect(
					withTimeout(
						noIntermediateUpdate,
						"intermediate batch observation",
						100,
					),
				).rejects.toThrow(/timed out/);
				events.close();
				await expect(
					wait(noIntermediateUpdate, "closed batch observation"),
				).resolves.toBeUndefined();
			} finally {
				events.close();
				await lix.close();
			}
		});

		test("observe rejects concurrent next and close resolves the pending next", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");
			const events = lix.observe(
				"SELECT key FROM lix_key_value WHERE key = $1",
				["contract-observe-pending"],
			);

			await wait(events.next(), "initial observation");
			const pending = events.next();
			await expect(events.next()).rejects.toMatchObject({
				name: "LixError",
				code: "LIX_OBSERVE_NEXT_IN_FLIGHT",
			});
			events.close();
			await expect(wait(pending, "closed observation")).resolves.toBeUndefined();
			await expect(events.next()).resolves.toBeUndefined();
			await lix.close();
		});

		test(
			"executes the bundled CSV plugin",
			async () => {
				const { bundledPluginArchives, openLix } = await loadSdk();
				const lix = await wait(openLix(), "open memory Lix");
				try {
					const archives = await wait(
						bundledPluginArchives(),
						"load bundled plugin archives",
					);
					const csv = archives.find((plugin) => plugin.key === "plugin_csv");
					if (!csv) throw new Error("expected bundled CSV plugin");
					await writeBytes(
						lix,
						`/.lix/plugins/${csv.key}.lixplugin`,
						csv.archiveBytes,
					);
					const source = "name,age\nAda,36\nGrace,37\n";
					await writeBytes(
						lix,
						"/contract.csv",
						new TextEncoder().encode(source),
					);

					const rows = await lix.execute(
						"SELECT cells FROM csv_row ORDER BY order_key",
					);
					expect(rows.rows.map((row) => row.get("cells"))).toEqual([
						["name", "age"],
						["Ada", "36"],
						["Grace", "37"],
					]);
					expect(await readTextFile(lix, "/contract.csv")).toBe(source);
				} finally {
					await lix.close();
				}
			},
			120_000,
		);

		test("close is idempotent and rejects later operations", async () => {
			const { openLix } = await loadSdk();
			const lix = await wait(openLix(), "open memory Lix");
			const branchId = await lix.activeBranchId();

			await lix.close();
			await expect(lix.close()).resolves.toBeUndefined();
			await expect(lix.execute("SELECT 1")).rejects.toMatchObject({
				name: "LixError",
				code: "LIX_ERROR_CLOSED",
			});
			await expect(lix.switchBranch({ branchId })).rejects.toMatchObject({
				name: "LixError",
				code: "LIX_ERROR_CLOSED",
			});
		});
	});
}

type SqlExecutor = Pick<ContractLix, "execute">;

async function writeFile(
	lix: SqlExecutor,
	path: string,
	text: string,
): Promise<void> {
	await writeBytes(lix, path, new TextEncoder().encode(text));
}

async function writeBytes(
	lix: SqlExecutor,
	path: string,
	data: Uint8Array,
): Promise<void> {
	await lix.execute(
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) " +
			"ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		[path, data],
	);
}

async function readTextFile(lix: SqlExecutor, path: string): Promise<string> {
	const result = await lix.execute("SELECT data FROM lix_file WHERE path = $1", [path]);
	const bytes = result.rows[0]?.value("data").asBytes();
	if (!bytes) throw new Error(`expected file at ${path}`);
	return new TextDecoder().decode(bytes);
}

async function withTimeout<T>(
	promise: Promise<T>,
	operation: string,
	timeoutMs: number,
): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_resolve, reject) => {
				timer = setTimeout(
					() => reject(new Error(`${operation} timed out after ${timeoutMs}ms`)),
					timeoutMs,
				);
			}),
		]);
	} finally {
		if (timer !== undefined) clearTimeout(timer);
	}
}
