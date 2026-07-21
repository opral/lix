import { expect, test } from "vitest";
import initWasm, {
	openMemoryFromSnapshot,
} from "../dist/wasm/lix_js_sdk.js";

const initialized = initWasm();

async function openMemoryLix(options: { snapshot?: Uint8Array } = {}) {
	await initialized;
	return openMemoryFromSnapshot(
		async () => {
			throw new Error("plugins are unavailable in this test");
		},
		undefined,
		options.snapshot,
	);
}

const noParams: [] = [];

test("Workerd snapshots preserve exact Lix state across bindings", async () => {
	const first = await openMemoryLix();
	let snapshot: Uint8Array;
	let branchBefore;
	let fileBefore;
	let revisionBefore;
	try {
		await first.createBranch({
			id: "snapshot-draft",
			name: "Snapshot draft",
		});
		await first.switchBranch({ branchId: "snapshot-draft" });
		await first.execute(
			"INSERT INTO lix_file (path, data) VALUES ($1, $2)",
			[
				{ kind: "text", value: "/snapshot.txt" },
				{ kind: "blob", value: null, blob: new TextEncoder().encode("saved") },
			],
		);
		branchBefore = await first.execute(
			"SELECT id, name FROM lix_branch WHERE id = 'snapshot-draft'",
			noParams,
		);
		fileBefore = await first.execute(
			"SELECT path, data, lixcol_change_id FROM lix_file WHERE path = '/snapshot.txt'",
			noParams,
		);
		revisionBefore = await first.execute(
			"SELECT lix_active_branch_commit_id()",
			noParams,
		);
		snapshot = await first.exportSnapshot();
		expect(snapshot.byteLength).toBeGreaterThan(12);
	} finally {
		await first.close();
	}

	const restored = await openMemoryLix({ snapshot });
	try {
		expect(await restored.activeBranchId()).toBe("snapshot-draft");
		expect(
			await restored.execute(
				"SELECT id, name FROM lix_branch WHERE id = 'snapshot-draft'",
				noParams,
			),
		).toEqual(branchBefore);
		const result = await restored.execute(
			"SELECT path, data, lixcol_change_id FROM lix_file WHERE path = '/snapshot.txt'",
			noParams,
		);
		expect(result).toEqual(fileBefore);
		expect(
			await restored.execute("SELECT lix_active_branch_commit_id()", noParams),
		).toEqual(revisionBefore);
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.[0]).toMatchObject({
			kind: "text",
			value: "/snapshot.txt",
		});
		expect(result.rows[0]?.[1]).toMatchObject({ kind: "blob" });
		expect(result.rows[0]?.[2]).toMatchObject({ kind: "text" });
		expect(await restored.exportSnapshot()).toEqual(snapshot);
	} finally {
		await restored.close();
	}
});

test("Workerd snapshots reject malformed bytes", async () => {
	await expect(
		openMemoryLix({ snapshot: new Uint8Array([1, 2, 3]) }),
	).rejects.toThrow(/invalid in-memory snapshot/);
});

test("Workerd executeBatch accepts nested statement parameters", async () => {
	const lix = await openMemoryLix();
	try {
		const [inserted] = await lix.executeBatch([
			{
				sql: "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
				params: [
					{ kind: "text", value: "/batch.txt" },
					{
						kind: "blob",
						value: null,
						blob: new TextEncoder().encode("before"),
					},
				],
			},
		]);
		expect(inserted?.rowsAffected).toBe(1);

		const current = await lix.execute(
			"SELECT lixcol_change_id FROM lix_file WHERE path = '/batch.txt'",
			noParams,
		);
		const revision = current.rows[0]?.[0];
		expect(revision).toMatchObject({ kind: "text" });
		const [updated] = await lix.executeBatch([
			{
				sql: "UPDATE lix_file SET data = $1 WHERE path = $2 AND lixcol_change_id = $3",
				params: [
					{
						kind: "blob",
						value: null,
						blob: new TextEncoder().encode("after"),
					},
					{ kind: "text", value: "/batch.txt" },
					revision!,
				],
			},
		]);
		expect(updated?.rowsAffected).toBe(1);
	} finally {
		await lix.close();
	}
});

test("Workerd executeReadBatch uses an explicit branch and returns revision bytes", async () => {
	const lix = await openMemoryLix();
	try {
		const main = await lix.activeBranchId();
		await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
			{ kind: "text", value: "/read-batch.txt" },
			{
				kind: "blob",
				value: null,
				blob: new TextEncoder().encode("main"),
			},
		]);
		const draft = await lix.createBranch({ name: "Workerd read batch" });
		await lix.switchBranch({ branchId: draft.id });
		await lix.execute("UPDATE lix_file SET data = $1 WHERE path = $2", [
			{
				kind: "blob",
				value: null,
				blob: new TextEncoder().encode("draft"),
			},
			{ kind: "text", value: "/read-batch.txt" },
		]);
		await lix.switchBranch({ branchId: main });

		const batch = await lix.executeReadBatch(draft.id, [
			{
				sql: "SELECT data FROM lix_file WHERE path = $1",
				params: [{ kind: "text", value: "/read-batch.txt" }],
			},
			{ sql: "SELECT lix_active_branch_commit_id() AS commit_id", params: [] },
		]);

		expect(batch.branchId).toBe(draft.id);
		expect(batch.storageMutationRevision).toBeInstanceOf(Uint8Array);
		expect(batch.results[0]?.rows[0]?.[0]).toMatchObject({ kind: "blob" });
		expect(batch.results[1]?.rows[0]?.[0]).toMatchObject({
			kind: "text",
			value: batch.branchCommitId,
		});
		expect(await lix.activeBranchId()).toBe(main);
	} finally {
		await lix.close();
	}
});

test("Workerd executeReadBatch identifies raw binding validation errors", async () => {
	const lix = await openMemoryLix();
	try {
		await expect(
			lix.executeReadBatch("main", null as unknown as []),
		).rejects.toMatchObject({
			code: "LIX_INVALID_PARAM",
			message: expect.stringContaining("executeReadBatch"),
		});
	} finally {
		await lix.close();
	}
});
