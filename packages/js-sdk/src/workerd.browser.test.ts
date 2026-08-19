import { expect, test } from "vitest";
import initWasm, {
	openMemoryFromSnapshot,
} from "../dist/wasm/lix_js_sdk.js";
import * as wasmBindings from "../dist/wasm/lix_js_sdk.js";

const initialized = initWasm();
const SNAPSHOT_DRAFT_BRANCH_ID = "01920000-0000-7000-8000-000000000441";

async function openMemoryLix(options: { snapshot?: Uint8Array } = {}) {
	await initialized;
	return openMemoryFromSnapshot(
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
			id: SNAPSHOT_DRAFT_BRANCH_ID,
			name: "Snapshot draft",
		});
		await first.switchBranch({ branchId: SNAPSHOT_DRAFT_BRANCH_ID });
		await first.execute(
			"INSERT INTO lix_file (path, content) VALUES ($1, $2)",
			[
				{ kind: "text", value: "/snapshot.txt" },
				{ kind: "blob", value: null, blob: new TextEncoder().encode("saved") },
			],
		);
		branchBefore = await first.execute(
			"SELECT id, name FROM lix_branch WHERE id = $1",
			[{ kind: "text", value: SNAPSHOT_DRAFT_BRANCH_ID }],
		);
		fileBefore = await first.execute(
			"SELECT path, content, lixcol_change_id FROM lix_file WHERE path = '/snapshot.txt'",
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
		// Branch selection belongs to the opening session, not the repository
		// snapshot. A restored repository therefore starts on its default branch;
		// callers explicitly select another branch for that tab/session.
		expect(await restored.activeBranchId()).not.toBe(SNAPSHOT_DRAFT_BRANCH_ID);
		expect(
			await restored.execute(
				"SELECT id, name FROM lix_branch WHERE id = $1",
				[{ kind: "text", value: SNAPSHOT_DRAFT_BRANCH_ID }],
			),
		).toEqual(branchBefore);
		await restored.switchBranch({ branchId: SNAPSHOT_DRAFT_BRANCH_ID });
		const result = await restored.execute(
			"SELECT path, content, lixcol_change_id FROM lix_file WHERE path = '/snapshot.txt'",
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

test("Workerd WASM bindings do not export parseSqlScript", () => {
	expect("parseSqlScript" in wasmBindings).toBe(false);
});

test("Workerd snapshots reject malformed bytes", async () => {
	await expect(
		openMemoryLix({ snapshot: new Uint8Array([1, 2, 3]) }),
	).rejects.toThrow(/invalid in-memory snapshot/);
});

test("raw Workerd sessions have independent idempotent lifecycles", async () => {
	const primary = await openMemoryLix();
	const child = await primary.openAnotherSession({});
	const nested = await child.openAnotherSession({});

	await primary.close();
	await primary.close();
	expect((await child.execute("SELECT 1", noParams)).rows).toHaveLength(1);

	await nested.close();
	expect((await child.execute("SELECT 1", noParams)).rows).toHaveLength(1);
	await child.close();
	await child.close();
});

test("Workerd executeBatch accepts nested statement parameters", async () => {
	const lix = await openMemoryLix();
	try {
		const [inserted] = await lix.executeBatch([
			{
				sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
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
				sql: "UPDATE lix_file SET content = $1 WHERE path = $2 AND lixcol_change_id = $3",
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
