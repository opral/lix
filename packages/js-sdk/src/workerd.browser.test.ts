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
