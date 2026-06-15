import {
	mkdirSync,
	readFileSync,
	statSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import {
	bundledPluginArchives,
	FsBackend,
	openLix,
	SqliteBackend,
	Value,
	type ExecuteResult,
	type Lix,
} from "./index.js";

test("openLix exposes the lix-sdk e2e flow", async () => {
	const lix = await openLix();
	const mainBranchId = await lix.activeBranchId();

	await registerCrmTaskSchema(lix);
	await lix.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"task-1",
			"Draft native SDK flow",
			false,
			JSON.stringify({ priority: "high", tags: ["sdk", "native"] }),
		],
	);

	const projected = await lix.execute(
		"SELECT title, meta FROM crm_task WHERE id = $1",
		["task-1"],
	);
	expect(get(projected, "title")).toBe("Draft native SDK flow");
	expect(get(projected, "meta")).toEqual({
		priority: "high",
		tags: ["sdk", "native"],
	});
	expect(await taskDone(lix, "task-1")).toBe(false);

	const mainHead = await lix.execute("SELECT lix_active_branch_commit_id()");
	const mainHeadCommitId = get(mainHead, "lix_active_branch_commit_id()");
	expect(typeof mainHeadCommitId).toBe("string");

	const draft = await lix.createBranch({
		id: "native-draft-branch",
		name: "Native draft",
	});
	expect(draft).toMatchObject({
		id: "native-draft-branch",
		name: "Native draft",
		hidden: false,
		commitId: mainHeadCommitId,
	});

	await lix.switchBranch({ branchId: draft.id });
	await lix.execute("UPDATE crm_task SET done = $1 WHERE id = $2", [
		true,
		"task-1",
	]);
	expect(await taskDone(lix, "task-1")).toBe(true);

	await lix.switchBranch({ branchId: mainBranchId });
	expect(await taskDone(lix, "task-1")).toBe(false);

	const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
	expect(preview).toMatchObject({
		outcome: "fastForward",
		targetBranchId: mainBranchId,
		sourceBranchId: draft.id,
		changeStats: {
			total: 1,
			added: 0,
			modified: 1,
			removed: 0,
		},
		conflicts: [],
	});
	expect(await taskDone(lix, "task-1")).toBe(false);

	const merge = await lix.mergeBranch({ sourceBranchId: draft.id });
	expect(merge).toMatchObject({
		outcome: "fastForward",
		targetBranchId: mainBranchId,
		changeStats: {
			total: 1,
			added: 0,
			modified: 1,
			removed: 0,
		},
	});
	expect(merge.createdMergeCommitId).toBeNull();
	expect(await taskDone(lix, "task-1")).toBe(true);

	await lix.close();
	await lix.close();
	await expect(lix.activeBranchId()).rejects.toThrow(/closed/);
	await expect(lix.execute("SELECT 1")).rejects.toThrow(/closed/);
	await expect(lix.beginTransaction()).rejects.toThrow(/closed/);
	await expect(lix.createBranch({ name: "After close" })).rejects.toThrow(
		/closed/,
	);
	await expect(lix.switchBranch({ branchId: mainBranchId })).rejects.toThrow(
		/closed/,
	);
	await expect(
		lix.mergeBranchPreview({ sourceBranchId: mainBranchId }),
	).rejects.toThrow(/closed/);
	await expect(
		lix.mergeBranch({ sourceBranchId: mainBranchId }),
	).rejects.toThrow(/closed/);
});

test("committed writes survive close and reopen", async () => {
	const path = tempLixPath();
	const first = await openLix({ backend: new SqliteBackend({ path }) });

	await registerCrmTaskSchema(first);
	await first.execute(
		"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
		["persistent-task", "Persist before close", false],
	);
	await first.close();

	const second = await openLix({ backend: new SqliteBackend({ path }) });
	expect(await taskTitle(second, "persistent-task")).toBe(
		"Persist before close",
	);
	await second.close();
});

test.each([
	["memory", () => openLix()],
	[
		"sqlite",
		() => openLix({ backend: new SqliteBackend({ path: tempLixPath() }) }),
	],
	["fs", () => openLix({ backend: new FsBackend({ path: tempFsDir() }) })],
] as const)("core native flow works with %s backend", async (_name, open) => {
	const lix = await open();

	await registerCrmTaskSchema(lix);
	await lix.execute(
		"INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
		["matrix-task", "Matrix", false],
	);
	expect(await taskTitle(lix, "matrix-task")).toBe("Matrix");

	const tx = await lix.beginTransaction();
	await tx.execute("UPDATE crm_task SET done = $1 WHERE id = $2", [
		true,
		"matrix-task",
	]);
	await tx.commit();
	expect(await taskDone(lix, "matrix-task")).toBe(true);

	const bytes = new Uint8Array([0x10, 0x20, 0x30]);
	const blob = await lix.execute("SELECT $1 AS bytes", [bytes]);
	expect(get(blob, "bytes")).toEqual(bytes);

	await lix.close();
});

test("fs backend imports local files and materializes lix_file writes", async () => {
	const dir = tempFsDir();
	mkdirSync(join(dir, "docs"), { recursive: true });
	writeFileSync(join(dir, "docs", "readme.md"), "local");

	const lix = await openLix({ backend: new FsBackend({ path: dir }) });
	expect(statSync(join(dir, ".lix")).isDirectory()).toBe(true);
	expect(statSync(join(dir, ".lix", "db.sqlite")).isFile()).toBe(true);

	const imported = await lix.execute(
		"SELECT path, data FROM lix_file WHERE name = $1",
		["readme.md"],
	);
	expect(get(imported, "path")).toBe("/docs/readme.md");
	expect(new TextDecoder().decode(get(imported, "data") as Uint8Array)).toBe(
		"local",
	);

	await lix.execute(
		"INSERT INTO lix_file (directory_id, name, data) VALUES ($1, $2, $3)",
		[null, "generated.md", new TextEncoder().encode("generated")],
	);
	expect(readFileSync(join(dir, "generated.md"), "utf8")).toBe("generated");
	await lix.close();

	const reopened = await openLix({ backend: new FsBackend({ path: dir }) });
	const persisted = await reopened.execute(
		"SELECT data FROM lix_file WHERE directory_id IS NULL AND name = $1",
		["generated.md"],
	);
	expect(new TextDecoder().decode(get(persisted, "data") as Uint8Array)).toBe(
		"generated",
	);
	await reopened.close();
});

test.skipIf(process.platform === "win32")(
	"fs backend ignores symlinks",
	async () => {
		const dir = tempFsDir();
		mkdirSync(join(dir, "docs"), { recursive: true });
		writeFileSync(join(dir, "target.txt"), "target");
		writeFileSync(join(dir, "docs", "readme.md"), "nested");
		symlinkSync("target.txt", join(dir, "link.txt"));
		symlinkSync("docs", join(dir, "linked-docs"));

		const lix = await openLix({ backend: new FsBackend({ path: dir }) });
		const files = await lix.execute(
			"SELECT path FROM lix_file WHERE path IN ($1, $2, $3) ORDER BY path",
			["/target.txt", "/link.txt", "/linked-docs/readme.md"],
		);
		expect(files.rows.map((row) => row.get("path"))).toEqual(["/target.txt"]);

		const directories = await lix.execute(
			"SELECT path FROM lix_directory WHERE path IN ($1, $2) ORDER BY path",
			["/docs/", "/linked-docs/"],
		);
		expect(directories.rows.map((row) => row.get("path"))).toEqual([
			"/docs/",
		]);
		await lix.close();
	},
);

test("fs readFile and writeFile use paths", async () => {
	const lix = await openLix();
	const data = new TextEncoder().encode("hello from fs wrapper");

	expect(await lix.fs.readFile("/docs/missing.txt")).toBeUndefined();
	await lix.fs.writeFile("/docs/wrapper.txt", data);

	const stored = await lix.fs.readFile("/docs/wrapper.txt");
	expect(stored).toEqual(data);
	const sqlRead = await lix.execute(
		"SELECT data FROM lix_file WHERE path = $1",
		["/docs/wrapper.txt"],
	);
	expect(get(sqlRead, "data")).toEqual(data);

	const updated = new TextEncoder().encode("updated");
	await lix.fs.writeFile("/docs/wrapper.txt", updated);
	expect(await lix.fs.readFile("/docs/wrapper.txt")).toEqual(updated);

	await lix.close();
});

test("transaction fs wrappers use transaction SQL execution", async () => {
	const lix = await openLix();
	const tx = await lix.beginTransaction();
	const data = new TextEncoder().encode("transactional");

	await tx.fs.writeFile("/tx.txt", data);
	expect(await tx.fs.readFile("/tx.txt")).toEqual(data);
	await tx.commit();
	expect(await lix.fs.readFile("/tx.txt")).toEqual(data);

	await lix.close();
});

test("installPlugin installs bundled plugin archive schemas", async () => {
	const lix = await openLix();
	const plugins = await bundledPluginArchives();

	for (const plugin of plugins) {
		await lix.installPlugin(plugin.archiveBytes);
		const stored = await lix.execute(
			"SELECT data FROM lix_file WHERE id = $1",
			[`lix_plugin_archive::${plugin.key}`],
		);
		expect(get(stored, "data")).toEqual(plugin.archiveBytes);
	}

	const schemas = await lix.execute(
		"SELECT table_name \
		 FROM information_schema.tables \
		 WHERE table_name IN ($1, $2, $3, $4) \
		 ORDER BY table_name",
		["csv_row", "csv_table", "markdown_block", "markdown_document"],
	);
	expect(schemas.rows.map((row) => row.get("table_name"))).toEqual([
		"csv_row",
		"csv_table",
		"markdown_block",
		"markdown_document",
	]);

	await lix.close();
});

test("installPlugin stores the archive and installs schemas", async () => {
	const lix = await openLix();
	const csvPlugin = (await bundledPluginArchives()).find(
		(plugin) => plugin.key === "plugin_csv",
	);
	if (!csvPlugin) {
		throw new Error("expected bundled CSV plugin");
	}

	await lix.installPlugin(csvPlugin.archiveBytes);
	const stored = await lix.execute(
		"SELECT name, data FROM lix_file WHERE id = $1",
		[`lix_plugin_archive::${csvPlugin.key}`],
	);
	expect(get(stored, "name")).toBe(`${csvPlugin.key}.lixplugin`);
	expect(get(stored, "data")).toEqual(csvPlugin.archiveBytes);

	const schemas = await lix.execute(
		"SELECT table_name \
		 FROM information_schema.tables \
		 WHERE table_name IN ($1, $2) \
		 ORDER BY table_name",
		["csv_row", "csv_table"],
	);
	expect(schemas.rows.map((row) => row.get("table_name"))).toEqual([
		"csv_row",
		"csv_table",
	]);

	await lix.close();
});

test("execute supports UNION ALL without trapping", async () => {
	const lix = await openLix();
	const result = await lix.execute("SELECT 1 UNION ALL SELECT 2");

	expect(result.rows.map((row) => row.get("Int64(1)"))).toEqual([1, 2]);
	await lix.close();
});

test("UNION DISTINCT executes without trapping native", async () => {
	const lix = await openLix();

	const result = await lix.execute("SELECT 1 UNION SELECT 1");

	expect(result.rows.map((row) => row.get("Int64(1)"))).toEqual([1]);

	await lix.close();
});

test("INSERT SELECT UNION ALL executes without trapping", async () => {
	const lix = await openLix();

	const result = await lix.execute(
		"INSERT INTO lix_directory (id, name) SELECT 'u1' AS id, 'u1' AS name UNION ALL SELECT 'u2' AS id, 'u2' AS name",
	);

	expect(result.rowsAffected).toBe(2);
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
	await expect(tx.execute("SELECT 1")).rejects.toThrow(/closed/);
	await expect(tx.commit()).rejects.toThrow(/closed/);
	await expect(tx.rollback()).rejects.toThrow(/closed/);

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
	await expect(tx.rollback()).rejects.toThrow(/closed/);
	await expect(tx.commit()).rejects.toThrow(/closed/);
	await expect(tx.execute("SELECT 1")).rejects.toThrow(/closed/);

	await lix.close();
});

test("beginTransaction preserves handle after failed statement", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["failed-tx-task", "Before failure", false, JSON.stringify({ batch: 1 })],
	);
	await expect(
		tx.execute("SELECT entity_pk FROM lix_state_history"),
	).rejects.toMatchObject({
		code: "LIX_HISTORY_FILTER_REQUIRED",
	});
	await tx.rollback();

	const result = await lix.execute("SELECT id FROM crm_task WHERE id = $1", [
		"failed-tx-task",
	]);
	expect(result.rows).toHaveLength(0);

	await lix.close();
});

test("beginTransaction can continue after failed statement", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"continued-tx-task-1",
			"Before failure",
			false,
			JSON.stringify({ batch: 1 }),
		],
	);
	await expect(
		tx.execute("SELECT entity_pk FROM lix_state_history"),
	).rejects.toMatchObject({
		code: "LIX_HISTORY_FILTER_REQUIRED",
	});
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		[
			"continued-tx-task-2",
			"After failure",
			true,
			JSON.stringify({ batch: 1 }),
		],
	);
	await tx.commit();

	const committed = await lix.execute(
		"SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
		["continued-tx-task-1", "continued-tx-task-2"],
	);
	expect(committed.rows.map((row) => row.get("id"))).toEqual([
		"continued-tx-task-1",
		"continued-tx-task-2",
	]);
	await expect(tx.rollback()).rejects.toThrow(/closed/);

	await lix.close();
});

test("beginTransaction preserves handle after invalid JS parameter", async () => {
	const lix = await openLix();
	const tx = await lix.beginTransaction();

	await expect(
		tx.execute("SELECT $1", [undefined as never]),
	).rejects.toMatchObject({
		code: "LIX_INVALID_PARAM",
	});
	await tx.rollback();

	await lix.close();
});

test("beginTransaction blocks session reads and writes on the same handle", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["tx-only-task", "Inside tx", false, JSON.stringify({ batch: 1 })],
	);

	await expect(lix.execute("SELECT 1 AS ok")).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	await expect(
		lix.execute(
			"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
			["outside-task", "Outside tx", false, JSON.stringify({ batch: 1 })],
		),
	).rejects.toMatchObject({ code: "LIX_INVALID_TRANSACTION_STATE" });

	await tx.commit();

	const committed = await lix.execute(
		"SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
		["outside-task", "tx-only-task"],
	);
	expect(committed.rows.map((row) => row.get("id"))).toEqual(["tx-only-task"]);

	await lix.close();
});

test("close preserves lix handle when an active transaction blocks close", async () => {
	const lix = await openLix();
	const tx = await lix.beginTransaction();

	await expect(lix.close()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	const txResult = await tx.execute("SELECT 1 AS tx_ok");
	expect(get(txResult, "tx_ok")).toBe(1);
	await tx.rollback();

	const result = await lix.execute("SELECT 1 AS ok");
	expect(get(result, "ok")).toBe(1);

	await lix.close();
});

test("createBranch can start from an explicit commit id", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const baseHead = await lix.execute("SELECT lix_active_branch_commit_id()");
	const fromCommitId = get(baseHead, "lix_active_branch_commit_id()");
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

	const branch = await lix.createBranch({
		id: "native-from-explicit-commit",
		name: "Native from explicit commit",
		fromCommitId: fromCommitId as string,
	});
	expect(branch).toMatchObject({
		id: "native-from-explicit-commit",
		name: "Native from explicit commit",
		hidden: false,
		commitId: fromCommitId,
	});

	await lix.switchBranch({ branchId: branch.id });
	const projected = await lix.execute("SELECT id FROM crm_task WHERE id = $1", [
		"after-base",
	]);
	expect(projected.rows).toHaveLength(0);

	await lix.close();
});

test("engine errors cross the native boundary", async () => {
	const lix = await openLix();

	try {
		await lix.execute("SELECT entity_pk FROM lix_state_history");
		throw new Error("expected history query to fail");
	} catch (error) {
		expect(error).toMatchObject({
			name: "LixError",
			code: "LIX_HISTORY_FILTER_REQUIRED",
		});
		expect((error as { hint?: string }).hint).toContain(
			"lix_active_branch_commit_id()",
		);
	}

	await lix.close();
});

test("merge conflicts expose structured preview details and merge error", async () => {
	const lix = await openLix();
	const mainBranchId = await lix.activeBranchId();
	await registerCrmTaskSchema(lix);
	await lix.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["conflict-task", "Base", false, JSON.stringify({ priority: "normal" })],
	);
	const draft = await lix.createBranch({
		id: "native-conflict-draft",
		name: "Native conflict draft",
	});

	await lix.switchBranch({ branchId: draft.id });
	await lix.execute("UPDATE crm_task SET title = $1 WHERE id = $2", [
		"Draft",
		"conflict-task",
	]);

	await lix.switchBranch({ branchId: mainBranchId });
	await lix.execute("UPDATE crm_task SET title = $1 WHERE id = $2", [
		"Main",
		"conflict-task",
	]);

	const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
	expect(preview.conflicts).toHaveLength(1);
	expect(preview.conflicts[0]).toMatchObject({
		kind: "sameEntityChanged",
		schemaKey: "crm_task",
		entityPk: ["conflict-task"],
	});
	expect(preview.conflicts[0]?.target).toBeDefined();
	expect(preview.conflicts[0]?.source).toBeDefined();

	try {
		await lix.mergeBranch({ sourceBranchId: draft.id });
		throw new Error("expected merge conflict");
	} catch (error) {
		expect(error).toMatchObject({
			name: "LixError",
			code: "LIX_MERGE_CONFLICT",
		});
		if (!(error instanceof Error)) throw error;
		expect(error.message).toContain("tracked-state conflict");
	}

	await lix.close();
});

test("execute rejects invalid runtime arguments before native call", async () => {
	const lix = await openLix();
	const unsafeLix = lix as unknown as {
		execute(sql: unknown, params?: unknown): Promise<ExecuteResult>;
	};

	await expect(
		openLix({ backend: { path: tempLixPath() } } as never),
	).rejects.toThrow(/openLix\(\) requires/);
	await expect(openLix(null as never)).rejects.toThrow(
		/options must be an object/,
	);
	await expect(unsafeLix.execute(123, [])).rejects.toMatchObject({
		name: "LixError",
		code: "LIX_INVALID_ARGUMENT",
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
			message: /bigint is not a valid SQL parameter/,
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
			message: /symbol is not a valid SQL parameter/,
			actual: "symbol",
		},
		{
			name: "function",
			value: () => undefined,
			message: /function is not a valid SQL parameter/,
			actual: "function",
		},
		{
			name: "nested undefined",
			value: { nested: undefined },
			message: /undefined is not a valid SQL parameter/,
			actual: "undefined",
		},
		{
			name: "nested BigInt",
			value: { nested: [10n] },
			message: /bigint is not a valid SQL parameter/,
			actual: "bigint",
		},
		{
			name: "nested Symbol",
			value: { nested: Symbol("x") },
			message: /symbol is not a valid SQL parameter/,
			actual: "symbol",
		},
		{
			name: "nested function",
			value: { nested: () => undefined },
			message: /function is not a valid SQL parameter/,
			actual: "function",
		},
		{
			name: "nested Date",
			value: { nested: new Date("2026-01-02T03:04:05.000Z") },
			message: /Date is not a valid SQL parameter/,
			actual: "Date",
		},
		{
			name: "nested Uint8Array",
			value: { nested: new Uint8Array([1, 2, 3]) },
			message: /typed array SQL parameters must be top-level Uint8Array values/,
			actual: "Uint8Array",
		},
		{
			name: "Map",
			value: new Map([["key", "value"]]),
			message: /plain objects or arrays/,
			actual: "Map",
		},
		{
			name: "Set",
			value: new Set(["value"]),
			message: /plain objects or arrays/,
			actual: "Set",
		},
		{
			name: "RegExp",
			value: /value/,
			message: /plain objects or arrays/,
			actual: "RegExp",
		},
		{
			name: "class instance",
			value: new (class Task {
				id = "task-1";
			})(),
			message: /plain objects or arrays/,
			actual: "Task",
		},
		{
			name: "nested Map",
			value: { nested: new Map([["key", "value"]]) },
			message: /plain objects or arrays/,
			actual: "Map",
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

test("execute treats LixValue-shaped objects as JSON parameters", async () => {
	const lix = await openLix();

	const realObject = await lix.execute("SELECT $1 AS v", [
		{ kind: "real", value: 1.5 },
	]);
	expect(get(realObject, "v")).toEqual({ kind: "real", value: 1.5 });
	const blobObject = await lix.execute("SELECT $1 AS v", [
		{ kind: "blob", value: "AQID" },
	]);
	expect(get(blobObject, "v")).toEqual({
		kind: "blob",
		value: "AQID",
	});

	await lix.close();
});

test("execute round-trips Uint8Array blob parameters", async () => {
	const lix = await openLix();

	const bytes = new Uint8Array([0x01, 0x02, 0x03, 0xff]);
	const result = await lix.execute("SELECT $1 AS v", [bytes]);
	const value = result.rows[0]?.value("v");

	expect(get(result, "v")).toEqual(bytes);
	expect(value?.kind).toBe("blob");
	expect(value?.toJS()).toEqual(bytes);
	expect(value?.asBytes()).toEqual(bytes);
	const returnedBytes = value?.asBytes();
	if (!returnedBytes) throw new Error("expected blob bytes");
	returnedBytes[0] = 0x99;
	expect(value?.asBytes()).toEqual(bytes);

	await lix.close();
});

test("Value and Row return copies for structured values", async () => {
	const source = { nested: { ok: true } };
	const value = Value.json(source);
	source.nested.ok = false;

	const first = value.toJS() as { nested: { ok: boolean } };
	first.nested.ok = false;
	expect(value.toJS()).toEqual({ nested: { ok: true } });

	const lix = await openLix();
	const result = await lix.execute("SELECT $1 AS value", [
		{ nested: { ok: true } },
	]);
	const rowValue = result.rows[0]?.get("value") as { nested: { ok: boolean } };
	rowValue.nested.ok = false;
	expect(result.rows[0]?.get("value")).toEqual({ nested: { ok: true } });
	expect(result.rows[0]?.toObject()).toEqual({
		value: { nested: { ok: true } },
	});

	await lix.close();
});

test("execute accepts explicit Value parameters", async () => {
	const lix = await openLix();

	const real = await lix.execute("SELECT $1 AS v", [Value.real(1.5)]);
	expect(get(real, "v")).toBe(1.5);
	const blob = await lix.execute("SELECT $1 AS v", [
		Value.blob(new Uint8Array([0x01, 0x02, 0x03])),
	]);
	expect(get(blob, "v")).toEqual(new Uint8Array([0x01, 0x02, 0x03]));
	const source = new Uint8Array([0x04, 0x05, 0x06]);
	const explicitBlob = Value.blob(source);
	source[0] = 0xff;
	const copiedBlob = await lix.execute("SELECT $1 AS v", [explicitBlob]);
	expect(get(copiedBlob, "v")).toEqual(new Uint8Array([0x04, 0x05, 0x06]));

	await lix.close();
});

test("execute rejects invalid explicit Value parameters", async () => {
	const lix = await openLix();

	expect(() => Value.json({ nested: undefined } as never)).toThrow(
		/undefined is not a valid SQL parameter/,
	);
	expect(() => Value.json(new Map() as never)).toThrow(
		/plain objects or arrays/,
	);
	expect(() => Value.integer(1.5)).toThrow(
		/explicit Value contains an invalid native value/,
	);
	expect(() => Value.integer(Number.MAX_SAFE_INTEGER + 1)).toThrow(
		/explicit Value contains an invalid native value/,
	);
	expect(() => Value.from(Number.MAX_SAFE_INTEGER + 1)).toThrow(/safe integer/);
	expect(() => Value.real(Number.POSITIVE_INFINITY)).toThrow(
		/explicit Value contains an invalid native value/,
	);
	expect(() => Value.text("X\uD83DY")).toThrow(/well-formed UTF-16/);
	expect(() => Value.blob("AQID" as never)).toThrow(
		/explicit Value contains an invalid native value/,
	);

	await lix.close();
});

test("execute treats objects with non-LixValue kind as JSON parameters", async () => {
	const lix = await openLix();

	const result = await lix.execute("SELECT $1 AS value", [
		{ kind: "task", id: 1 },
	]);
	expect(get(result, "value")).toEqual({ kind: "task", id: 1 });

	await lix.close();
});

test("execute treats arrays as JSON parameters", async () => {
	const lix = await openLix();

	const result = await lix.execute("SELECT $1 AS value", [
		[1, "x", { ok: true }],
	]);
	expect(get(result, "value")).toEqual([1, "x", { ok: true }]);

	await lix.close();
});

test("execute rejects extra SQL parameters", async () => {
	const lix = await openLix();

	try {
		await lix.execute("SELECT $1 AS v", [1, 2]);
		throw new Error("expected extra params to fail");
	} catch (error) {
		expect(error).toMatchObject({
			name: "LixError",
			code: "LIX_INVALID_PARAM",
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
		"INSERT INTO lix_directory (id, parent_id, name) VALUES ($1, $2, $3)",
		["history-binary-dir", null, "history"],
	);
	await lix.execute(
		"INSERT INTO lix_file (id, directory_id, name, data) VALUES ($1, $2, $3, $4)",
		[
			"history-binary-native-repro",
			"history-binary-dir",
			"native-repro.bin",
			new Uint8Array([0x80, 0xff, 0x00]),
		],
	);

	const result = await lix.execute(
		"SELECT schema_key, snapshot_content \
		 FROM lix_state_history \
		 WHERE start_commit_id = lix_active_branch_commit_id()",
	);
	const directoryRow = result.rows.find(
		(row) => row.get("schema_key") === "lix_directory_descriptor",
	);
	expect(directoryRow?.get("snapshot_content")).toMatchObject({
		parent_id: null,
	});

	await lix.close();
});

async function registerCrmTaskSchema(lix: Lix): Promise<void> {
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
	const result = await lix.execute("SELECT done FROM crm_task WHERE id = $1", [
		taskId,
	]);
	expect(result.rows).toHaveLength(1);
	const done = get(result, "done");
	expect(typeof done).toBe("boolean");
	return done as boolean;
}

async function taskTitle(lix: Lix, taskId: string): Promise<string> {
	const result = await lix.execute("SELECT title FROM crm_task WHERE id = $1", [
		taskId,
	]);
	expect(result.rows).toHaveLength(1);
	const title = get(result, "title");
	expect(typeof title).toBe("string");
	return title as string;
}

function get(result: ExecuteResult, column: string, rowIndex = 0): unknown {
	return result.rows[rowIndex]?.get(column);
}

function tempLixPath(): string {
	const dir = join(tmpdir(), "lix-js-sdk-tests");
	mkdirSync(dir, { recursive: true });
	return join(
		dir,
		`lix-test-${Date.now()}-${Math.random().toString(16).slice(2)}.lix`,
	);
}

function tempFsDir(): string {
	const dir = join(tmpdir(), "lix-js-sdk-tests");
	mkdirSync(dir, { recursive: true });
	return join(
		dir,
		`lix-fs-test-${Date.now()}-${Math.random().toString(16).slice(2)}`,
	);
}
