import {
	existsSync,
	mkdirSync,
	readFileSync,
	statSync,
	symlinkSync,
	unlinkSync,
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
import { addon } from "./native.js";

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

test("observe emits initial snapshot and committed updates", async () => {
	const lix = await openLix();
	const events = lix.observe(
		"SELECT key, value FROM lix_key_value WHERE key = $1 ORDER BY key",
		["js-observe"],
	);

	const initial = await events.next();
	expect(initial?.sequence).toBe(0);
	expect(typeof initial?.mutationSequence).toBe("number");
	expect(initial?.result.rows).toHaveLength(0);

	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('js-observe', 'v0')",
	);

	const update = await events.next();
	expect(update?.sequence).toBe(1);
	expect(update?.mutationSequence).toBeGreaterThanOrEqual(
		initial?.mutationSequence ?? 0,
	);
	expect(update?.result.rows).toHaveLength(1);
	expect(update?.result.rows[0]?.value("key").toJS()).toBe("js-observe");
	expect(update?.result.rows[0]?.value("value").toJS()).toBe("v0");

	events.close();
	await expect(events.next()).resolves.toBeUndefined();
	await lix.close();
});

test("observe rejects concurrent next calls on the same handle", async () => {
	const lix = await openLix();
	const events = lix.observe("SELECT key FROM lix_key_value WHERE key = $1", [
		"js-observe-concurrent",
	]);

	await events.next();
	const pending = events.next();
	await expect(events.next()).rejects.toMatchObject({
		code: "LIX_OBSERVE_NEXT_IN_FLIGHT",
	});

	events.close();
	await expect(withTimeout(pending)).resolves.toBeUndefined();
	await lix.close();
});

test("observe close resolves a pending next call", async () => {
	const lix = await openLix();
	const events = lix.observe("SELECT key FROM lix_key_value WHERE key = $1", [
		"js-observe-close",
	]);

	await events.next();
	const pending = events.next();
	events.close();

	await expect(withTimeout(pending)).resolves.toBeUndefined();
	await expect(events.next()).resolves.toBeUndefined();
	await lix.close();
});

test("observe close reliably resolves pending next calls", async () => {
	const lix = await openLix();

	for (let i = 0; i < 50; i += 1) {
		const events = lix.observe("SELECT key FROM lix_key_value WHERE key = $1", [
			`js-observe-close-stress-${i}`,
		]);

		await events.next();
		const pending = events.next();
		events.close();
		await expect(withTimeout(pending)).resolves.toBeUndefined();
	}

	await lix.close();
});

test("observe remains usable after next rejects", async () => {
	const lix = await openLix();
	const events = lix.observe(
		"SELECT key, value FROM lix_key_value WHERE key = $1",
		["js-observe-error"],
	);

	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('js-observe-error', 'rolled-back')",
	);
	await expect(events.next()).rejects.toMatchObject({ name: "LixError" });
	await tx.rollback();

	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('js-observe-error', 'after-error')",
	);
	const update = await events.next();
	expect(update?.sequence).toBe(0);
	expect(update?.result.rows[0]?.value("value").toJS()).toBe("after-error");

	events.close();
	await lix.close();
});

test.each([
	["memory", () => openLix()],
	[
		"sqlite",
		() => openLix({ backend: new SqliteBackend({ path: tempLixPath() }) }),
	],
	[
		"fs",
		() =>
			openLix({
				backend: new FsBackend({
					path: tempFsDir(),
					syncAllFiles: true,
				}),
			}),
	],
	[
		"fs-external-lix",
		() => {
			const dir = tempFsDir();
			mkdirSync(dir, { recursive: true });
			writeFileSync(join(dir, "matrix.md"), "matrix");
			return openLix({
				backend: new FsBackend({
					path: dir,
					lixDir: tempExternalLixDir(),
					syncAllFiles: true,
				}),
			});
		},
	],
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

test("native fs open returns a promise", async () => {
	const dir = tempFsDir();
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, "note.md"), "local");

	const native = addon.Lix.openFs(dir, undefined, true);
	expect(native).toBeInstanceOf(Promise);
	const lix = await native;
	await lix.close();
});

test("native awaited APIs return promises", async () => {
	const opened = addon.Lix.openMemory();
	expect(opened).toBeInstanceOf(Promise);
	const lix = await opened;

	const execute = lix.execute("SELECT 1 AS ok", []);
	expect(execute).toBeInstanceOf(Promise);
	expect((await execute).rows).toHaveLength(1);

	const activeBranchId = lix.activeBranchId();
	expect(activeBranchId).toBeInstanceOf(Promise);
	expect(await activeBranchId).toEqual(expect.any(String));

	const transaction = lix.beginTransaction();
	expect(transaction).toBeInstanceOf(Promise);
	const tx = await transaction;

	const txExecute = tx.execute("SELECT 2 AS ok", []);
	expect(txExecute).toBeInstanceOf(Promise);
	expect((await txExecute).rows).toHaveLength(1);

	const rollback = tx.rollback();
	expect(rollback).toBeInstanceOf(Promise);
	await rollback;

	const close = lix.close();
	expect(close).toBeInstanceOf(Promise);
	await close;
});

test("native actor preserves queued execution order", async () => {
	const lix = await openLix();
	await registerCrmTaskSchema(lix);

	const first = lix.execute(
		"INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
		["queued-1", "Queued 1", false, "{}"],
	);
	const second = lix.execute("UPDATE crm_task SET title = $1 WHERE id = $2", [
		"Queued 2",
		"queued-1",
	]);
	const third = lix.execute("SELECT title FROM crm_task WHERE id = $1", [
		"queued-1",
	]);

	await first;
	await second;
	expect(get(await third, "title")).toBe("Queued 2");
	await lix.close();
});

test("native actor settles commands queued behind close", async () => {
	const lix = await openLix();

	const firstClose = lix.close();
	const readAfterClose = lix.execute("SELECT 1 AS ok");
	const secondClose = lix.close();

	const settled = await settlesWithin(
		Promise.allSettled([firstClose, readAfterClose, secondClose]),
		1000,
	);
	expect(settled[0].status).toBe("fulfilled");
	expect(settled[1]).toMatchObject({
		status: "rejected",
		reason: { code: "LIX_ERROR_CLOSED" },
	});
	expect(settled[2].status).toBe("fulfilled");
});

test("fs backend imports local files and materializes lix_file writes", async () => {
	const dir = tempFsDir();
	mkdirSync(join(dir, "docs"), { recursive: true });
	writeFileSync(join(dir, "docs", "readme.md"), "local");

	const lix = await openLix({
		backend: new FsBackend({ path: dir, syncAllFiles: true }),
	});
	expect(statSync(join(dir, ".lix")).isDirectory()).toBe(true);
	expect(
		statSync(join(dir, ".lix", ".internal", "rocksdb")).isDirectory(),
	).toBe(true);

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

	const reopened = await openLix({
		backend: new FsBackend({ path: dir, syncAllFiles: true }),
	});
	const persisted = await reopened.execute(
		"SELECT data FROM lix_file WHERE directory_id IS NULL AND name = $1",
		["generated.md"],
	);
	expect(new TextDecoder().decode(get(persisted, "data") as Uint8Array)).toBe(
		"generated",
	);
	await reopened.close();
});

test("execute originKey is exposed on change and history surfaces without metadata", async () => {
	const lix = await openLix();
	const fileId = "origin-key-file";
	const metadata = { purpose: "metadata-only" };

	await lix.execute(
		"INSERT INTO lix_file (id, path, data, lixcol_metadata) VALUES ($1, $2, $3, $4)",
		[fileId, "/origin-key.md", new TextEncoder().encode("one\n"), metadata],
		{ originKey: "test-origin" },
	);
	const inserted = await currentFileChange(lix, fileId);
	const insertedHeadCommitId = await activeHeadCommitId(lix);
	expect(get(inserted, "origin_key")).toBe("test-origin");
	expect(get(inserted, "lixcol_metadata")).toEqual(metadata);
	expect(
		get(
			await lix.execute(
				"SELECT lixcol_origin_key FROM lix_file_history WHERE id = $1 AND lixcol_start_commit_id = $2",
				[fileId, insertedHeadCommitId],
			),
			"lixcol_origin_key",
		),
	).toBe("test-origin");
	expect(
		get(
			await lix.execute(
				"SELECT origin_key FROM lix_state_history WHERE change_id = $1 AND start_commit_id = $2",
				[get(inserted, "lixcol_change_id"), insertedHeadCommitId],
			),
			"origin_key",
		),
	).toBe("test-origin");

	await lix.execute("UPDATE lix_file SET data = $1 WHERE id = $2", [
		new TextEncoder().encode("two\n"),
		fileId,
	]);
	const unstamped = await currentFileChange(lix, fileId);
	expect(get(unstamped, "origin_key")).toBeNull();
	expect(get(unstamped, "lixcol_metadata")).toEqual(metadata);

	const transaction = await lix.beginTransaction();
	await transaction.execute(
		"UPDATE lix_file SET data = $1 WHERE id = $2",
		[new TextEncoder().encode("three\n"), fileId],
		{ originKey: "tx-origin" },
	);
	await transaction.commit();
	const txStamped = await currentFileChange(lix, fileId);
	const txHeadCommitId = await activeHeadCommitId(lix);
	expect(get(txStamped, "origin_key")).toBe("tx-origin");
	expect(
		get(
			await lix.execute(
				"SELECT origin_key FROM lix_state_history WHERE change_id = $1 AND start_commit_id = $2",
				[get(txStamped, "lixcol_change_id"), txHeadCommitId],
			),
			"origin_key",
		),
	).toBe("tx-origin");
	expect(get(txStamped, "lixcol_metadata")).toEqual(metadata);

	await lix.close();
});

test("fs backend with external lixDir imports the directory and writes normal files back", async () => {
	const dir = tempFsDir();
	const filePath = join(dir, "note.md");
	const siblingPath = join(dir, "sibling.md");
	mkdirSync(dir, { recursive: true });
	writeFileSync(filePath, "local");
	writeFileSync(siblingPath, "sibling");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: true,
		}),
	});

	const files = await lix.execute(
		"SELECT path, data FROM lix_file WHERE path IN ($1, $2) ORDER BY path",
		["/note.md", "/sibling.md"],
	);
	expect(files.rows.map((row) => row.get("path"))).toEqual([
		"/note.md",
		"/sibling.md",
	]);
	expect(new TextDecoder().decode(get(files, "data") as Uint8Array)).toBe(
		"local",
	);
	expect(existsSync(join(dir, ".lix"))).toBe(false);

	await lix.execute("UPDATE lix_file SET data = $1 WHERE path = $2", [
		new TextEncoder().encode("updated"),
		"/note.md",
	]);
	expect(readFileSync(filePath, "utf8")).toBe("updated");
	expect(readFileSync(siblingPath, "utf8")).toBe("sibling");

	await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
		"/generated.md",
		new TextEncoder().encode("generated"),
	]);
	expect(readFileSync(join(dir, "generated.md"), "utf8")).toBe("generated");
	expect(existsSync(join(dir, ".lix"))).toBe(false);

	writeFileSync(filePath, "external");
	await waitFor(async () => {
		const bytes = await readFile(lix, "/note.md");
		return bytes ? new TextDecoder().decode(bytes) : undefined;
	}, "external");

	await lix.close();
});

test("fs backend on-demand sync imports selected paths and lix-created files", async () => {
	const dir = tempFsDir();
	const includedPath = join(dir, "docs", "note.md");
	const excludedPath = join(dir, "docs", "sibling.md");
	const generatedPath = join(dir, "generated.md");
	mkdirSync(join(dir, "docs"), { recursive: true });
	writeFileSync(includedPath, "included");
	writeFileSync(excludedPath, "excluded");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await lix.importFilesystemPaths(["docs/note.md"]);

	const files = await lix.execute(
		"SELECT path FROM lix_file WHERE path IN ($1, $2) ORDER BY path",
		["/docs/note.md", "/docs/sibling.md"],
	);
	expect(files.rows.map((row) => row.get("path"))).toEqual(["/docs/note.md"]);

	await lix.execute("UPDATE lix_file SET data = $1 WHERE path = $2", [
		new TextEncoder().encode("updated"),
		"/docs/note.md",
	]);
	expect(readFileSync(includedPath, "utf8")).toBe("updated");
	expect(readFileSync(excludedPath, "utf8")).toBe("excluded");

	await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
		"/generated.md",
		new TextEncoder().encode("generated"),
	]);
	expect(readFileSync(generatedPath, "utf8")).toBe("generated");

	writeFileSync(includedPath, "external");
	await waitFor(async () => {
		const bytes = await readFile(lix, "/docs/note.md");
		return bytes ? new TextDecoder().decode(bytes) : undefined;
	}, "external");

	writeFileSync(excludedPath, "changed outside filter");
	expect(await readFile(lix, "/docs/sibling.md")).toBeUndefined();

	await lix.close();
});

test("fs backend imports existing files into a filtered filesystem", async () => {
	const dir = tempFsDir();
	const importedPath = join(dir, "docs", "opened.md");
	mkdirSync(join(dir, "docs"), { recursive: true });
	writeFileSync(importedPath, "opened");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});

	expect(await readFile(lix, "/docs/opened.md")).toBeUndefined();

	await lix.importFilesystemPaths(["docs/opened.md"]);
	expect(
		new TextDecoder().decode((await readFile(lix, "/docs/opened.md"))!),
	).toBe("opened");

	writeFileSync(importedPath, "edited");
	await waitFor(async () => {
		const bytes = await readFile(lix, "/docs/opened.md");
		return bytes ? new TextDecoder().decode(bytes) : undefined;
	}, "edited");

	await lix.close();
});

test("importFilesystemPaths validates paths and requires a filesystem backend", async () => {
	const memory = await openLix();
	await expect(memory.importFilesystemPaths(["note.md"])).rejects.toThrow(
		"filesystem backend",
	);
	await memory.close();

	const dir = tempFsDir();
	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await expect(lix.importFilesystemPaths([""])).rejects.toThrow(
		"non-empty strings",
	);
	await expect(lix.importFilesystemPaths(["docs/"])).rejects.toThrow(
		"file paths, not directory paths",
	);
	await lix.close();
});

test("fs backend syncAllFiles validates option shape", () => {
	const dir = tempFsDir();
	mkdirSync(dir, { recursive: true });

	expect(
		() =>
			new FsBackend({
				path: dir,
				storage: "memory",
			} as never),
	).toThrow("FsBackend storage is no longer supported");
	expect(() => new FsBackend({ path: dir } as never)).toThrow(
		"FsBackend syncAllFiles must be a boolean",
	);
	expect(() => new FsBackend({ path: dir, syncAllFiles: true })).not.toThrow();
	expect(() => new FsBackend({ path: dir, syncAllFiles: false })).not.toThrow();
	expect(
		() =>
			new FsBackend({
				path: dir,
				syncAllFiles: {} as boolean,
			}),
	).toThrow("FsBackend syncAllFiles must be a boolean");
});

test("fs backend on-demand sync imports no regular workspace files initially", async () => {
	const dir = tempFsDir();
	const lixDir = tempExternalLixDir();
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, "note.md"), "excluded");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir,
			syncAllFiles: false,
		}),
	});

	expect(await readFile(lix, "/note.md")).toBeUndefined();
	await writeFile(
		lix,
		"/.lix/app_data/test.bin",
		new TextEncoder().encode("internal"),
	);
	expect(readFileSync(join(lixDir, "app_data", "test.bin"), "utf8")).toBe(
		"internal",
	);
	expect(existsSync(join(dir, "note.md"))).toBe(true);

	await lix.close();
});

test("fs backend on-demand sync treats paths as exact filenames", async () => {
	const dir = tempFsDir();
	const filePath = join(dir, " spaced.md");
	mkdirSync(dir, { recursive: true });
	writeFileSync(filePath, "literal whitespace");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await lix.importFilesystemPaths([" spaced.md"]);

	const bytes = await readFile(lix, "/ spaced.md");
	expect(bytes ? new TextDecoder().decode(bytes) : undefined).toBe(
		"literal whitespace",
	);

	await lix.close();
});

test("fs backend on-demand sync does not create directories for missing imports", async () => {
	const dir = tempFsDir();
	mkdirSync(dir, { recursive: true });

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await lix.importFilesystemPaths(["missing/note.md"]);

	const directories = await lix.execute(
		"SELECT path FROM lix_directory WHERE path = $1",
		["/missing/"],
	);
	expect(directories.rows).toHaveLength(0);
	expect(existsSync(join(dir, "missing"))).toBe(false);

	await lix.close();
});

test("fs backend on-demand sync propagates deletion of imported files", async () => {
	const dir = tempFsDir();
	const filePath = join(dir, "note.md");
	mkdirSync(dir, { recursive: true });
	writeFileSync(filePath, "local");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await lix.importFilesystemPaths(["note.md"]);

	expect(await readFile(lix, "/note.md")).toBeDefined();

	unlinkSync(filePath);
	await waitFor(async () => await readFile(lix, "/note.md"), undefined);

	await lix.close();
});

test("fs backend on-demand sync does not delete excluded files through parent directories", async () => {
	const dir = tempFsDir();
	const includedPath = join(dir, "docs", "note.md");
	mkdirSync(join(dir, "docs"), { recursive: true });
	writeFileSync(includedPath, "local");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await lix.importFilesystemPaths(["docs/note.md"]);

	await writeFile(
		lix,
		"/docs/sibling.md",
		new TextEncoder().encode("outside filter"),
	);
	expect(await readFile(lix, "/docs/sibling.md")).toBeDefined();

	unlinkSync(includedPath);
	await waitFor(async () => await readFile(lix, "/docs/note.md"), undefined);

	const sibling = await readFile(lix, "/docs/sibling.md");
	expect(sibling ? new TextDecoder().decode(sibling) : undefined).toBe(
		"outside filter",
	);

	await lix.close();
});

test("fs backend on-demand sync matches directory paths by segment boundaries", async () => {
	const dir = tempFsDir();
	const excludedPath = join(dir, "foo", "file.md");
	const includedPath = join(dir, "foo-bar", "note.md");
	mkdirSync(join(dir, "foo"), { recursive: true });
	mkdirSync(join(dir, "foo-bar"), { recursive: true });
	writeFileSync(excludedPath, "outside filter");
	writeFileSync(includedPath, "local");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: false,
		}),
	});
	await lix.importFilesystemPaths(["foo-bar/note.md"]);

	expect(await readFile(lix, "/foo/file.md")).toBeUndefined();
	expect(readFileSync(excludedPath, "utf8")).toBe("outside filter");
	expect(readFileSync(includedPath, "utf8")).toBe("local");

	await lix.close();
});

test("fs backend with external lixDir materializes lix storage outside the workspace", async () => {
	const dir = tempFsDir();
	const lixDir = tempExternalLixDir();
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, "note.md"), "note");

	const lix = await openLix({
		backend: new FsBackend({ path: dir, lixDir, syncAllFiles: true }),
	});

	await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
		"/.lix/app_data/ephemeral.txt",
		new TextEncoder().encode("external"),
	]);
	expect(existsSync(join(dir, ".lix"))).toBe(false);
	expect(readFileSync(join(lixDir, "app_data", "ephemeral.txt"), "utf8")).toBe(
		"external",
	);

	await lix.close();
});

test("fs backend with external lixDir persists lix storage when reused", async () => {
	const dir = tempFsDir();
	const lixDir = tempExternalLixDir();
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, "note.md"), "first");

	const lix = await openLix({
		backend: new FsBackend({ path: dir, lixDir, syncAllFiles: true }),
	});
	await lix.execute("UPDATE lix_file SET data = $1 WHERE path = $2", [
		new TextEncoder().encode("second"),
		"/note.md",
	]);
	await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
		"/.lix/app_data/ephemeral.txt",
		new TextEncoder().encode("ephemeral"),
	]);
	await lix.close();

	const reopened = await openLix({
		backend: new FsBackend({ path: dir, lixDir, syncAllFiles: true }),
	});
	const bytes = await readFile(reopened, "/note.md");
	expect(bytes ? new TextDecoder().decode(bytes) : undefined).toBe("second");
	const ephemeral = await readFile(reopened, "/.lix/app_data/ephemeral.txt");
	expect(ephemeral ? new TextDecoder().decode(ephemeral) : undefined).toBe(
		"ephemeral",
	);
	expect(existsSync(join(dir, ".lix"))).toBe(false);

	await reopened.close();
});

test("fs backend with a fresh external lixDir reimports disk state without old lix storage", async () => {
	const dir = tempFsDir();
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, "note.md"), "first");

	const lix = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: true,
		}),
	});
	await lix.execute("UPDATE lix_file SET data = $1 WHERE path = $2", [
		new TextEncoder().encode("second"),
		"/note.md",
	]);
	await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
		"/.lix/app_data/ephemeral.txt",
		new TextEncoder().encode("ephemeral"),
	]);
	await lix.close();

	const reopened = await openLix({
		backend: new FsBackend({
			path: dir,
			lixDir: tempExternalLixDir(),
			syncAllFiles: true,
		}),
	});
	const bytes = await readFile(reopened, "/note.md");
	expect(bytes ? new TextDecoder().decode(bytes) : undefined).toBe("second");
	const ephemeral = await readFile(reopened, "/.lix/app_data/ephemeral.txt");
	expect(ephemeral).toBeUndefined();
	expect(existsSync(join(dir, ".lix"))).toBe(false);

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

		const lix = await openLix({
			backend: new FsBackend({ path: dir, syncAllFiles: true }),
		});
		const files = await lix.execute(
			"SELECT path FROM lix_file WHERE path IN ($1, $2, $3) ORDER BY path",
			["/target.txt", "/link.txt", "/linked-docs/readme.md"],
		);
		expect(files.rows.map((row) => row.get("path"))).toEqual(["/target.txt"]);

		const directories = await lix.execute(
			"SELECT path FROM lix_directory WHERE path IN ($1, $2) ORDER BY path",
			["/docs/", "/linked-docs/"],
		);
		expect(directories.rows.map((row) => row.get("path"))).toEqual(["/docs/"]);
		await lix.close();
	},
);

test("SQL file upsert and read use paths", async () => {
	const lix = await openLix();
	const data = new TextEncoder().encode("hello from SQL");

	expect(await readFile(lix, "/docs/missing.txt")).toBeUndefined();
	await writeFile(lix, "/docs/wrapper.txt", data);

	const stored = await readFile(lix, "/docs/wrapper.txt");
	expect(stored).toEqual(data);
	const sqlRead = await lix.execute(
		"SELECT data FROM lix_file WHERE path = $1",
		["/docs/wrapper.txt"],
	);
	expect(get(sqlRead, "data")).toEqual(data);

	const updated = new TextEncoder().encode("updated");
	await writeFile(lix, "/docs/wrapper.txt", updated);
	expect(await readFile(lix, "/docs/wrapper.txt")).toEqual(updated);

	await lix.close();
});

test("transaction SQL file writes use transaction execution", async () => {
	const lix = await openLix();
	const tx = await lix.beginTransaction();
	const data = new TextEncoder().encode("transactional");

	await writeFile(tx, "/tx.txt", data);
	expect(await readFile(tx, "/tx.txt")).toEqual(data);
	await tx.commit();
	expect(await readFile(lix, "/tx.txt")).toEqual(data);

	await lix.close();
});

test("SQL plugin archive upsert installs bundled plugin archive schemas", async () => {
	const lix = await openLix();
	const plugins = await bundledPluginArchives();

	for (const plugin of plugins) {
		await upsertPluginArchive(lix, plugin.key, plugin.archiveBytes);
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

test("SQL plugin archive upsert stores the archive and installs schemas", async () => {
	const lix = await openLix();
	const csvPlugin = (await bundledPluginArchives()).find(
		(plugin) => plugin.key === "plugin_csv",
	);
	if (!csvPlugin) {
		throw new Error("expected bundled CSV plugin");
	}

	await upsertPluginArchive(lix, csvPlugin.key, csvPlugin.archiveBytes);
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

type SqlExecutor = Pick<Lix, "execute">;

async function upsertPluginArchive(
	lix: SqlExecutor,
	key: string,
	archiveBytes: Uint8Array,
): Promise<void> {
	await writeFile(lix, `/.lix/plugins/${key}.lixplugin`, archiveBytes);
}

async function writeFile(
	lix: SqlExecutor,
	path: string,
	data: Uint8Array,
): Promise<void> {
	await lix.execute(
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) \
		 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		[path, data],
	);
}

async function readFile(
	lix: SqlExecutor,
	path: string,
): Promise<Uint8Array | undefined> {
	const result = await lix.execute(
		"SELECT data FROM lix_file WHERE path = $1",
		[path],
	);
	if (result.rows.length === 0) {
		return undefined;
	}
	return result.rows[0]?.value("data").asBytes() ?? new Uint8Array();
}

async function currentFileChange(
	lix: Lix,
	fileId: string,
): Promise<ExecuteResult> {
	return await lix.execute(
		`
			SELECT f.lixcol_change_id, f.lixcol_metadata, c.origin_key
			FROM lix_file AS f
			LEFT JOIN lix_change AS c ON c.id = f.lixcol_change_id
			WHERE f.id = $1
		`,
		[fileId],
	);
}

async function activeHeadCommitId(lix: Lix): Promise<string> {
	const result = await lix.execute("SELECT lix_active_branch_commit_id()");
	return String(get(result, "lix_active_branch_commit_id()"));
}

function get(result: ExecuteResult, column: string, rowIndex = 0): unknown {
	return result.rows[rowIndex]?.get(column);
}

async function withTimeout<T>(promise: Promise<T>, ms = 1_000): Promise<T> {
	let timeout: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_resolve, reject) => {
				timeout = setTimeout(
					() => reject(new Error(`timed out after ${ms}ms`)),
					ms,
				);
			}),
		]);
	} finally {
		if (timeout !== undefined) {
			clearTimeout(timeout);
		}
	}
}

async function settlesWithin<T>(promise: Promise<T>, ms = 1_000): Promise<T> {
	return withTimeout(promise, ms);
}

async function waitFor<T>(
	read: () => Promise<T>,
	expected: T,
	ms = 3_000,
): Promise<void> {
	const started = Date.now();
	let latest: T | undefined;
	do {
		latest = await read();
		if (latest === expected) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 50));
	} while (Date.now() - started < ms);
	expect(latest).toBe(expected);
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

function tempExternalLixDir(): string {
	const parent = tempFsDir();
	mkdirSync(parent, { recursive: true });
	return join(parent, ".lix");
}
