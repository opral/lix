import { expect, test } from "vitest";
import { openLix, type Lix } from "./index.js";
import { loadTestPluginArchives } from "./plugin-test-archives.node.js";

const encode = (text: string) => new TextEncoder().encode(text);
const decode = (bytes: unknown) => new TextDecoder().decode(bytes as Uint8Array);

async function insertFile(lix: Lix, path: string, text: string) {
	const result = await lix.execute(
		"INSERT INTO lix_file (path, content) VALUES ($1, $2) RETURNING id",
		[path, encode(text)],
	);
	return result.rows[0]!.id as string;
}

async function readFile(lix: Lix, id: string) {
	const result = await lix.execute(
		"SELECT content FROM lix_file WHERE id = $1",
		[id],
	);
	return decode(result.rows[0]!.content);
}

async function installMarkdownPlugin(lix: Lix) {
	const plugin = (await loadTestPluginArchives()).find(
		(candidate) => candidate.key === "plugin_markdown",
	);
	if (!plugin) throw new Error("expected Markdown test plugin");
	await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
		`/.lix/plugins/${plugin.key}.lixplugin`,
		plugin.archiveBytes,
	]);
}

// opral/lix#1900
test("explicit transaction commits after an unrelated concurrent write", async () => {
	const lix = await openLix();
	const fileId = await insertFile(lix, "/a.md", "a");

	const tx = await lix.beginTransaction();
	await tx.execute("UPDATE lix_file SET content = $2 WHERE id = $1", [
		fileId,
		encode("b"),
	]);
	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('unrelated', 1)",
	);
	const receipt = await tx.commit();

	expect(receipt.commit).not.toBeNull();
	expect(await readFile(lix, fileId)).toBe("b");
	const unrelated = await lix.execute(
		"SELECT value FROM lix_key_value WHERE key = 'unrelated'",
	);
	expect(unrelated.rows).toEqual([{ value: 1 }]);
	await lix.close();
});

test("explicit transaction conflicts when the same row changed concurrently", async () => {
	const lix = await openLix();
	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('shared', 0)",
	);

	const tx = await lix.beginTransaction();
	await tx.execute("UPDATE lix_key_value SET value = 1 WHERE key = 'shared'");
	await lix.execute("UPDATE lix_key_value SET value = 2 WHERE key = 'shared'");

	await expect(tx.commit()).rejects.toMatchObject({
		code: "LIX_TRANSACTION_CONFLICT",
		details: {
			retryable: true,
			overlaps: [expect.objectContaining({ schemaKey: "lix_key_value" })],
		},
	});
	const shared = await lix.execute(
		"SELECT value FROM lix_key_value WHERE key = 'shared'",
	);
	expect(shared.rows).toEqual([{ value: 2 }]);
	await lix.close();
});

test("markdown file writes conflict on the same file and merge across files", async () => {
	const lix = await openLix();
	await installMarkdownPlugin(lix);
	const a = await insertFile(lix, "/a.md", "# A\n\nFirst.\n");
	const b = await insertFile(lix, "/b.md", "# B\n\nFirst.\n");

	const sameFile = await lix.beginTransaction();
	await sameFile.execute("UPDATE lix_file SET content = $2 WHERE id = $1", [
		a,
		encode("# A\n\nFrom the transaction.\n"),
	]);
	await lix.execute("UPDATE lix_file SET content = $2 WHERE id = $1", [
		a,
		encode("# A\n\nFirst.\n\nConcurrent paragraph.\n"),
	]);
	await expect(sameFile.commit()).rejects.toMatchObject({
		code: "LIX_TRANSACTION_CONFLICT",
	});
	expect(await readFile(lix, a)).toBe(
		"# A\n\nFirst.\n\nConcurrent paragraph.\n",
	);

	const otherFile = await lix.beginTransaction();
	await otherFile.execute("UPDATE lix_file SET content = $2 WHERE id = $1", [
		a,
		encode("# A\n\nFrom the transaction.\n"),
	]);
	await lix.execute("UPDATE lix_file SET content = $2 WHERE id = $1", [
		b,
		encode("# B\n\nConcurrent.\n"),
	]);
	await otherFile.commit();
	expect(await readFile(lix, a)).toBe("# A\n\nFrom the transaction.\n");
	expect(await readFile(lix, b)).toBe("# B\n\nConcurrent.\n");
	await lix.close();
});

test("lix.transaction() commits and returns the callback value", async () => {
	const lix = await openLix();
	const result = await lix.transaction(async (tx) => {
		await tx.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('helper', 'ok')",
		);
		return "done";
	});
	expect(result.value).toBe("done");
	expect(result.retries).toBe(0);
	expect(result.commit).not.toBeNull();
	await lix.close();
});

test("lix.transaction() reruns the callback after a conflict", async () => {
	const lix = await openLix();
	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('counter', 0)",
	);
	let attempts = 0;
	const result = await lix.transaction(async (tx) => {
		attempts += 1;
		const current = await tx.execute(
			"SELECT value FROM lix_key_value WHERE key = 'counter'",
		);
		const value = current.rows[0]!.value as number;
		if (attempts === 1) {
			// A concurrent increment lands between the read and the commit.
			await lix.execute(
				"UPDATE lix_key_value SET value = 10 WHERE key = 'counter'",
			);
		}
		await tx.execute("UPDATE lix_key_value SET value = $1 WHERE key = 'counter'", [
			value + 1,
		]);
		return value + 1;
	});
	expect(attempts).toBe(2);
	expect(result.retries).toBe(1);
	expect(result.value).toBe(11);
	const counter = await lix.execute(
		"SELECT value FROM lix_key_value WHERE key = 'counter'",
	);
	expect(counter.rows).toEqual([{ value: 11 }]);
	await lix.close();
});

test("lix.transaction() stops after maxRetries and rolls back other errors", async () => {
	const lix = await openLix();
	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('contended', 0)",
	);
	let attempts = 0;
	await expect(
		lix.transaction(
			async (tx) => {
				attempts += 1;
				await tx.execute(
					"UPDATE lix_key_value SET value = 1 WHERE key = 'contended'",
				);
				await lix.execute(
					"UPDATE lix_key_value SET value = $1 WHERE key = 'contended'",
					[100 + attempts],
				);
			},
			{ maxRetries: 1 },
		),
	).rejects.toMatchObject({
		code: "LIX_TRANSACTION_CONFLICT",
		details: { transactionRetryCount: 1, maxTransactionRetries: 1 },
	});
	expect(attempts).toBe(2);

	await expect(
		lix.transaction(async (tx) => {
			await tx.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ('never', 1)",
			);
			throw new Error("callback failed");
		}),
	).rejects.toThrow("callback failed");
	const never = await lix.execute(
		"SELECT value FROM lix_key_value WHERE key = 'never'",
	);
	expect(never.rows).toEqual([]);
	// The rolled-back attempt released the handle's transaction slot.
	await (await lix.beginTransaction()).rollback();

	await expect(
		lix.transaction(async () => undefined, { maxRetries: -1 }),
	).rejects.toMatchObject({ code: "LIX_INVALID_ARGUMENT" });
	await lix.close();
});
