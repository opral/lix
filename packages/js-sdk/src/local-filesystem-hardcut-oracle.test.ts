import {
	mkdirSync,
	mkdtempSync,
	readFileSync,
	renameSync,
	rmSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { LocalFilesystem, openLix, type Lix } from "./index.js";

const decoder = new TextDecoder();
const encoder = new TextEncoder();

async function readLixFile(lix: Lix, path: string): Promise<Uint8Array | undefined> {
	const result = await lix.execute(
		"SELECT content FROM lix_file WHERE path = $1",
		[path],
	);
	return result.rows[0]?.value("content").asBytes();
}

async function waitForFile(
	lix: Lix,
	path: string,
	expected: Uint8Array | undefined,
): Promise<void> {
	const deadline = Date.now() + 10_000;
	for (;;) {
		const actual = await readLixFile(lix, path);
		if (
			actual?.byteLength === expected?.byteLength &&
			(actual === undefined ||
				actual.every((value, index) => value === expected?.[index]))
		) {
			return;
		}
		if (Date.now() >= deadline) {
			throw new Error(
				`timed out waiting for ${path}: actual=${String(actual)}, expected=${String(expected)}`,
			);
		}
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
}

async function activeHead(lix: Lix): Promise<string> {
	const result = await lix.execute("SELECT lix_active_branch_commit_id()");
	return String(result.rows[0]?.get("lix_active_branch_commit_id()"));
}

test("LocalFilesystem positional path owns automatic lifecycle and close drain", async () => {
	const root = mkdtempSync(join(tmpdir(), "lix-local-filesystem-hardcut-"));
	mkdirSync(join(root, "nested"), { recursive: true });
	writeFileSync(join(root, "seed.txt"), "seed");
	writeFileSync(join(root, "nested", "seed.bin"), Uint8Array.from([0, 1, 255]));
	mkdirSync(join(root, ".lix"), { recursive: true });
	writeFileSync(join(root, ".lix", "oracle-sentinel.bin"), "metadata");

	let lix: Lix | undefined;
	try {
		expect(() => new LocalFilesystem("")).toThrow(/non-empty path/i);
		expect(() => new LocalFilesystem(42 as never)).toThrow(/path/i);
		const storage = new LocalFilesystem(root);
		expect(storage.path).toBe(root);
		expect("importPaths" in storage).toBe(false);
		expect("syncDiskToLix" in storage).toBe(false);
		lix = await openLix({ storage });

		expect(decoder.decode(await readLixFile(lix, "/seed.txt"))).toBe("seed");
		expect(await readLixFile(lix, "/.lix/oracle-sentinel.bin")).toBeUndefined();

		writeFileSync(join(root, "created.txt"), "created");
		await waitForFile(lix, "/created.txt", encoder.encode("created"));
		writeFileSync(join(root, "seed.txt"), "modified");
		await waitForFile(lix, "/seed.txt", encoder.encode("modified"));
		unlinkSync(join(root, "nested", "seed.bin"));
		await waitForFile(lix, "/nested/seed.bin", undefined);
		renameSync(join(root, "created.txt"), join(root, "renamed.txt"));
		await waitForFile(lix, "/created.txt", undefined);
		await waitForFile(lix, "/renamed.txt", encoder.encode("created"));
		mkdirSync(join(root, "deep", "nested"), { recursive: true });
		const binary = Uint8Array.from([0, 255, 17, 0, 34]);
		writeFileSync(join(root, "deep", "nested", "data.bin"), binary);
		await waitForFile(lix, "/deep/nested/data.bin", binary);

		const stableHead = await activeHead(lix);
		await new Promise((resolve) => setTimeout(resolve, 1_500));
		expect(await activeHead(lix)).toBe(stableHead);

		const acceptedWrite = lix.execute(
			"INSERT INTO lix_file (path, content) VALUES ($1, $2)",
			["/close-drain.bin", Uint8Array.from([9, 8, 0, 7])],
		);
		const closing = lix.close();
		await Promise.all([acceptedWrite, closing]);
		expect(readFileSync(join(root, "close-drain.bin"))).toEqual(
			Buffer.from([9, 8, 0, 7]),
		);
		lix = undefined;

		const reopened = await openLix({ storage: new LocalFilesystem(root) });
		try {
			expect(await readLixFile(reopened, "/close-drain.bin")).toEqual(
				Uint8Array.from([9, 8, 0, 7]),
			);
			expect(decoder.decode(await readLixFile(reopened, "/seed.txt"))).toBe(
				"modified",
			);
		} finally {
			await reopened.close();
		}
	} finally {
		await lix?.close().catch(() => undefined);
		rmSync(root, { recursive: true, force: true });
	}
});
