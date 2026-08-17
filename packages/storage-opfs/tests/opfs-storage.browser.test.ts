import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

test("persists a complete local Lix", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-test:${crypto.randomUUID()}`,
	});
	const first = await openLix({ storage });
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["durable-opfs", { value: 42 }],
	);
	await first.close();

	const second = await openLix({ storage });
	try {
		expect(
			(
				await second.execute("SELECT value FROM lix_key_value WHERE key = $1", [
					"durable-opfs",
				])
			).rows[0]?.get("value"),
		).toEqual({ value: 42 });
	} finally {
		await second.close();
	}
});

test("exclusively owns a name across handles", async () => {
	const name = `lix-opfs-owner-test:${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	await expect(openLix({ storage: new OpfsStorage({ name }) })).rejects.toThrow(
		"already open",
	);
	await first.close();
});

test("opens distinct repositories in parallel workers", async () => {
	const first = openLix({
		storage: new OpfsStorage({ name: `lix-opfs-parallel-a:${crypto.randomUUID()}` }),
	});
	const second = openLix({
		storage: new OpfsStorage({ name: `lix-opfs-parallel-b:${crypto.randomUUID()}` }),
	});
	const [left, right] = await Promise.all([first, second]);
	await Promise.all([left.close(), right.close()]);
});

test("rejects durable reads instead of weakening their semantics", async () => {
	const worker = new Worker(new URL("./durable-read.worker.ts", import.meta.url), {
		type: "module",
	});
	const result = new Promise<{ code: string | undefined }>((resolve, reject) => {
		worker.onmessage = (event: MessageEvent<
			| { ok: true; code: string | undefined }
			| { ok: false; error: string }
		>) => {
			worker.terminate();
			if (event.data.ok) resolve(event.data);
			else reject(new Error(event.data.error));
		};
		worker.onerror = (event) => {
			worker.terminate();
			reject(event.error ?? new Error(event.message));
		};
	});
	worker.postMessage({
		name: `lix-opfs-durable-read-test:${crypto.randomUUID()}`,
	});
	expect((await result).code).toBe("LIX_STORAGE_DURABILITY");
});
