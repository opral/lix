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

test("shares a name across Lix workers", async () => {
	const name = `lix-opfs-shared-test:${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	const second = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		await first.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["shared-value", { value: 1 }],
		);
		expect(
			(
				await second.execute("SELECT value FROM lix_key_value WHERE key = $1", [
					"shared-value",
				])
			).rows[0]?.get("value"),
		).toEqual({ value: 1 });
		await second.execute(
			"UPDATE lix_key_value SET value = $1 WHERE key = $2",
			[{ value: 2 }, "shared-value"],
		);
		expect(
			(
				await first.execute("SELECT value FROM lix_key_value WHERE key = $1", [
					"shared-value",
				])
			).rows[0]?.get("value"),
		).toEqual({ value: 2 });
	} finally {
		await Promise.all([first.close(), second.close()]);
	}
});

test("wakes lix.observe after another Lix worker commits", async () => {
	const name = `lix-opfs-observe-test:${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	const second = await openLix({ storage: new OpfsStorage({ name }) });
	const observation = second.observe(
		"SELECT value FROM lix_key_value WHERE key = $1",
		["observed-value"],
	);
	try {
		const initial = await observation.next();
		expect(initial?.result.rows).toHaveLength(0);

		const changed = observation.next();
		await first.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["observed-value", { value: 1 }],
		);

		const update = await withTimeout(changed, 2_000);
		expect(update?.result.rows[0]?.get("value")).toEqual({ value: 1 });
	} finally {
		observation.close();
		await Promise.all([first.close(), second.close()]);
	}
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
	const registration = new OpfsStorage({
		name: `lix-opfs-durable-read-test:${crypto.randomUUID()}`,
	}).lixStorage;
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
		registration,
	});
	expect((await result).code).toBe("LIX_STORAGE_DURABILITY");
});

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		const timeout = setTimeout(
			() => reject(new Error(`timed out after ${timeoutMs}ms`)),
			timeoutMs,
		);
		promise.then(
			(value) => {
				clearTimeout(timeout);
				resolve(value);
			},
			(error) => {
				clearTimeout(timeout);
				reject(error);
			},
		);
	});
}
