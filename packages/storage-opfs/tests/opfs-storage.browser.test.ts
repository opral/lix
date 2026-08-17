import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { restoreSynchronousModeBestEffort } from "../js/sqlite-cleanup.js";

test("does not replace a committed result with a synchronous cleanup error", () => {
	const database = {
		exec: () => {
			throw new Error("injected post-commit cleanup failure");
		},
	} as Parameters<typeof restoreSynchronousModeBestEffort>[0];

	expect(() => restoreSynchronousModeBestEffort(database, 1)).not.toThrow();
});

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
	const suffix = crypto.randomUUID();
	const leftStorage = new OpfsStorage({ name: `lix-opfs-isolated-a:${suffix}` });
	const rightStorage = new OpfsStorage({ name: `lix-opfs-isolated-b:${suffix}` });
	const [left, right] = await Promise.all([
		openLix({ storage: leftStorage }),
		openLix({ storage: rightStorage }),
	]);
	try {
		await Promise.all([
			left.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["repository-isolation", "left"],
			),
			right.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["repository-isolation", "right"],
			),
		]);
		expect(await keyValue(left, "repository-isolation")).toBe("left");
		expect(await keyValue(right, "repository-isolation")).toBe("right");
	} finally {
		await Promise.all([left.close(), right.close()]);
	}

	const [reopenedLeft, reopenedRight] = await Promise.all([
		openLix({ storage: leftStorage }),
		openLix({ storage: rightStorage }),
	]);
	try {
		expect(await keyValue(reopenedLeft, "repository-isolation")).toBe("left");
		expect(await keyValue(reopenedRight, "repository-isolation")).toBe("right");
	} finally {
		await Promise.all([reopenedLeft.close(), reopenedRight.close()]);
	}
});

test("keeps branch state isolated and durable across reopen", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-branch-isolation:${crypto.randomUUID()}`,
	});
	const lix = await openLix({ storage });
	const mainBranchId = await lix.activeBranchId();
	await lix.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["branch-isolation", "main"],
	);
	const draft = await lix.createBranch({ name: "OPFS isolation draft" });
	await lix.switchBranch({ branchId: draft.id });
	await lix.execute(
		"UPDATE lix_key_value SET value = $1 WHERE key = $2",
		["draft", "branch-isolation"],
	);
	expect(await keyValue(lix, "branch-isolation")).toBe("draft");
	await lix.switchBranch({ branchId: mainBranchId });
	expect(await keyValue(lix, "branch-isolation")).toBe("main");
	await lix.close();

	const reopened = await openLix({ storage });
	try {
		expect(await keyValue(reopened, "branch-isolation")).toBe("main");
		await reopened.switchBranch({ branchId: draft.id });
		expect(await keyValue(reopened, "branch-isolation")).toBe("draft");
	} finally {
		await reopened.close();
	}
});

test("concurrent engines converge without losing divergent writes", async () => {
	const name = `lix-opfs-divergent-clients:${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	const second = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		await Promise.all([
			first.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["divergent-left", "left"],
			),
			second.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				["divergent-right", "right"],
			),
		]);

		const [firstRows, secondRows] = await Promise.all([
			keyValues(first, ["divergent-left", "divergent-right"]),
			keyValues(second, ["divergent-left", "divergent-right"]),
		]);
		expect(firstRows).toEqual([
			["divergent-left", "left"],
			["divergent-right", "right"],
		]);
		expect(secondRows).toEqual(firstRows);

		await Promise.all([
			first.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
				["same-key", "left"],
			),
			second.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
				["same-key", "right"],
			),
		]);
		const [firstWinner, secondWinner] = await Promise.all([
			keyValue(first, "same-key"),
			keyValue(second, "same-key"),
		]);
		expect(["left", "right"]).toContain(firstWinner);
		expect(secondWinner).toBe(firstWinner);
	} finally {
		await Promise.all([first.close(), second.close()]);
	}
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

async function keyValue(
	lix: Awaited<ReturnType<typeof openLix>>,
	key: string,
): Promise<unknown> {
	return (
		await lix.execute("SELECT value FROM lix_key_value WHERE key = $1", [key])
	).rows[0]?.get("value");
}

async function keyValues(
	lix: Awaited<ReturnType<typeof openLix>>,
	keys: [string, string],
): Promise<unknown[][]> {
	const result = await lix.execute(
		"SELECT key, value FROM lix_key_value WHERE key IN ($1, $2) ORDER BY key",
		keys,
	);
	return result.rows.map((row) => [row.get("key"), row.get("value")]);
}
