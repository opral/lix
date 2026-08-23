import {
	openLix,
	type LixStorageProvider,
	type LixStorageProviderRegistration,
	type LixStorageSpace,
} from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import {
	OPFS_RPC_CHANNEL,
	type OpfsRpcRequest,
	type OpfsRpcResponse,
} from "../js/rpc.js";
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

test("preserves mixed projection order across batched point reads", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-batched-read:${crypto.randomUUID()}`,
	});
	const provider = await openProvider(storage.lixStorage);
	const space: LixStorageSpace = {
		id: 41,
		name: "batched-read",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	const otherSpace: LixStorageSpace = {
		...space,
		id: 42,
		name: "batched-read-other-space",
	};
	const entries = Array.from({ length: 650 }, (_, index) => ({
		key: keyBytes(index),
		value: new Uint8Array([index & 0xff, index >>> 8]),
	}));
	const emptyValueEntry = { key: keyBytes(700), value: new Uint8Array() };
	const otherSpaceEntry = {
		key: entries[12]!.key,
		value: new Uint8Array([42]),
	};
	try {
		const write = await provider.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: entries.length * 6,
		});
		await write.putMany(space, [...entries, emptyValueEntry]);
		await write.putMany(otherSpace, [otherSpaceEntry]);
		await write.commit();

		const read = await provider.beginRead({
			consistency: "snapshot",
			durability: "visible",
		});
		const requests = [
			{
				space,
				keys: entries.slice(0, 299).map((entry) => entry.key),
				options: { projection: "fullValue" },
			},
			{
				space,
				keys: [entries[12]!.key, keyBytes(999)],
				options: { projection: "keyOnly" },
			},
			{
				space: otherSpace,
				keys: [entries[12]!.key, keyBytes(999)],
				options: { projection: "fullValue" },
			},
			{
				space,
				keys: entries.slice(299).map((entry) => entry.key),
				options: { projection: "keyOnly" },
			},
			{
				space,
				keys: [entries[12]!.key, emptyValueEntry.key],
				options: { projection: "fullValue" },
			},
		] satisfies Parameters<typeof read.getMany>[0];
		const values = await read.getMany(requests);

		const expected = [
			...entries.slice(0, 299).map((entry) => ({
				kind: "fullValue",
				value: entry.value,
			})),
			{ kind: "keyOnly" },
			null,
			{ kind: "fullValue", value: otherSpaceEntry.value },
			null,
			...entries.slice(299).map(() => ({ kind: "keyOnly" } as const)),
			{ kind: "fullValue", value: entries[12]!.value },
			{ kind: "fullValue", value: emptyValueEntry.value },
		];
		expect(values).toEqual(expected);
	} finally {
		await provider.close();
	}
});

test("does not replay a request after its owner response completes", async () => {
	const storageName = `lix-opfs-completed-request:${crypto.randomUUID()}`;
	const lix = await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	const channel = new BroadcastChannel(OPFS_RPC_CHANNEL);
	const request: OpfsRpcRequest = {
		kind: "request",
		requestId: crypto.randomUUID(),
		clientId: crypto.randomUUID(),
		storageName,
		operation: "open",
		payload: undefined,
	};
	const responses: OpfsRpcResponse[] = [];
	channel.onmessage = (event: MessageEvent<OpfsRpcResponse>) => {
		if (
			event.data.kind === "response" &&
			event.data.requestId === request.requestId
		) {
			responses.push(event.data);
		}
	};
	try {
		channel.postMessage(request);
		await expect
			.poll(() => responses.length, { timeout: 2_000 })
			.toBe(1);
		channel.postMessage(request);
		// A unique request from the same sender is a deterministic FIFO barrier.
		// The old owner re-executed the duplicate and emitted its second response
		// before this barrier; the completed-ID cache drops it instead.
		await postRpcAndWait(channel, {
			...request,
			requestId: crypto.randomUUID(),
		});
		expect(responses).toHaveLength(1);
	} finally {
		channel.close();
		await lix.close();
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

function keyBytes(index: number): Uint8Array {
	const key = new Uint8Array(4);
	new DataView(key.buffer).setUint32(0, index, false);
	return key;
}

async function openProvider(
	registration: LixStorageProviderRegistration,
): Promise<LixStorageProvider> {
	const module = (await import(/* @vite-ignore */ registration.moduleUrl)) as {
		createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
	};
	return module.createLixStorageProvider(registration.options);
}

function postRpcAndWait(
	channel: BroadcastChannel,
	request: OpfsRpcRequest,
): Promise<OpfsRpcResponse> {
	return withTimeout(
		new Promise((resolve) => {
			const listener = (event: MessageEvent<OpfsRpcResponse>) => {
				if (
					event.data.kind !== "response" ||
					event.data.requestId !== request.requestId
				) {
					return;
				}
				channel.removeEventListener("message", listener);
				resolve(event.data);
			};
			channel.addEventListener("message", listener);
			channel.postMessage(request);
		}),
		2_000,
	);
}
