import {
	openLix,
	type LixStorageProvider,
	type LixStorageProviderRegistration,
	type LixStorageSpace,
} from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { OpfsStorageClient } from "../js/client.js";
import {
	OPFS_RPC_CHANNEL,
	OPFS_RPC_PROTOCOL_VERSION,
	type OpfsChannelMessage,
	type OpfsRpcRequest,
	type OpfsRpcResponse,
} from "../js/rpc.js";
import { restoreSynchronousModeBestEffort } from "../js/sqlite-cleanup.js";
import {
	configureSqliteOpfsDurability,
	fenceSqliteOpfsDurability,
} from "../js/sqlite-durability.js";

type CheckpointExecOptions = {
	sql: string;
	rowMode: "array";
	resultRows: Array<Array<string | number | bigint | null>>;
};

test("does not replace a committed result with a synchronous cleanup error", () => {
	const database = {
		exec: () => {
			throw new Error("injected post-commit cleanup failure");
		},
	} as Parameters<typeof restoreSynchronousModeBestEffort>[0];

	expect(() => restoreSynchronousModeBestEffort(database, 1)).not.toThrow();
});

test("requires the SQLite modes used by the OPFS durability fence", () => {
	const pragmas: string[] = [];
	const database = {
		selectValue: (sql: string) => {
			pragmas.push(sql);
			return sql.includes("locking_mode") ? "exclusive" : "wal";
		},
	};

	configureSqliteOpfsDurability(database);
	expect(pragmas).toEqual([
		"PRAGMA locking_mode = EXCLUSIVE",
		"PRAGMA journal_mode = WAL",
	]);
	expect(() =>
		configureSqliteOpfsDurability({
			...database,
			selectValue: (sql) =>
				sql.includes("locking_mode") ? "normal" : "wal",
		}),
	).toThrowError(expect.objectContaining({ code: "LIX_STORAGE_DURABILITY" }));
	expect(() =>
		configureSqliteOpfsDurability({
			...database,
			selectValue: (sql) =>
				sql.includes("locking_mode") ? "exclusive" : "delete",
		}),
	).toThrowError(expect.objectContaining({ code: "LIX_STORAGE_DURABILITY" }));
});

test("accepts only a completed FULL WAL checkpoint as a durability fence", () => {
	let checkpointSql: string | undefined;
	const database = {
		exec: (options: string | CheckpointExecOptions) => {
			if (typeof options === "string") return;
			checkpointSql = options.sql;
			options.resultRows.push([0, 7, 7]);
		},
	};

	fenceSqliteOpfsDurability(database);
	expect(checkpointSql).toBe("PRAGMA wal_checkpoint(FULL)");

	for (const result of [
		[1, 7, 3],
		[0, 7, 3],
		[0, -1, -1],
	] as const) {
		expect(() =>
			fenceSqliteOpfsDurability({
				exec: (options: string | CheckpointExecOptions) => {
					if (typeof options !== "string") {
						options.resultRows.push([...result]);
					}
				},
			}),
		).toThrowError(expect.objectContaining({ code: "LIX_STORAGE_DURABILITY" }));
	}
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

test("joins one storage session generation across provider clients", async () => {
	const name = `lix-opfs-session-clients:${crypto.randomUUID()}`;
	const registration = new OpfsStorage({ name }).lixStorage;
	const [first, second] = await Promise.all([
		openProvider(registration),
		openProvider(registration),
	]);
	const space: LixStorageSpace = {
		id: 44,
		name: "session-clients",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	try {
		const [firstToken, secondToken] = await Promise.all([
			first.acquireSession(),
			second.acquireSession(),
		]);
		expect(secondToken).toBe(firstToken);

		const write = await first.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: 2,
			sessionToken: firstToken,
		});
		await write.putMany(space, [{ key: new Uint8Array([1]), value: new Uint8Array([2]) }]);
		await write.commit();

		const read = await second.beginRead({
			consistency: "latest",
			durability: "visible",
			sessionToken: secondToken,
		});
		expect(
			await read.getMany([
				{ space, keys: [new Uint8Array([1])], options: { projection: "fullValue" } },
			]),
		).toEqual([{ kind: "fullValue", value: new Uint8Array([2]) }]);
	} finally {
		await Promise.all([first.close(), second.close()]);
	}
});

test("preserves the storage session across an owner handoff", async () => {
	const name = `lix-opfs-session-handoff:${crypto.randomUUID()}`;
	const first = await runSessionHandoffWorker({ name, phase: "acquire" });
	const second = await runSessionHandoffWorker({
		name,
		phase: "reopen",
		expectedToken: first.token,
	});
	expect(second).toEqual({
		token: first.token,
		tokenlessFenced: true,
		writeCommitted: true,
	});
});

test("keeps a live client writable after its owner worker is replaced", async () => {
	const name = `lix-opfs-live-session-handoff:${crypto.randomUUID()}`;
	const channelName = `lix-opfs-live-session-channel:${crypto.randomUUID()}`;
	const ownerUrl = new URL("../dist/owner.js", import.meta.url);
	ownerUrl.searchParams.set("rpcChannel", channelName);
	let owner = new Worker(ownerUrl, { type: "module" });
	const client = await OpfsStorageClient.open(name, channelName);
	const space: LixStorageSpace = {
		id: 47,
		name: "live-session-handoff",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	try {
		const token = await client.acquireSession();
		owner.terminate();
		await expect
			.poll(async () => {
				const locks = await navigator.locks.query();
				return locks.held?.some((lock) => lock.name?.endsWith(name)) ?? false;
			})
			.toBe(false);

		owner = new Worker(ownerUrl, { type: "module" });
		const write = await client.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: 2,
			sessionToken: token,
		});
		await write.putMany(space, [
			{ key: new Uint8Array([1]), value: new Uint8Array([2]) },
		]);
		await write.commit();

		const read = await client.beginRead({
			consistency: "latest",
			durability: "visible",
			sessionToken: token,
		});
		expect(
			await read.getMany([
				{
					space,
					keys: [new Uint8Array([1])],
					options: { projection: "fullValue" },
				},
			]),
		).toEqual([{ kind: "fullValue", value: new Uint8Array([2]) }]);
	} finally {
		await client.close();
		owner.terminate();
	}
});

async function runSessionHandoffWorker(request: {
	name: string;
	phase: "acquire" | "reopen";
	expectedToken?: string;
}): Promise<{
	token: string;
	tokenlessFenced?: boolean;
	writeCommitted?: boolean;
}> {
	const worker = new Worker(
		new URL("./opfs-session-handoff.worker.ts", import.meta.url),
		{ type: "module" },
	);
	try {
		return await new Promise((resolve, reject) => {
			worker.onmessage = (event) => {
				const response = event.data as
					| { ok: true; result: Awaited<ReturnType<typeof runSessionHandoffWorker>> }
					| { ok: false; message: string };
				if (response.ok) resolve(response.result);
				else reject(new Error(response.message));
			};
			worker.onerror = (event) => reject(new Error(event.message));
			worker.postMessage(request);
		});
	} finally {
		worker.terminate();
	}
}

test("fences a tokenless write prepared before session acquisition", async () => {
	const provider = await openProvider(
		new OpfsStorage({
			name: `lix-opfs-prepared-tokenless:${crypto.randomUUID()}`,
		}).lixStorage,
	);
	const space: LixStorageSpace = {
		id: 45,
		name: "prepared-tokenless",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	try {
		const prepared = await provider.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: 2,
		});
		await prepared.putMany(space, [
			{ key: new Uint8Array([1]), value: new Uint8Array([2]) },
		]);

		const token = await provider.acquireSession();
		await expect(prepared.commit()).rejects.toMatchObject({
			code: "LIX_STORAGE_FENCED",
		});

		const read = await provider.beginRead({
			consistency: "latest",
			durability: "visible",
			sessionToken: token,
		});
		expect(
			await read.getMany([
				{ space, keys: [new Uint8Array([1])], options: { projection: "fullValue" } },
			]),
		).toEqual([null]);
	} finally {
		await provider.close();
	}
});

test("fences a commit prepared by a previous owner generation", async () => {
	const name = `lix-opfs-stale-writer:${crypto.randomUUID()}`;
	void new OpfsStorage({ name }).lixStorage;
	const client = await OpfsStorageClient.open(name);
	try {
		const sessionToken = await client.acquireSession();
		await expect(
			client.commit({
				deletes: [],
				puts: [],
				deleteRanges: [],
				immutablePuts: [],
				preconditions: [],
				strictDurability: false,
				stats: {
					putEntries: 0,
					deletedEntries: 0,
					deletedRanges: 0,
					writtenBytes: 0,
					storageCalls: 0,
				},
				sessionToken,
				ownerEpoch: "previous-owner",
			}),
		).rejects.toMatchObject({ code: "LIX_STORAGE_FENCED" });
	} finally {
		await client.close();
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
		protocolVersion: OPFS_RPC_PROTOCOL_VERSION,
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

test("keeps a complete SQL read coherent during cross-client commit churn", async () => {
	const name = `lix-opfs-read-churn:${crypto.randomUUID()}`;
	const monitor = new BroadcastChannel(OPFS_RPC_CHANNEL);
	const readRequests = new Set<string>();
	let expiredReads = 0;
	monitor.onmessage = (event: MessageEvent<OpfsChannelMessage>) => {
		const message = event.data;
		if (
			message.kind === "request" &&
			message.storageName === name &&
			["beginRead", "readMany", "scanPage"].includes(message.operation)
		) {
			readRequests.add(message.requestId);
		} else if (
			message.kind === "response" &&
			!message.ok &&
			readRequests.has(message.requestId) &&
			message.error.code === "LIX_STORAGE_READ_EXPIRED"
		) {
			expiredReads += 1;
		}
	};
	const lix = await openLix({ storage: new OpfsStorage({ name }) });
	const writer = await openProvider(new OpfsStorage({ name }).lixStorage);
	const writerSessionToken = await writer.acquireSession();
	const churnSpace: LixStorageSpace = {
		id: 2_000_000_000,
		name: "cross-client-read-churn",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	let releaseFirstCommit!: () => void;
	const firstCommit = new Promise<void>((resolve) => {
		releaseFirstCommit = resolve;
	});
	try {
		const commits = (async () => {
			for (let index = 0; index < 96; index += 1) {
				const write = await writer.beginWrite({
					awaitDurable: false,
					preconditions: [],
					batchCapacityHintBytes: 8,
					sessionToken: writerSessionToken,
				});
				await write.putMany(churnSpace, [
					{ key: keyBytes(index), value: new Uint8Array([index & 0xff]) },
				]);
				try {
					await write.commit();
				} finally {
					if (index === 0) releaseFirstCommit();
				}
			}
		})();

		const reads = (async () => {
			await firstCommit;
			for (let index = 0; index < 12; index += 1) {
				const result = await lix.execute(
					`SELECT 1 AS present FROM lix_directory WHERE lower(path) = lower($1)
				 UNION ALL
				 SELECT 1 AS present FROM lix_file WHERE lower(path) = lower($1)
				 LIMIT 1`,
					[`/absent-during-churn-${index}.md`],
				);
				expect(result.rows).toHaveLength(0);
			}
		})();
		const outcomes = await Promise.allSettled([commits, reads]);
		for (const outcome of outcomes) {
			if (outcome.status === "rejected") throw outcome.reason;
		}
		expect(expiredReads).toBeGreaterThan(1);
	} finally {
		monitor.close();
		await Promise.all([writer.close(), lix.close()]);
	}
});

test("durable read fences a causally prior ordinary commit", async () => {
	const name = `lix-opfs-durable-read-test:${crypto.randomUUID()}`;
	const provider = await openProvider(new OpfsStorage({ name }).lixStorage);
	const space: LixStorageSpace = {
		id: 43,
		name: "durable-read",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	const key = new TextEncoder().encode("causally-prior");
	const value = new TextEncoder().encode("ordinary-commit");
	try {
		const write = await provider.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: key.byteLength + value.byteLength,
		});
		await write.putMany(space, [{ key, value }]);
		await write.commit();

		const visibleRead = await provider.beginRead({
			consistency: "latest",
			durability: "visible",
		});
		const read = await provider.beginRead({
			consistency: "latest",
			durability: "durable",
		});
		// A durability fence changes physical persistence only. It must not
		// publish a logical storage change or invalidate the visible read.
		expect(read.snapshotCacheKey()).toBe(visibleRead.snapshotCacheKey());
		expect(
			await read.getMany([
				{ space, keys: [key], options: { projection: "fullValue" } },
			]),
		).toEqual([{ kind: "fullValue", value }]);
	} finally {
		await provider.close();
	}
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
