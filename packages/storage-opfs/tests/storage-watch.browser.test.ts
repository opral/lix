import { openLix, type LixStorageProvider, type LixStorageProviderRegistration } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { OpfsStorageClient } from "../js/client.js";
import {
	OPFS_RPC_CHANNEL,
	type OpfsRpcRequest,
	type OpfsRpcResponse,
} from "../js/rpc.js";

test("storage watch resolves after a commit from another engine", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-watch-external:${crypto.randomUUID()}`,
	});
	const lix = await openLix({ storage });
	const provider = await openProvider(storage.lixStorage);
	const watch = await provider.watchForChanges();
	try {
		const changed = watch.changed();
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["external-watch", 1],
		);
		await expect(withTimeout(changed, 2_000)).resolves.toBeUndefined();
	} finally {
		watch.close();
		await Promise.all([provider.close(), lix.close()]);
	}
});

test("storage watch retains a change until changed is called", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-watch-registration-gap:${crypto.randomUUID()}`,
	});
	const lix = await openLix({ storage });
	const provider = await openProvider(storage.lixStorage);
	const watch = await provider.watchForChanges();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["before-changed", 1],
		);
		await expect(withTimeout(watch.changed(), 2_000)).resolves.toBeUndefined();
	} finally {
		watch.close();
		await Promise.all([provider.close(), lix.close()]);
	}
});

test("storage watch coalesces multiple unseen commits", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-watch-coalescing:${crypto.randomUUID()}`,
	});
	const lix = await openLix({ storage });
	const provider = await openProvider(storage.lixStorage);
	const watch = await provider.watchForChanges();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["coalesced-a", 1],
		);
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["coalesced-b", 2],
		);
		await expect(withTimeout(watch.changed(), 2_000)).resolves.toBeUndefined();

		const noSecondNotification = watch.changed();
		await expect(staysPending(noSecondNotification, 100)).resolves.toBe(true);
		watch.close();
		await expect(noSecondNotification).rejects.toMatchObject({
			code: "LIX_STORAGE_CLOSED",
		});
	} finally {
		watch.close();
		await Promise.all([provider.close(), lix.close()]);
	}
});

test("closing a watch or provider rejects its pending waiter", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-watch-close:${crypto.randomUUID()}`,
	});
	const provider = await openProvider(storage.lixStorage);
	const explicitlyClosed = await provider.watchForChanges();
	const explicitPending = explicitlyClosed.changed();
	explicitlyClosed.close();
	await expect(explicitPending).rejects.toMatchObject({
		code: "LIX_STORAGE_CLOSED",
	});

	const providerClosed = await provider.watchForChanges();
	const providerPending = providerClosed.changed();
	await provider.close();
	await expect(providerPending).rejects.toMatchObject({
		code: "LIX_STORAGE_CLOSED",
	});
});

test("heartbeat state refresh repairs a missed broadcast", async () => {
	const channelName = `lix-opfs-watch-heartbeat:${crypto.randomUUID()}`;
	const storageName = `heartbeat-storage:${crypto.randomUUID()}`;
	const ownerEpoch = crypto.randomUUID();
	let generation = 0;
	const fakeOwner = new BroadcastChannel(channelName);
	fakeOwner.onmessage = (event: MessageEvent<OpfsRpcRequest>) => {
		const request = event.data;
		if (!request || request.kind !== "request" || request.storageName !== storageName) return;
		const response: OpfsRpcResponse = {
			kind: "response",
			requestId: request.requestId,
			clientId: request.clientId,
			ok: true,
			result:
				request.operation === "open" || request.operation === "heartbeat"
					? { ownerEpoch, generation }
					: undefined,
		};
		fakeOwner.postMessage(response);
	};

	const client = await OpfsStorageClient.open(storageName, channelName);
	const watch = await client.watchForChanges();
	try {
		generation = 1;
		const changed = watch.changed();
		await client.refreshState();
		await expect(withTimeout(changed, 1_000)).resolves.toBeUndefined();
	} finally {
		watch.close();
		await client.close();
		fakeOwner.close();
	}
});

test("fails fast when an old owner has no storage session protocol", async () => {
	const channelName = `lix-opfs-old-owner:${crypto.randomUUID()}`;
	const storageName = `old-owner-storage:${crypto.randomUUID()}`;
	const fakeOldOwner = new BroadcastChannel(channelName);
	fakeOldOwner.onmessage = (event: MessageEvent<OpfsRpcRequest>) => {
		const request = event.data;
		if (!request || request.kind !== "request" || request.storageName !== storageName) return;
		const response: OpfsRpcResponse = {
			kind: "response",
			requestId: request.requestId,
			clientId: request.clientId,
			ok: true,
			result:
				request.operation === "open"
					? { ownerEpoch: crypto.randomUUID(), generation: 0 }
					: undefined,
		};
		fakeOldOwner.postMessage(response);
	};

	const client = await OpfsStorageClient.open(storageName, channelName);
	try {
		await expect(withTimeout(client.acquireSession(), 1_000)).rejects.toMatchObject({
			code: "LIX_STORAGE_UNSUPPORTED",
		});
	} finally {
		await client.close();
		fakeOldOwner.close();
	}
});

test("does not send current requests onto the legacy RPC bus", async () => {
	const storageName = `legacy-bus-isolation:${crypto.randomUUID()}`;
	const legacyChannel = new BroadcastChannel("lix-js:storage-opfs:v1");
	let legacyRequests = 0;
	legacyChannel.onmessage = (event: MessageEvent<OpfsRpcRequest>) => {
		if (event.data?.kind === "request" && event.data.storageName === storageName) {
			legacyRequests += 1;
		}
	};
	const worker = new Worker(new URL("../dist/owner.js", import.meta.url), {
		type: "module",
	});
	try {
		const client = await OpfsStorageClient.open(storageName);
		try {
			await client.acquireSession();
			await new Promise((resolve) => setTimeout(resolve, 100));
			expect(legacyRequests).toBe(0);
		} finally {
			await client.close();
		}
	} finally {
		legacyChannel.close();
		worker.terminate();
	}
});

test("rejects an incompatible owner of the stable repository lock without hanging", async () => {
	const storageName = `old-lock-owner:${crypto.randomUUID()}`;
	let signalLockAcquired!: () => void;
	let releaseOldLock!: () => void;
	const lockAcquired = new Promise<void>((resolve) => {
		signalLockAcquired = resolve;
	});
	const oldLockReleased = new Promise<void>((resolve) => {
		releaseOldLock = resolve;
	});
	const oldOwner = navigator.locks.request(
		`lix:opfs-sqlite:${storageName}`,
		{ mode: "exclusive" },
		async (lock) => {
			if (!lock) throw new Error("failed to acquire simulated old owner lock");
			signalLockAcquired();
			await oldLockReleased;
		},
	);
	await lockAcquired;

	const worker = new Worker(new URL("../dist/owner.js", import.meta.url), {
		type: "module",
	});
	let oldLockWasReleased = false;
	try {
		await expect(
			withTimeout(OpfsStorageClient.open(storageName), 3_000),
		).rejects.toMatchObject({
			code: "LIX_STORAGE_UNSUPPORTED",
		});

		releaseOldLock();
		await oldOwner;
		oldLockWasReleased = true;
		const client = await OpfsStorageClient.open(storageName);
		try {
			await expect(client.acquireSession()).resolves.toMatch(/^(0|[1-9]\d*)$/);
		} finally {
			await client.close();
		}
	} finally {
		if (!oldLockWasReleased) {
			releaseOldLock();
			await oldOwner;
		}
		worker.terminate();
	}
});

test("bounds owner discovery when no current worker answers", async () => {
	const channelName = `lix-opfs-no-owner:${crypto.randomUUID()}`;
	const startedAt = performance.now();
	await expect(
		withTimeout(
			OpfsStorageClient.open(`no-owner:${crypto.randomUUID()}`, channelName),
			3_000,
		),
	).rejects.toMatchObject({ code: "LIX_STORAGE_IO" });
	expect(performance.now() - startedAt).toBeLessThan(3_000);
});

test("accepted ownership extends the deadline for a slow backend startup", async () => {
	const channelName = `lix-opfs-slow-owner:${crypto.randomUUID()}`;
	const storageName = `slow-owner-storage:${crypto.randomUUID()}`;
	const fakeOwner = new BroadcastChannel(channelName);
	const answered = new Set<string>();
	fakeOwner.onmessage = (event: MessageEvent<OpfsRpcRequest>) => {
		const request = event.data;
		if (
			request?.kind === "request" &&
			request.operation === "close" &&
			request.storageName === storageName
		) {
			fakeOwner.postMessage({
				kind: "response",
				requestId: request.requestId,
				clientId: request.clientId,
				ok: true,
				result: undefined,
			});
			return;
		}
		if (
			!request ||
			request.kind !== "request" ||
			request.operation !== "open" ||
			request.storageName !== storageName ||
			answered.has(request.requestId)
		) {
			return;
		}
		answered.add(request.requestId);
		fakeOwner.postMessage({
			kind: "accepted",
			requestId: request.requestId,
			clientId: request.clientId,
		});
		setTimeout(() => {
			fakeOwner.postMessage({
				kind: "response",
				requestId: request.requestId,
				clientId: request.clientId,
				ok: true,
				result: { ownerEpoch: crypto.randomUUID(), generation: 0 },
			});
		}, 2_200);
	};

	const client = await OpfsStorageClient.open(storageName, channelName);
	await client.close();
	fakeOwner.close();
});

test("only the repository owner answers protocol mismatch across relay workers", async () => {
	const storageName = `owner-protocol-authority:${crypto.randomUUID()}`;
	const ownerUrl = new URL("../dist/owner.js", import.meta.url);
	const workers = [
		new Worker(ownerUrl, { type: "module" }),
		new Worker(ownerUrl, { type: "module" }),
	];
	const channel = new BroadcastChannel(OPFS_RPC_CHANNEL);
	const clientId = crypto.randomUUID();
	const requestId = crypto.randomUUID();
	const responses: OpfsRpcResponse[] = [];
	channel.onmessage = (event: MessageEvent<OpfsRpcResponse>) => {
		if (
			event.data?.kind === "response" &&
			event.data.requestId === requestId
		) {
			responses.push(event.data);
		}
	};

	const client = await OpfsStorageClient.open(storageName);
	try {
		channel.postMessage({
			kind: "request",
			protocolVersion: 1,
			requestId,
			clientId,
			storageName,
			operation: "open",
			payload: undefined,
		});
		await expect
			.poll(() => responses.length, { timeout: 2_000 })
			.toBe(1);
		expect(responses[0]).toMatchObject({
			ok: false,
			error: { code: "LIX_STORAGE_UNSUPPORTED" },
		});

		await expect(client.acquireSession()).resolves.toMatch(/^(0|[1-9]\d*)$/);
	} finally {
		await client.close();
		channel.close();
		for (const worker of workers) worker.terminate();
	}
});

async function openProvider(
	registration: LixStorageProviderRegistration,
): Promise<LixStorageProvider> {
	const module = (await import(/* @vite-ignore */ registration.moduleUrl)) as {
		createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
	};
	return module.createLixStorageProvider(registration.options);
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
	return Promise.race([
		promise,
		new Promise<T>((_, reject) =>
			setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs),
		),
	]);
}

async function staysPending(promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
	return Promise.race([
		promise.then(() => false, () => false),
		new Promise<true>((resolve) => setTimeout(() => resolve(true), timeoutMs)),
	]);
}
