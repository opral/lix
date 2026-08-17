import { openLix, type LixStorageProvider, type LixStorageProviderRegistration } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { OpfsStorageClient } from "../js/client.js";
import type { OpfsRpcRequest, OpfsRpcResponse } from "../js/rpc.js";

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
