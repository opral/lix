import type { LixStorageSpace } from "@lix-js/sdk";

// The direct entry is the browser-safe bundle of provider.ts (including Wasm).
// @ts-expect-error esbuild intentionally replaces the declaration-emitting output.
import { OpfsBackend as BundledOpfsBackend } from "../dist/direct.js";

const OpfsBackend = BundledOpfsBackend as typeof import("../js/provider.js").OpfsBackend;

type Request = {
	name: string;
	phase: "acquire" | "reopen";
	expectedToken?: string;
};

self.onmessage = async (event: MessageEvent<Request>) => {
	let backend: Awaited<ReturnType<typeof OpfsBackend.open>> | undefined;
	try {
		backend = await OpfsBackend.open(event.data.name);
		if (event.data.phase === "acquire") {
			const token = await backend.acquireSession();
			await backend.close();
			backend = undefined;
			postMessage({
				ok: true,
				result: { token },
			});
			return;
		}

		let tokenlessFenced = false;
		try {
			await backend.beginWrite({
				awaitDurable: false,
				preconditions: [],
				batchCapacityHintBytes: 2,
			});
		} catch (error) {
			tokenlessFenced =
				(error as { code?: string }).code === "LIX_STORAGE_FENCED";
		}
		const token = await backend.acquireSession();
		if (token !== event.data.expectedToken) {
			throw new Error("owner handoff changed the storage session token");
		}
		const space: LixStorageSpace = {
			id: 46,
			name: "session-handoff",
			valueSemantics: "mutable",
			valueIntegrity: "backendVerified",
		};
		const write = await backend.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: 2,
			sessionToken: token,
		});
		await write.putMany(space, [
			{ key: new Uint8Array([1]), value: new Uint8Array([2]) },
		]);
		await write.commit();
		await backend.close();
		backend = undefined;
		postMessage({
			ok: true,
			result: { token, tokenlessFenced, writeCommitted: true },
		});
	} catch (error) {
		postMessage({
			ok: false,
			message: error instanceof Error ? error.message : String(error),
		});
	} finally {
		await backend?.close();
	}
};
