import type {
	LixStorage,
	LixStorageProviderRegistration,
} from "@lix-js/sdk";
import { OPFS_RPC_CHANNEL } from "./rpc.js";

export type OpfsStorageOptions = {
	/** Identifies one persistent Lix database within the current origin. */
	name: string;
};

const ownerWorkers = new Map<string, Worker>();

/** Selects the SQLite Wasm + OPFS storage provider for `openLix()`. */
export class OpfsStorage implements LixStorage {
	readonly name: string;

	constructor(options: OpfsStorageOptions) {
		if (!options || typeof options.name !== "string" || options.name.length === 0) {
			throw new TypeError("OpfsStorage requires a non-empty name");
		}
		this.name = options.name;
	}

	/**
	 * Starting the owner here is intentional: the SDK loads the provider in a
	 * separate Lix worker, where `Worker` cannot be constructed. The provider
	 * then talks to this package-owned dedicated worker through BroadcastChannel.
	 */
	get lixStorage(): LixStorageProviderRegistration {
		const shared = ensureOwnerWorker();
		return {
			version: 3,
			moduleUrl: shared
				? new URL("./provider.js", import.meta.url).href
				: new URL("./direct.js", import.meta.url).href,
			options: {
				name: this.name,
				mode: shared ? "shared" : "direct",
				channelName: shared ? OPFS_RPC_CHANNEL : undefined,
			},
		};
	}
}

function ensureOwnerWorker(): boolean {
	if (typeof Worker === "undefined" || typeof BroadcastChannel === "undefined") {
		return false;
	}
	const ownerUrl = new URL("./owner.js", import.meta.url).href;
	if (!ownerWorkers.has(ownerUrl)) {
		let worker: Worker;
		try {
			worker = new Worker(ownerUrl, {
				type: "module",
				name: "lix-opfs-owner",
			});
		} catch {
			return false;
		}
		worker.onerror = () => {
			ownerWorkers.delete(ownerUrl);
			worker.terminate();
		};
		ownerWorkers.set(ownerUrl, worker);
	}
	return true;
}
