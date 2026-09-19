// The legacy storage-RPC transport remains a conformance-test harness only.
// Public OpfsStorage uses the elected engine's direct backend instead.
import type { LixStorage, LixStorageProviderRegistration } from "@lix-js/sdk";
import { OPFS_RPC_CHANNEL } from "./legacy-rpc/rpc.js";
import { physicalName } from "../js/physical-name.js";
let worker: Worker | undefined;
export class OpfsStorage implements LixStorage {
	constructor(readonly options: { name: string }) {}
	get name() {
		return this.options.name;
	}
	get lixStorage(): LixStorageProviderRegistration {
		worker ??= new Worker(new URL("./.generated/owner.js", import.meta.url), {
			type: "module",
		});
		return {
			version: 3,
			moduleUrl: new URL("./.generated/provider.js", import.meta.url).href,
			options: {
				name: this.name,
				mode: "shared",
				channelName: OPFS_RPC_CHANNEL,
				sharedEngineKey: `lix:opfs:${physicalName(this.name)}`,
			},
		};
	}
}
