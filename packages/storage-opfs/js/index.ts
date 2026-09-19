import type {
	LixStorage,
	LixStorageProviderRegistration,
} from "@lix-js/sdk";
import { physicalName } from "./physical-name.js";

export type OpfsStorageOptions = {
	/** Identifies one persistent Lix database within the current origin. */
	name: string;
};

/** Selects the SQLite Wasm + OPFS storage provider for `openLix()`. */
export class OpfsStorage implements LixStorage {
	readonly name: string;

	constructor(options: OpfsStorageOptions) {
		if (!options || typeof options.name !== "string" || options.name.length === 0) {
			throw new TypeError("OpfsStorage requires a non-empty name");
		}
		this.name = options.name;
	}

	/** The elected dedicated engine worker opens this provider directly. */
	get lixStorage(): LixStorageProviderRegistration {
		return {
			version: 3,
			moduleUrl: new URL("./direct.js", import.meta.url).href,
			options: {
				name: this.name,
				sharedEngineKey: `lix:opfs:${physicalName(this.name)}`,
			},
		};
	}
}
