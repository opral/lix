import type {
	LixStorage,
	LixStorageProviderRegistration,
} from "@lix-js/sdk";

export type OpfsStorageOptions = {
	/** Identifies one persistent Lix database within the current origin. */
	name: string;
};

/** Selects the SQLite Wasm + OPFS storage provider for `openLix()`. */
export class OpfsStorage implements LixStorage {
	readonly name: string;
	readonly lixStorage: LixStorageProviderRegistration;

	constructor(options: OpfsStorageOptions) {
		if (!options || typeof options.name !== "string" || options.name.length === 0) {
			throw new TypeError("OpfsStorage requires a non-empty name");
		}
		this.name = options.name;
		this.lixStorage = {
			version: 2,
			moduleUrl: new URL("./provider.js", import.meta.url).href,
			options: { name: this.name },
		};
	}
}
