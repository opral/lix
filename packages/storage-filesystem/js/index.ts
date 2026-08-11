/** Options for opening filesystem-backed Lix storage. */
export type FilesystemStorageOptions = {
	/** Directory synchronized with the Lix repository. */
	path: string;
	/** Whether every file below `path` is synchronized. Defaults to `true`. */
	syncAllFiles?: boolean;
};

type FilesystemStorageConnection = {
	importFilesystemPaths(paths: string[]): Promise<void>;
	syncDiskToLix(): Promise<void>;
};

/**
 * Filesystem-backed storage for Lix.
 *
 * Pass an instance to `openLix({ storage })`. The Rust
 * `lix-storage-filesystem` adapter performs the actual storage and sync work.
 */
export class FilesystemStorage {
	readonly path: string;
	readonly syncAllFiles: boolean;
	#connection: FilesystemStorageConnection | undefined;

	readonly lixStorage: {
		readonly version: 1;
		readonly config: {
			readonly kind: "filesystem";
			readonly path: string;
			readonly syncAllFiles: boolean;
		};
		connect(connection: FilesystemStorageConnection | undefined): void;
	};

	constructor(options: FilesystemStorageOptions) {
		if (!options || typeof options !== "object") {
			throw new TypeError("FilesystemStorage options must be an object");
		}
		if (typeof options.path !== "string" || options.path.length === 0) {
			throw new TypeError("FilesystemStorage requires a non-empty path");
		}
		if (
			options.syncAllFiles !== undefined &&
			typeof options.syncAllFiles !== "boolean"
		) {
			throw new TypeError("FilesystemStorage syncAllFiles must be a boolean");
		}

		this.path = options.path;
		this.syncAllFiles = options.syncAllFiles ?? true;
		this.lixStorage = {
			version: 1,
			config: {
				kind: "filesystem",
				path: this.path,
				syncAllFiles: this.syncAllFiles,
			},
			connect: (connection) => {
				this.#connection = connection;
			},
		};
	}

	/** Import selected filesystem paths into the open Lix repository. */
	async importPaths(paths: readonly string[]): Promise<void> {
		if (!Array.isArray(paths)) {
			throw new TypeError("importPaths() paths must be an array");
		}
		for (const path of paths) {
			if (typeof path !== "string" || path.length === 0) {
				throw new TypeError(
					"importPaths() paths must contain non-empty strings",
				);
			}
			if (path.endsWith("/")) {
				throw new TypeError(
					"importPaths() paths must not end with a trailing slash",
				);
			}
		}
		await this.#requireConnection("importPaths").importFilesystemPaths([...paths]);
	}

	/** Immediately synchronize current disk changes into Lix. */
	async syncDiskToLix(): Promise<void> {
		await this.#requireConnection("syncDiskToLix").syncDiskToLix();
	}

	#requireConnection(operation: string): FilesystemStorageConnection {
		if (this.#connection) return this.#connection;
		const error = new Error(
			`FilesystemStorage.${operation}() requires an open Lix using this storage`,
		) as Error & { code: string };
		error.name = "LixError";
		error.code = "LIX_FILESYSTEM_STORAGE_NOT_OPEN";
		throw error;
	}
}
