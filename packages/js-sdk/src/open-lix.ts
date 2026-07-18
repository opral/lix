import {
	localFilesystemAlreadyOpen,
	localFilesystemNotOpen,
} from "./errors.js";
import type { LixBinding } from "./binding-types.js";
import { Lix } from "./lix.js";
import type {
	LocalFilesystemOptions,
	OpenLixOptions,
	SQLiteOptions,
} from "./types.js";

export { Lix, LixTransaction, ObserveEvents } from "./lix.js";

export class SQLite {
	readonly path: string;

	constructor(options: SQLiteOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("SQLite requires a non-empty path");
		}
		this.path = options.path;
	}
}

const openLocalFilesystems = new WeakMap<LocalFilesystem, LixBinding | null>();

export class LocalFilesystem {
	readonly path: string;
	readonly lixDir: string | undefined;
	readonly syncAllFiles: boolean;

	constructor(options: LocalFilesystemOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("LocalFilesystem requires a non-empty path");
		}
		if (
			options.lixDir !== undefined &&
			(typeof options.lixDir !== "string" || options.lixDir.length === 0)
		) {
			throw new TypeError("LocalFilesystem lixDir must be a non-empty string");
		}
		if (typeof options.syncAllFiles !== "boolean") {
			throw new TypeError("LocalFilesystem syncAllFiles must be a boolean");
		}
		this.path = options.path;
		this.lixDir = options.lixDir;
		this.syncAllFiles = options.syncAllFiles;
	}

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
					"importPaths() paths must contain file paths, not directory paths",
				);
			}
		}
		await this.client("importPaths").importFilesystemPaths([...paths]);
	}

	async syncDiskToLix(): Promise<void> {
		return this.client("syncDiskToLix").syncDiskToLix();
	}

	private client(operation: string): LixBinding {
		const client = openLocalFilesystems.get(this);
		if (!client) {
			throw localFilesystemNotOpen(operation);
		}
		return client;
	}
}

export async function openLix(options: OpenLixOptions = {}): Promise<Lix> {
	if (!options || typeof options !== "object") {
		throw new TypeError("openLix() options must be an object");
	}
	if ("backend" in options) {
		throw new TypeError(
			"openLix() option 'backend' was removed; use 'storage' instead",
		);
	}
	if (options.server !== undefined) {
		if (options.storage !== undefined) {
			throw new TypeError(
				"openLix() remote mode cannot be combined with client storage",
			);
		}
		const { openRemoteLixBinding } = await import("./remote/client.js");
		return new Lix(await openRemoteLixBinding(options.server));
	}
	const { openLixWorkerBinding } = await import("./worker/client.js");
	if (options.storage === undefined) {
		return new Lix(await openLixWorkerBinding({ kind: "memory" }));
	}
	if (options.storage instanceof SQLite) {
		return new Lix(
			await openLixWorkerBinding({
				kind: "sqlite",
				path: options.storage.path,
			}),
		);
	}
	if (options.storage instanceof LocalFilesystem) {
		const storage = options.storage;
		if (openLocalFilesystems.has(storage)) {
			throw localFilesystemAlreadyOpen();
		}
		openLocalFilesystems.set(storage, null);
		try {
			const binding = await openLixWorkerBinding(
				{
					kind: "localFilesystem",
					path: storage.path,
					lixDir: storage.lixDir,
					syncAllFiles: storage.syncAllFiles,
				},
				() => openLocalFilesystems.delete(storage),
			);
			openLocalFilesystems.set(storage, binding);
			return new Lix(binding);
		} catch (error) {
			openLocalFilesystems.delete(storage);
			throw error;
		}
	}
	throw new TypeError(
		"openLix() requires storage to be SQLite or LocalFilesystem",
	);
}
