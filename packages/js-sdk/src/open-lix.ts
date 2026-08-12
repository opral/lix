import {
	localFilesystemAlreadyOpen,
	localFilesystemNotOpen,
} from "./errors.js";
import type { LixBinding } from "./binding-types.js";
import {
	ACTIVE_ACCOUNT_CLIENT_STATE_KEY,
	openClientState,
} from "./client-state.js";
import { Lix } from "./lix.js";
import type {
	IndexedDbStorageOptions,
	LocalFilesystemOptions,
	OpenLixOptions,
} from "./types.js";

export { Lix, LixTransaction, ObserveEvents } from "./lix.js";

const openLocalFilesystems = new WeakMap<LocalFilesystem, LixBinding | null>();
const openIndexedDbStorageNames = new Set<string>();

export class IndexedDbStorage {
	readonly name: string;

	constructor(options: IndexedDbStorageOptions) {
		if (!options || typeof options.name !== "string" || options.name.length === 0) {
			throw new TypeError("IndexedDbStorage requires a non-empty name");
		}
		this.name = options.name;
	}
}

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
					"importPaths() paths must not end with a trailing slash",
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
	if (
		options.telemetry !== undefined &&
		(typeof options.telemetry !== "object" ||
			typeof options.telemetry.onSpan !== "function")
	) {
		throw new TypeError("openLix() telemetry requires an onSpan callback");
	}
	if (options.server !== undefined) {
		const { openRemoteLixBinding } = await import("./remote/client.js");
		if (options.storage === undefined) {
			return new Lix(await openRemoteLixBinding(options.server));
		}
		assertIndexedDbStorage(options.storage);
		const storage = options.storage;
		const databaseName = remoteIndexedDbName(storage.name, options.server.url);
		const { openLixWorkerBinding } = await import("./worker/client.js");
		if (openIndexedDbStorageNames.has(databaseName)) {
			throw new Error("IndexedDbStorage is already open");
		}
		openIndexedDbStorageNames.add(databaseName);
		let clientBinding: LixBinding | undefined;
		let clientState: ReturnType<typeof openClientState> | undefined;
		try {
			clientBinding = await openLixWorkerBinding(
				{ kind: "indexedDb", name: databaseName },
				() => openIndexedDbStorageNames.delete(databaseName),
			);
			clientState = openClientState({
				binding: clientBinding,
				closeBinding: true,
			});
		} catch (error) {
			openIndexedDbStorageNames.delete(databaseName);
			await clientBinding?.close().catch(() => undefined);
			throw error;
		}

		let remoteBinding: LixBinding | undefined;
		try {
			const restoredAccountId = await clientState.get<string>(
				ACTIVE_ACCOUNT_CLIENT_STATE_KEY,
			);
			remoteBinding = await openRemoteLixBinding(options.server, {
				initialActiveAccountId: restoredAccountId,
			});
			const activeAccountId = await remoteBinding.activeAccountId();
			if (activeAccountId !== restoredAccountId) {
				await clientState.set(ACTIVE_ACCOUNT_CLIENT_STATE_KEY, activeAccountId);
			}
			return new Lix(remoteBinding, clientState);
		} catch (error) {
			await remoteBinding?.close().catch(() => undefined);
			await clientState.close().catch(() => undefined);
			throw error;
		}
	}
	const { openLixWorkerBinding } = await import("./worker/client.js");
	if (options.storage === undefined) {
		return new Lix(
			await openLixWorkerBinding(
				{ kind: "memory" },
				undefined,
				options.telemetry,
			),
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
				options.telemetry,
			);
			openLocalFilesystems.set(storage, binding);
			return new Lix(binding);
		} catch (error) {
			openLocalFilesystems.delete(storage);
			throw error;
		}
	}
	if (options.storage instanceof IndexedDbStorage) {
		const storage = options.storage;
		const databaseName = storage.name;
		if (openIndexedDbStorageNames.has(databaseName)) {
			throw new Error("IndexedDbStorage is already open");
		}
		openIndexedDbStorageNames.add(databaseName);
		let binding: LixBinding | undefined;
		try {
			binding = await openLixWorkerBinding(
				{ kind: "indexedDb", name: databaseName },
				() => openIndexedDbStorageNames.delete(databaseName),
				options.telemetry,
			);
			const clientState = openClientState({ binding });
			return new Lix(binding, clientState);
		} catch (error) {
			openIndexedDbStorageNames.delete(databaseName);
			await binding?.close().catch(() => undefined);
			throw error;
		}
	}
	throw new TypeError(
		"openLix() requires storage to be LocalFilesystem or IndexedDbStorage",
	);
}

function assertIndexedDbStorage(value: unknown): asserts value is IndexedDbStorage {
	if (!(value instanceof IndexedDbStorage)) {
		throw new TypeError("openLix() remote storage must be IndexedDbStorage");
	}
}

function remoteIndexedDbName(storageName: string, value: string | URL): string {
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		throw new TypeError("openLix() remote server url must be an absolute URL");
	}
	url.pathname = url.pathname.replace(/\/$/, "");
	url.search = "";
	url.hash = "";
	return `${storageName}:remote:${url.href}`;
}
