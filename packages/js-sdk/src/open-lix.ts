import type { LixBinding } from "./binding-types.js";
import {
	ACTIVE_ACCOUNT_CLIENT_STATE_KEY,
	openClientState,
} from "./client-state.js";
import { Lix } from "./lix.js";
import { isLixStorage, type LixStorage } from "./storage-adapter.js";
import type { IndexedDbStorageOptions, OpenLixOptions } from "./types.js";

export { Lix, LixTransaction, ObserveEvents } from "./lix.js";

const openStorages = new WeakSet<LixStorage>();
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
			remoteBinding = await openRemoteLixBinding(options.server);
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
	if (isLixStorage(options.storage)) {
		const storage = options.storage;
		if (openStorages.has(storage)) {
			throw storageAlreadyOpen();
		}
		openStorages.add(storage);
		let binding: LixBinding | undefined;
		const disconnect = () => {
			storage.lixStorage.connect(undefined);
			openStorages.delete(storage);
		};
		try {
			binding = await openLixWorkerBinding(
				storage.lixStorage.config,
				disconnect,
				options.telemetry,
			);
			storage.lixStorage.connect({
				importFilesystemPaths: (paths) =>
					binding!.importFilesystemPaths(paths),
				syncDiskToLix: () => binding!.syncDiskToLix(),
			});
			return new Lix(binding);
		} catch (error) {
			disconnect();
			await binding?.close().catch(() => undefined);
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
		"openLix() requires a Lix storage adapter or IndexedDbStorage",
	);
}

function storageAlreadyOpen(): Error & { code: string } {
	const error = new Error(
		"openLix() storage is already open; close the existing Lix or create a new storage adapter",
	) as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_STORAGE_IN_USE";
	return error;
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
