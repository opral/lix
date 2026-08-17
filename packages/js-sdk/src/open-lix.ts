import type { LixBinding } from "./binding-types.js";
import { Lix } from "./lix.js";
import {
	isJsProviderLixStorage,
	isLixStorage,
	type LixStorage,
} from "./storage-adapter.js";
import type { OpenLixOptions } from "./types.js";

export { Lix, LixTransaction, ObserveEvents } from "./lix.js";

const openStorages = new WeakSet<LixStorage>();

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
		if (options.server.mode === "remote") {
			const { openRemoteLixBinding } = await import("./remote/client.js");
			if ("storage" in options && options.storage !== undefined) {
				throw new TypeError("openLix() remote mode does not accept storage");
			}
			return new Lix(await openRemoteLixBinding(options.server));
		}
	}
	const syncServerUrl =
		options.server?.mode === "sync"
			? new URL(options.server.url).toString()
			: undefined;
	if (options.server !== undefined && syncServerUrl === undefined) {
		throw new TypeError("openLix() server mode must be 'remote' or 'sync'");
	}
	const { openLixWorkerBinding } = await import("./worker/client.js");
	if (options.storage === undefined) {
		return new Lix(
			await openLixWorkerBinding(
				{ kind: "memory" },
				undefined,
				options.telemetry,
				syncServerUrl,
			),
		);
	}
	if (isJsProviderLixStorage(options.storage)) {
		return openJsProviderStorage(
			options.storage,
			options.telemetry,
			syncServerUrl,
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
				syncServerUrl,
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
	throw new TypeError("openLix() requires a Lix storage adapter");
}

async function openJsProviderStorage(
	storage: LixStorage & {
		readonly lixStorage: {
			readonly version: 2;
			readonly moduleUrl: string;
			readonly options: unknown;
		};
	},
	telemetry: OpenLixOptions["telemetry"],
	syncServerUrl: string | undefined,
): Promise<Lix> {
	const { openLixWorkerBinding } = await import("./worker/client.js");
	if (openStorages.has(storage)) throw storageAlreadyOpen();
	openStorages.add(storage);
	let binding: LixBinding | undefined;
	try {
		const opened = await openLixWorkerBinding(
			{
				kind: "jsStorage",
				moduleUrl: storage.lixStorage.moduleUrl,
				options: storage.lixStorage.options,
			},
			() => openStorages.delete(storage),
			telemetry,
			syncServerUrl,
		);
		binding = opened;
		return new Lix(opened);
	} catch (error) {
		openStorages.delete(storage);
		await binding?.close().catch(() => undefined);
		throw error;
	}
}

function storageAlreadyOpen(): Error & { code: string } {
	const error = new Error(
		"openLix() storage is already open; close the existing Lix or create a new storage adapter",
	) as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_STORAGE_IN_USE";
	return error;
}
