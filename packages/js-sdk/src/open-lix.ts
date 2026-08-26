import type { LixBinding } from "./binding-types.js";
import { Lix } from "./lix.js";
import {
	isJsProviderLixStorage,
	isLixStorage,
	type LixStorage,
} from "./storage-adapter.js";
import type {
	LixOpenProgress,
	OpenLixOptions,
	SyncLixServerOptions,
} from "./types.js";

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
	if (
		options.telemetry?.parentContext !== undefined &&
		typeof options.telemetry.parentContext !== "function"
	) {
		throw new TypeError(
			"openLix() telemetry parentContext must be a context provider function",
		);
	}
	if (
		options.onProgress !== undefined &&
		typeof options.onProgress !== "function"
	) {
		throw new TypeError("openLix() onProgress must be a function");
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
	const syncServer =
		options.server?.mode === "sync"
			? {
					url: new URL(options.server.url).toString(),
					headers: options.server.headers,
					fetch: options.server.fetch,
				}
			: undefined;
	if (syncServer?.fetch !== undefined && typeof syncServer.fetch !== "function") {
		throw new TypeError("openLix() sync server fetch must be a function");
	}
	if (syncServer?.headers !== undefined && typeof syncServer.headers !== "function") {
		// Validate static headers before opening a worker/native runtime.
		new Headers(syncServer.headers);
	}
	if (options.server !== undefined && syncServer === undefined) {
		throw new TypeError("openLix() server mode must be 'remote' or 'sync'");
	}
	if (syncServer !== undefined && options.storage === undefined) {
		throw new TypeError(
			"openLix() sync mode requires a durability-capable storage adapter",
		);
	}
	const { openLixWorkerBinding } = await import("./worker/client.js");
	if (options.storage === undefined) {
		return new Lix(
			await openLixWorkerBinding(
				{ kind: "memory" },
				undefined,
				options.telemetry,
				syncServer,
				options.onProgress,
			),
		);
	}
	if (isJsProviderLixStorage(options.storage)) {
		return openJsProviderStorage(
			options.storage,
			options.telemetry,
			syncServer,
			options.onProgress,
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
				syncServer,
				options.onProgress,
			);
			const routed = routeStorageBinding(binding);
			storage.lixStorage.connect({
				importFilesystemPaths: (paths) =>
					routed.current().importFilesystemPaths(paths),
				syncDiskToLix: () => routed.current().syncDiskToLix(),
			});
			return new Lix(routed.binding);
		} catch (error) {
			disconnect();
			await binding?.close().catch(() => undefined);
			throw error;
		}
	}
	throw new TypeError("openLix() requires a Lix storage adapter");
}

function routeStorageBinding(root: LixBinding): {
	binding: LixBinding;
	current(): LixBinding;
} {
	const live = new Set<LixBinding>();
	const wrap = (binding: LixBinding): LixBinding => {
		live.add(binding);
		return new Proxy(binding, {
			get(target, property, receiver) {
				if (property === "openAnotherSession") {
					return async (
						options: Parameters<LixBinding["openAnotherSession"]>[0],
					) => wrap(await target.openAnotherSession(options));
				}
				if (property === "close") {
					return async () => {
						try {
							await target.close();
						} finally {
							live.delete(target);
						}
					};
				}
				const value = Reflect.get(target, property, receiver) as unknown;
				return typeof value === "function" ? value.bind(target) : value;
			},
		});
	};
	return {
		binding: wrap(root),
		current: () => {
			const bindings = [...live];
			return bindings[bindings.length - 1] ?? root;
		},
	};
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
	syncServer: Omit<SyncLixServerOptions, "mode"> | undefined,
	onProgress: ((progress: LixOpenProgress) => void) | undefined,
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
			syncServer,
			onProgress,
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
