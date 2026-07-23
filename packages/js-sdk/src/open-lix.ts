import {
	localFilesystemAlreadyOpen,
	localFilesystemNotOpen,
} from "./errors.js";
import type { LixBinding } from "./binding-types.js";
import {
	ACTIVE_BRANCH_CLIENT_STATE_KEY,
	openClientState,
} from "./client-state.js";
import { Lix } from "./lix.js";
import type {
	LixSnapshotStorage,
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
		assertSnapshotStorage(options.storage);
		const { openPersistentLixWorkerBinding } =
			await import("./worker/client.js");
		const clientBinding = await openPersistentLixWorkerBinding({
			storage: options.storage,
			namespace: remoteClientStateNamespace(options.server.url),
		});
		let clientState;
		try {
			clientState = await openClientState({
				binding: clientBinding,
				closeBinding: true,
			});
		} catch (error) {
			await clientBinding.close().catch(() => undefined);
			throw error;
		}

		const restoredBranchId = clientState.get<string>(
			ACTIVE_BRANCH_CLIENT_STATE_KEY,
		);
		let remoteBinding: LixBinding | undefined;
		try {
			try {
				remoteBinding = await openRemoteLixBinding(options.server, {
					initialActiveBranchId: restoredBranchId,
				});
			} catch (error) {
				if (!restoredBranchId || !isBranchNotFoundError(error)) throw error;
				remoteBinding = await openRemoteLixBinding(options.server);
			}
			const activeBranchId = await remoteBinding.activeBranchId();
			if (activeBranchId !== restoredBranchId) {
				await clientState.set(ACTIVE_BRANCH_CLIENT_STATE_KEY, activeBranchId);
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
	if (options.storage instanceof SQLite) {
		return new Lix(
			await openLixWorkerBinding(
				{
					kind: "sqlite",
					path: options.storage.path,
				},
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
	if (isSnapshotStorage(options.storage)) {
		const { openPersistentLixWorkerBinding } =
			await import("./worker/client.js");
		const binding = await openPersistentLixWorkerBinding({
			storage: options.storage,
			namespace: "local",
			telemetry: options.telemetry,
		});
		try {
			const clientState = await openClientState({ binding });
			return new Lix(binding, clientState);
		} catch (error) {
			await binding.close().catch(() => undefined);
			throw error;
		}
	}
	throw new TypeError(
		"openLix() requires storage to be SQLite, LocalFilesystem, or a Lix snapshot storage adapter",
	);
}

function isSnapshotStorage(value: unknown): value is LixSnapshotStorage {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof (value as Partial<LixSnapshotStorage>).load === "function" &&
		typeof (value as Partial<LixSnapshotStorage>).save === "function"
	);
}

function assertSnapshotStorage(
	value: unknown,
): asserts value is LixSnapshotStorage {
	if (!isSnapshotStorage(value)) {
		throw new TypeError(
			"openLix() remote storage must implement load() and save()",
		);
	}
}

function remoteClientStateNamespace(value: string | URL): string {
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		throw new TypeError("openLix() remote server url must be an absolute URL");
	}
	url.pathname = url.pathname.replace(/\/$/, "");
	url.search = "";
	url.hash = "";
	return `remote:${url.href}`;
}

function isBranchNotFoundError(
	error: unknown,
): error is Error & { code: "LIX_BRANCH_NOT_FOUND" } {
	return (
		error instanceof Error &&
		"code" in error &&
		error.code === "LIX_BRANCH_NOT_FOUND"
	);
}
