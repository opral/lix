import { localFilesystemAlreadyOpen } from "./errors.js";
import type { LixBinding } from "./binding-types.js";
import {
	ACTIVE_ACCOUNT_CLIENT_STATE_KEY,
	ACTIVE_BRANCH_CLIENT_STATE_KEY,
	openClientState,
	openStoredClientState,
} from "./client-state.js";
import { Lix } from "./lix.js";
import type { LixSnapshotStorage, OpenLixOptions } from "./types.js";

export { Lix, LixTransaction, ObserveEvents } from "./lix.js";

const openLocalFilesystems = new WeakSet<LocalFilesystem>();

export class LocalFilesystem {
	readonly path: string;

	constructor(path: string) {
		if (typeof path !== "string" || path.length === 0) {
			throw new TypeError("LocalFilesystem requires a non-empty path");
		}
		this.path = path;
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
		const clientState = await openStoredClientState({
			storage: options.storage,
			namespace: remoteClientStateNamespace(options.server.url),
		});

		const restoredBranchId = clientState.get<string>(
			ACTIVE_BRANCH_CLIENT_STATE_KEY,
		);
		const restoredAccountId = clientState.get<string>(
			ACTIVE_ACCOUNT_CLIENT_STATE_KEY,
		);
		let remoteBinding: LixBinding | undefined;
		try {
			try {
				remoteBinding = await openRemoteLixBinding(options.server, {
					initialActiveBranchId: restoredBranchId,
					initialActiveAccountId: restoredAccountId,
				});
			} catch (error) {
				if (!restoredBranchId || !isBranchNotFoundError(error)) throw error;
				remoteBinding = await openRemoteLixBinding(options.server, {
					initialActiveAccountId: restoredAccountId,
				});
			}
			const activeBranchId = await remoteBinding.activeBranchId();
			const activeAccountId = await remoteBinding.activeAccountId();
			if (activeBranchId !== restoredBranchId) {
				await clientState.set(ACTIVE_BRANCH_CLIENT_STATE_KEY, activeBranchId);
			}
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
		openLocalFilesystems.add(storage);
		try {
			const binding = await openLixWorkerBinding(
				{
					kind: "localFilesystem",
					path: storage.path,
				},
				() => openLocalFilesystems.delete(storage),
				options.telemetry,
			);
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
		"openLix() requires storage to be LocalFilesystem or a Lix snapshot storage adapter",
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
