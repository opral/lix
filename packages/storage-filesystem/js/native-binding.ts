import {
	createComponentDispatch,
	type ComponentDispatch,
} from "@lix-js/sdk/storage-runtime";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type {
	LixStorageConfig,
	LixBinding,
	ObserveEventsBinding,
	SyncServerBindingOptions,
	TelemetryDispatch,
	TelemetryParentContext,
	OpenProgressDispatch,
	SnapshotRestoreBinding,
} from "@lix-js/sdk/storage-runtime";
import { restoreSnapshot } from "@lix-js/sdk/storage-runtime";

type NativeAddon = {
	convertFilesystemReplicaToPartial(
		path: string,
		syncAllFiles: boolean,
		url: string,
		headers: [string, string][],
		branchId?: string,
	): Promise<void>;
	retryFilesystemReplicaMigrationCleanup(
		path: string,
		syncAllFiles: boolean,
		url: string,
		headers: [string, string][],
	): Promise<number>;
	createHosted(
		url: string,
		headers: [string, string][],
		idempotencyKey?: string,
	): Promise<import("@lix-js/sdk").HostedLix>;
	deleteHosted(url: string, headers: [string, string][]): Promise<void>;
	Lix: {
		openMemory(
			telemetry?: (request: Uint8Array) => void,
			telemetryParentJson?: string,
			serverUrl?: string,
			serverHeaders?: [string, string][],
			openProgress?: (progressJson: string) => void,
			componentDispatch?: ComponentDispatch,
			durability?: import("@lix-js/sdk").Durability,
		): Promise<NativeLixBinding>;
		openMemoryFromSnapshot(
			telemetry?: (request: Uint8Array) => void,
			telemetryParentJson?: string,
			openProgress?: (progressJson: string) => void,
			componentDispatch?: ComponentDispatch,
			durability?: import("@lix-js/sdk").Durability,
		): SnapshotRestoreBinding<NativeLixBinding>;
		openFilesystemStorage(
			path: string,
			syncAllFiles: boolean,
			telemetry?: (request: Uint8Array) => void,
			telemetryParentJson?: string,
			serverUrl?: string,
			serverHeaders?: [string, string][],
			openProgress?: (progressJson: string) => void,
			componentDispatch?: ComponentDispatch,
			durability?: import("@lix-js/sdk").Durability,
		): Promise<NativeLixBinding>;
		openFilesystemStorageFromSnapshot(
			path: string,
			syncAllFiles: boolean,
			telemetry?: (request: Uint8Array) => void,
			telemetryParentJson?: string,
			openProgress?: (progressJson: string) => void,
			componentDispatch?: ComponentDispatch,
			durability?: import("@lix-js/sdk").Durability,
		): SnapshotRestoreBinding<NativeLixBinding>;
	};
};

type NativeObserveEventsBinding = Omit<
	ObserveEventsBinding,
	"setTelemetryParent"
> & {
	setTelemetryParent(parentJson?: string): void;
};

type NativeLixBinding = Omit<
	LixBinding,
	"observe" | "setTelemetryParent" | "recoverReplicaWithServer"
> & {
	recoverReplicaWithServer(
		id: string,
		url: string,
		headers: [string, string][],
	): Promise<import("@lix-js/sdk").ReplicaRecoveryReceipt>;
	setTelemetryParent(parentJson?: string): void;
	observe(
		sql: Parameters<LixBinding["observe"]>[0],
		params: Parameters<LixBinding["observe"]>[1],
	): Promise<NativeObserveEventsBinding>;
};

function normalizeNativeObserveEvents(
	events: NativeObserveEventsBinding,
): ObserveEventsBinding {
	return new Proxy(events, {
		get(target, property, receiver) {
			if (property === "setTelemetryParent") {
				return (parent?: TelemetryParentContext) =>
					target.setTelemetryParent(
						parent === undefined ? undefined : JSON.stringify(parent),
					);
			}
			const value = Reflect.get(target, property, receiver) as unknown;
			return typeof value === "function" ? value.bind(target) : value;
		},
	}) as ObserveEventsBinding;
}

function normalizeNativeBinding(binding: NativeLixBinding): LixBinding {
	return new Proxy(binding, {
		get(target, property, receiver) {
			if (property === "setTelemetryParent") {
				return (parent?: TelemetryParentContext) =>
					target.setTelemetryParent(
						parent === undefined ? undefined : JSON.stringify(parent),
					);
			}
			if (property === "recoverReplicaWithServer") {
				return async (
					id: string,
					server: import("@lix-js/sdk/storage-runtime").SyncServerBindingOptions,
				) => {
					if (server.transport)
						throw new Error("Custom fetch is unsupported for native recovery");
					const headers = server.headerProvider
						? await server.headerProvider()
						: server.headers;
					return target.recoverReplicaWithServer(id, server.url, headers);
				};
			}
			if (property === "observe") {
				return async (
					sql: Parameters<LixBinding["observe"]>[0],
					params: Parameters<LixBinding["observe"]>[1],
				) => normalizeNativeObserveEvents(await target.observe(sql, params));
			}
			const value = Reflect.get(target, property, receiver) as unknown;
			return typeof value === "function" ? value.bind(target) : value;
		},
	}) as unknown as LixBinding;
}

const require = createRequire(import.meta.url);
const localNativePath = fileURLToPath(
	new URL("../lix_storage_filesystem.node", import.meta.url),
);

const nativePackages = {
	"linux-x64": "@lix-js/storage-filesystem-linux-x64",
	"linux-arm64": "@lix-js/storage-filesystem-linux-arm64",
	"darwin-arm64": "@lix-js/storage-filesystem-darwin-arm64",
	"win32-x64": "@lix-js/storage-filesystem-win32-x64",
} as const;

function resolveNativePath() {
	if (existsSync(localNativePath)) return localNativePath;
	const key =
		`${process.platform}-${process.arch}` as keyof typeof nativePackages;
	const packageName = nativePackages[key];
	let packageResolutionError: unknown;
	if (packageName) {
		try {
			return require.resolve(packageName);
		} catch (error) {
			packageResolutionError = error;
		}
	}
	if (!packageName) {
		throw new Error(`Unsupported platform ${process.platform}-${process.arch}`);
	}
	throw packageResolutionError;
}

let addon: NativeAddon | undefined;
let addonLoadError: NativeAddonUnavailableError | undefined;

class NativeAddonUnavailableError extends Error {
	constructor(message: string, options: ErrorOptions) {
		super(message, options);
		this.name = "NativeAddonUnavailableError";
	}
}

function loadAddon(): NativeAddon {
	if (addon) return addon;
	if (addonLoadError) throw addonLoadError;
	try {
		addon = require(resolveNativePath()) as NativeAddon;
		return addon;
	} catch (cause) {
		const error = new NativeAddonUnavailableError(
			`Failed to load @lix-js/storage-filesystem native addon for ${process.platform}-${process.arch}. ` +
				"This package requires the matching optional native binary package. " +
				"Run `npm run build` from packages/storage-filesystem for local development, or install a release that includes your platform binary.",
			{ cause },
		);
		addonLoadError = error;
		throw error;
	}
}

export async function openLixBinding(
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
	telemetryParent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
	openProgress?: OpenProgressDispatch,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixBinding> {
	if (server?.transport) {
		throw new TypeError(
			"Custom sync fetch is only supported by the browser worker",
		);
	}
	const nativeOpenProgress = openProgress
		? (progressJson: string) => {
				try {
					openProgress(JSON.parse(progressJson));
				} catch {
					// Open progress is observational and cannot fail repository opening.
				}
			}
		: undefined;
	const componentDispatch = createComponentDispatch();
	switch (storage.kind) {
		case "memory": {
			const nativeAddon = loadAddon();
			const nativeTelemetry = telemetry
				? (request: Uint8Array) => {
						if (request.byteLength > 0) telemetry(request);
					}
				: undefined;
			if (snapshot) {
				const restore = nativeAddon.Lix.openMemoryFromSnapshot(
					nativeTelemetry,
					telemetryParent ? JSON.stringify(telemetryParent) : undefined,
					nativeOpenProgress,
					componentDispatch,
					storage.durability,
				);
				return normalizeNativeBinding(await restoreSnapshot(snapshot, restore));
			}
			if (nativeTelemetry) {
				return normalizeNativeBinding(
					await nativeAddon.Lix.openMemory(
						nativeTelemetry,
						telemetryParent ? JSON.stringify(telemetryParent) : undefined,
						server?.url,
						server?.headers,
						nativeOpenProgress,
						componentDispatch,
						storage.durability,
					),
				);
			}
			return normalizeNativeBinding(
				await nativeAddon.Lix.openMemory(
					undefined,
					undefined,
					server?.url,
					server?.headers,
					nativeOpenProgress,
					componentDispatch,
					storage.durability,
				),
			);
		}
		case "jsStorage":
			throw new Error(
				"JavaScript storage providers are only available in browsers",
			);
		case "filesystem": {
			const nativeAddon = loadAddon();
			const nativeTelemetry = telemetry
				? (request: Uint8Array) => {
						if (request.byteLength > 0) telemetry(request);
					}
				: undefined;
			if (snapshot) {
				const restore = nativeAddon.Lix.openFilesystemStorageFromSnapshot(
					storage.path,
					storage.syncAllFiles,
					nativeTelemetry,
					telemetryParent ? JSON.stringify(telemetryParent) : undefined,
					nativeOpenProgress,
					componentDispatch,
					storage.durability,
				);
				return normalizeNativeBinding(await restoreSnapshot(snapshot, restore));
			}
			if (nativeTelemetry) {
				return normalizeNativeBinding(
					await nativeAddon.Lix.openFilesystemStorage(
						storage.path,
						storage.syncAllFiles,
						nativeTelemetry,
						telemetryParent ? JSON.stringify(telemetryParent) : undefined,
						server?.url,
						server?.headers,
						nativeOpenProgress,
						componentDispatch,
						storage.durability,
					),
				);
			}
			return normalizeNativeBinding(
				await nativeAddon.Lix.openFilesystemStorage(
					storage.path,
					storage.syncAllFiles,
					undefined,
					undefined,
					server?.url,
					server?.headers,
					nativeOpenProgress,
					componentDispatch,
					storage.durability,
				),
			);
		}
	}
}

export async function createHostedBinding(
	server: import("@lix-js/sdk/storage-runtime").HostedServerBindingOptions,
) {
	return loadAddon().createHosted(
		server.url,
		server.headers,
		server.idempotencyKey,
	);
}
export async function deleteHostedBinding(
	server: import("@lix-js/sdk/storage-runtime").HostedServerBindingOptions,
) {
	await loadAddon().deleteHosted(server.url, server.headers);
}

export async function convertReplicaBinding(
	storage: LixStorageConfig,
	server: SyncServerBindingOptions,
	branchId?: string,
): Promise<void> {
	if (storage.kind !== "filesystem")
		throw new TypeError("Node conversion requires FilesystemStorage");
	if (server.transport)
		throw new TypeError("Custom sync fetch is only supported in browsers");
	const headers = server.headerProvider
		? await server.headerProvider()
		: server.headers;
	await loadAddon().convertFilesystemReplicaToPartial(
		storage.path,
		storage.syncAllFiles,
		server.url,
		headers,
		branchId,
	);
}

export async function retryReplicaMigrationCleanupBinding(
	storage: LixStorageConfig,
	server: SyncServerBindingOptions,
): Promise<number> {
	if (storage.kind !== "filesystem")
		throw new TypeError("Node migration cleanup requires FilesystemStorage");
	if (server.transport)
		throw new TypeError("Custom sync fetch is only supported in browsers");
	const headers = server.headerProvider
		? await server.headerProvider()
		: server.headers;
	return loadAddon().retryFilesystemReplicaMigrationCleanup(
		storage.path,
		storage.syncAllFiles,
		server.url,
		headers,
	);
}
