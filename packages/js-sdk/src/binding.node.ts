import type {
	LixBinding,
	LixStorageConfig,
	TelemetryDispatch,
	TelemetryParentContext,
	SyncServerBindingOptions,
	OpenProgressDispatch,
} from "./binding-types.js";

type FilesystemRuntime = typeof import("./binding.browser.js");
async function filesystemRuntime(
	storage: LixStorageConfig,
): Promise<FilesystemRuntime> {
	if (storage.kind !== "filesystem" || !storage.runtimeModuleUrl)
		throw new Error(
			"FilesystemStorage must provide its native runtime module; update @lix-js/storage-filesystem to match the SDK",
		);
	return import(/* @vite-ignore */ storage.runtimeModuleUrl);
}
export async function openLixBinding(
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
	parent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
	progress?: OpenProgressDispatch,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixBinding> {
	if (storage.kind === "filesystem")
		return (await filesystemRuntime(storage)).openLixBinding(
			storage,
			telemetry,
			parent,
			server,
			progress,
			snapshot,
		);
	return (await import("./binding.node-wasm.js")).openNodeWasmBinding(
		storage,
		telemetry,
		parent,
		server,
		progress,
		snapshot,
	);
}
export async function createHostedBinding(
	...args: Parameters<FilesystemRuntime["createHostedBinding"]>
) {
	return (await import("./binding.browser.js")).createHostedBinding(...args);
}
export async function deleteHostedBinding(
	...args: Parameters<FilesystemRuntime["deleteHostedBinding"]>
) {
	return (await import("./binding.browser.js")).deleteHostedBinding(...args);
}
export async function convertReplicaBinding(
	storage: LixStorageConfig,
	...args: Parameters<FilesystemRuntime["convertReplicaBinding"]> extends [
		LixStorageConfig,
		...infer R,
	]
		? R
		: never
) {
	const runtime =
		storage.kind === "filesystem"
			? await filesystemRuntime(storage)
			: await import("./binding.browser.js");
	return runtime.convertReplicaBinding(storage, ...args);
}
export async function retryReplicaMigrationCleanupBinding(
	storage: LixStorageConfig,
	...args: Parameters<
		FilesystemRuntime["retryReplicaMigrationCleanupBinding"]
	> extends [LixStorageConfig, ...infer R]
		? R
		: never
) {
	const runtime =
		storage.kind === "filesystem"
			? await filesystemRuntime(storage)
			: await import("./binding.browser.js");
	return runtime.retryReplicaMigrationCleanupBinding(storage, ...args);
}
