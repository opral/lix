/**
 * Operations exposed to a storage adapter while its Lix is open.
 *
 * This is intentionally narrower than the engine binding. Storage packages
 * receive only the controls required to manage their own persistence layer.
 */
export type LixStorageConnection = {
	importFilesystemPaths(paths: string[]): Promise<void>;
	syncDiskToLix(): Promise<void>;
};

/** Engine configuration currently supported by external JavaScript storage packages. */
export type LixStorageAdapterConfig = {
	kind: "filesystem";
	path: string;
	syncAllFiles: boolean;
};

/**
 * Protocol implemented by JavaScript storage packages accepted by `openLix()`.
 *
 * Applications normally use a concrete package such as
 * `@lix-js/storage-filesystem` rather than implementing this directly.
 */
export type LixStorage = {
	readonly lixStorage: {
		readonly version: 1;
		readonly config: LixStorageAdapterConfig;
		connect(connection: LixStorageConnection | undefined): void;
	};
};

export function isLixStorage(value: unknown): value is LixStorage {
	if (!value || typeof value !== "object" || !("lixStorage" in value)) {
		return false;
	}
	const adapter = (value as { lixStorage?: unknown }).lixStorage;
	return Boolean(
		adapter &&
			typeof adapter === "object" &&
			(adapter as { version?: unknown }).version === 1 &&
			typeof (adapter as { connect?: unknown }).connect === "function" &&
			(adapter as { config?: { kind?: unknown } }).config?.kind ===
				"filesystem",
	);
}
