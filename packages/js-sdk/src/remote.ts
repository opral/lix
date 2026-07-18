import { Lix } from "./lix.js";
import { openRemoteLixBinding } from "./remote/client.js";
import type { RemoteLixServerOptions } from "./types.js";

export { Lix, LixTransaction, ObserveEvents } from "./lix.js";
export { Row } from "./result.js";
export { Value } from "./value.js";
export type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	ExecuteResult,
	JsonValue,
	LixValue,
	MergeBranchOptions,
	MergeBranchOutcome,
	MergeBranchPreview,
	MergeBranchReceipt,
	MergeChangeStats,
	MergeConflict,
	MergeConflictSide,
	ObserveEvent,
	RemoteLixFetch,
	RemoteLixServerOptions,
	SqlParam,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "./types.js";

export type RemoteOpenLixOptions = {
	storage?: never;
	server: RemoteLixServerOptions;
};

/**
 * Opens a thin remote Lix client without referencing the local worker or WASM
 * module graph. Use this entrypoint in deployment targets that only use remote
 * mode and enforce per-asset size limits.
 */
export async function openLix(options: RemoteOpenLixOptions): Promise<Lix> {
	if (!options || typeof options !== "object") {
		throw new TypeError("openLix() options must be an object");
	}
	if ("backend" in options) {
		throw new TypeError(
			"openLix() option 'backend' was removed; use 'storage' instead",
		);
	}
	if (options.server === undefined) {
		throw new TypeError(
			"@lix-js/sdk/remote openLix() requires a remote server",
		);
	}
	if (options.storage !== undefined) {
		throw new TypeError(
			"openLix() remote mode cannot be combined with client storage",
		);
	}
	return new Lix(await openRemoteLixBinding(options.server));
}
