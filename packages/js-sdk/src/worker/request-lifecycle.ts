import type { WorkerOperation } from "./protocol.js";

// Every new public operation must declare its lost-acknowledgement policy.
// SQL text is not a safe classifier (CTEs and functions can write). Detached
// cleanup also commits: a repeat can report zero after a lost completed count.
const commitPolicy = {
	"replica.cleanup": true,
	"replica.convert": true,
	"hosted.create": true,
	"hosted.delete": true,
	"hosted.createFrom": true,
	open: false,
	"openSnapshot.write": false,
	"openSnapshot.finish": false,
	openAnotherSession: false,
	execute: true,
	executeMany: true,
	query: false,
	"readSnapshot.open": false,
	"readSnapshot.close": false,
	beginTransaction: false,
	"transaction.execute": false,
	"transaction.executeMany": false,
	"transaction.query": false,
	"transaction.stream": false,
	"transaction.commit": true,
	"transaction.rollback": false,
	replicaRecoverySources: false,
	exportReplicaRecovery: false,
	recoverReplica: true,
	recoverReplicaWithServer: true,
	syncHealth: false,
	prepareOfflineEditing: false,
	activeBranchId: false,
	activeAccountId: false,
	createBranch: true,
	switchBranch: true,
	mergeBranchPreview: false,
	mergeBranch: true,
	importFilesystemPaths: true,
	syncDiskToLix: true,
	exportSnapshot: false,
	"exportSnapshot.next": false,
	"exportSnapshot.cancel": false,
	stream: false,
	"stream.next": false,
	"stream.close": false,
	observe: false,
	"observe.next": false,
	"observe.close": false,
	close: false,
} satisfies Record<WorkerOperation["kind"], boolean>;

export function mayHaveCommitted(operation: WorkerOperation): boolean {
	return commitPolicy[operation.kind];
}
export function operationDeadline(
	operation: WorkerOperation,
): number | undefined {
	if (operation.kind === "observe.next") return undefined;
	// One page may wait on a sort or aggregate over the whole input before its
	// first row exists. Streams and snapshot reads have no whole-operation
	// deadline and are closable (`stream.close`, `readSnapshot.close`), so a
	// slow page or a slow read of a pinned state must not be fatal.
	if (
		operation.kind === "stream" ||
		operation.kind === "stream.next" ||
		operation.kind === "stream.close" ||
		operation.kind === "transaction.stream" ||
		operation.kind === "readSnapshot.open" ||
		operation.kind === "readSnapshot.close" ||
		(operation.kind === "query" && operation.readSnapshotId !== undefined)
	)
		return undefined;
	if (operation.kind === "close") return 5_000;
	if (operation.kind === "open") return 30_000;
	return 60_000;
}
export function lostOperationError(
	operation: WorkerOperation,
	error: Error,
): Error {
	if (!mayHaveCommitted(operation)) return error;
	return Object.assign(
		new Error(
			"The repository operation may have committed before its acknowledgement was lost; do not retry it blindly",
			{ cause: error },
		),
		{ code: "LIX_WRITE_OUTCOME_UNKNOWN" },
	);
}
