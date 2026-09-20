import type { WorkerOperation } from "./protocol.js";

// SQL text is not a safe read/write classifier (CTEs and functions can write).
export function mayHaveCommitted(operation: WorkerOperation): boolean {
	return new Set([
		"execute",
		"executeBatch",
		"transaction.commit",
		"createBranch",
		"switchBranch",
		"mergeBranch",
		"importFilesystemPaths",
		"syncDiskToLix",
		"recoverReplica",
		"recoverReplicaWithServer",
		"replica.convert",
		"hosted.create",
		"hosted.delete",
		"hosted.createFrom",
	]).has(operation.kind);
}
export function operationDeadline(
	operation: WorkerOperation,
): number | undefined {
	if (operation.kind === "observe.next") return undefined;
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
