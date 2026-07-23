const OPERATION_COMMITTED = "lixSnapshotOperationCommitted";

export type SnapshotPersistenceAfterCommitError = Error & {
	readonly code: "LIX_SNAPSHOT_PERSISTENCE_FAILED";
	readonly lixSnapshotOperationCommitted: true;
};

/** Marks that the engine operation committed before durable snapshot saving failed. */
export function snapshotPersistenceAfterCommitError(
	cause: unknown,
): SnapshotPersistenceAfterCommitError {
	const message =
		cause instanceof Error
			? cause.message
			: "Saving the committed Lix snapshot failed";
	const error = new Error(message, { cause }) as SnapshotPersistenceAfterCommitError;
	error.name = "LixSnapshotPersistenceError";
	Object.defineProperties(error, {
		code: {
			value: "LIX_SNAPSHOT_PERSISTENCE_FAILED",
			enumerable: true,
		},
		[OPERATION_COMMITTED]: {
			value: true,
			enumerable: true,
		},
	});
	return error;
}

export function isSnapshotPersistenceAfterCommitError(
	error: unknown,
): error is SnapshotPersistenceAfterCommitError {
	return (
		typeof error === "object" &&
		error !== null &&
		OPERATION_COMMITTED in error &&
		(error as Record<string, unknown>)[OPERATION_COMMITTED] === true
	);
}
