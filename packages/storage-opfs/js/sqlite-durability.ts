import type {
	LixStorageError,
	LixStorageErrorCode,
} from "@lix-js/sdk";

type SqliteValue = string | number | bigint | null;

type SqliteModeDatabase = {
	selectValue(sql: string): unknown;
};

type SqliteCheckpointDatabase = {
	exec: unknown;
};

type SqliteCheckpointExecutor = (options: {
	sql: string;
	rowMode: "array";
	resultRows: SqliteValue[][];
}) => unknown;

/**
 * Establish the SQLite mode required for an explicit OPFS durability fence.
 *
 * SQLite Wasm requires exclusive locking before enabling WAL for OPFS-backed
 * databases. Both PRAGMAs can silently return a mode other than the requested
 * one, so treating execution alone as success would let a later WAL checkpoint
 * degrade into a no-op.
 */
export function configureSqliteOpfsDurability(
	database: SqliteModeDatabase,
): void {
	const lockingMode = normalizePragmaValue(
		database.selectValue("PRAGMA locking_mode = EXCLUSIVE"),
	);
	if (lockingMode !== "exclusive") {
		throw durabilityError(
			"SQLite OPFS storage could not activate exclusive locking",
			{ actualLockingMode: lockingMode },
		);
	}

	const journalMode = normalizePragmaValue(
		database.selectValue("PRAGMA journal_mode = WAL"),
	);
	if (journalMode !== "wal") {
		throw durabilityError("SQLite OPFS storage could not activate WAL mode", {
			actualJournalMode: journalMode,
		});
	}
}

/**
 * Make every SQLite commit currently visible on this connection durable.
 *
 * Under WAL + synchronous=NORMAL, a completed FULL checkpoint synchronizes
 * the WAL before copying it and synchronizes the database file afterwards.
 * The SAH-pool VFS lowers SQLite xSync to FileSystemSyncAccessHandle.flush().
 */
export function fenceSqliteOpfsDurability(
	database: SqliteCheckpointDatabase,
): void {
	const rows: SqliteValue[][] = [];
	try {
		(database.exec as SqliteCheckpointExecutor).call(database, {
			sql: "PRAGMA wal_checkpoint(FULL)",
			rowMode: "array",
			resultRows: rows,
		});
	} catch (error) {
		throw durabilityError("SQLite OPFS durability checkpoint failed", {
			cause: error instanceof Error ? error.message : String(error),
		});
	}

	const row = rows[0];
	const busy = asInteger(row?.[0]);
	const walFrames = asInteger(row?.[1]);
	const checkpointedFrames = asInteger(row?.[2]);
	if (
		rows.length !== 1 ||
		busy !== 0 ||
		walFrames === undefined ||
		walFrames < 0 ||
		checkpointedFrames !== walFrames
	) {
		throw durabilityError(
			"SQLite OPFS durability checkpoint did not complete",
			{ busy, walFrames, checkpointedFrames },
		);
	}
}

function normalizePragmaValue(value: unknown): string | undefined {
	return typeof value === "string" ? value.toLowerCase() : undefined;
}

function asInteger(value: SqliteValue | undefined): number | undefined {
	if (typeof value === "bigint") {
		const converted = Number(value);
		return Number.isSafeInteger(converted) ? converted : undefined;
	}
	return typeof value === "number" && Number.isSafeInteger(value)
		? value
		: undefined;
}

function durabilityError(message: string, details?: unknown): LixStorageError {
	const code: LixStorageErrorCode = "LIX_STORAGE_DURABILITY";
	const error = new Error(message) as LixStorageError;
	error.name = "LixStorageError";
	Object.assign(error, { code, details });
	return error;
}
