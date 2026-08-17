type SqliteExecutor = {
	exec(sql: string): unknown;
};

export function restoreSynchronousModeBestEffort(
	database: SqliteExecutor,
	previousSynchronous: number,
): void {
	try {
		database.exec(
			`PRAGMA synchronous = ${previousSynchronous === 2 ? "FULL" : "NORMAL"}`,
		);
	} catch {
		// This runs after the transaction outcome is already fixed. A cleanup
		// failure must not replace either a successful COMMIT or the original
		// transaction error with an ambiguous ordinary failure. Leaving FULL in
		// place is safe; later storage operations surface any persistent I/O fault.
	}
}
