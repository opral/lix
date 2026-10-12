import { expect, test } from "vitest";
import { lostOperationError, operationDeadline } from "./request-lifecycle.js";
test("SQL and commit with lost acknowledgement are never presented as safely retryable", () => {
	for (const operation of [
		{ kind: "execute", sql: "SELECT side_effect()", params: [] },
		{ kind: "transaction.commit", transactionId: 4 },
	] as const) {
		expect(
			lostOperationError(operation as any, new Error("owner died")),
		).toMatchObject({
			code: "LIX_WRITE_OUTCOME_UNKNOWN",
		});
	}
	const error = new Error("owner died");
	expect(
		lostOperationError({ kind: "observe.next", observeId: 1 }, error),
	).toBe(error);
});
test("opening and closing have finite budgets; observation waits remain long-lived", () => {
	expect(
		operationDeadline({
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		}),
	).toBe(30000);
	expect(operationDeadline({ kind: "close" })).toBe(5000);
	expect(
		operationDeadline({ kind: "observe.next", observeId: 1 }),
	).toBeUndefined();
});
test("stream and snapshot reads have no fatal deadline and never report an unknown write outcome", () => {
	const reads = [
		{ kind: "stream", sql: "SELECT 1", params: [] },
		{ kind: "stream.next", streamId: 1 },
		{ kind: "stream.close", streamId: 1 },
		{ kind: "readSnapshot.open" },
		{ kind: "readSnapshot.close", readSnapshotId: 1 },
		{ kind: "query", sql: "SELECT 1", params: [], readSnapshotId: 1 },
		{ kind: "transaction.stream", transactionId: 1, sql: "SELECT 1", params: [] },
	] as const;
	const error = new Error("owner died");
	for (const operation of reads) {
		expect(operationDeadline(operation)).toBeUndefined();
		expect(lostOperationError(operation, error)).toBe(error);
	}
	// A buffered read of the latest state has no fatal deadline either.
	expect(operationDeadline({ kind: "query", sql: "SELECT 1", params: [] })).toBeUndefined();
	expect(
		operationDeadline({ kind: "transaction.query", transactionId: 1, sql: "SELECT 1", params: [] }),
	).toBeUndefined();
	expect(lostOperationError({ kind: "executeMany", sql: "INSERT", paramSets: [] }, error)).toMatchObject({
		code: "LIX_WRITE_OUTCOME_UNKNOWN",
	});
});
