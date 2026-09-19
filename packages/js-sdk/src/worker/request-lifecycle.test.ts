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
