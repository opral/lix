import { expect, test, vi } from "vitest";
import type { LixBinding, ObserveEventsBinding } from "../binding-types.js";
import type {
	WorkerHostEndpoint,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";
import { startWorkerHost } from "./host.js";

function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	const promise = new Promise<T>((resolvePromise) => {
		resolve = resolvePromise;
	});
	return { promise, resolve };
}

test("observation setup bypasses a blocked finite operation", async () => {
	const firstExecute = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive: (message: WorkerInput) => void = () => undefined;
	const endpoint: WorkerHostEndpoint = {
		postMessage(message) {
			responses.push(message);
		},
		onMessage(listener) {
			receive = listener;
		},
	};
	let executeCalls = 0;
	let observeCalls = 0;
	const closedObservations: number[] = [];
	const observation = (ordinal: number): ObserveEventsBinding => ({
		setTelemetryParent() {},
		async next() {
			return {
				sequence: 0,
				mutationSequence: ordinal,
				result: { columns: [], rows: [], rowsAffected: 0, notices: [] },
			};
		},
		close() {
			closedObservations.push(ordinal);
		},
	});
	const binding = {
		setTelemetryParent() {},
		async execute() {
			executeCalls += 1;
			if (executeCalls === 1) await firstExecute.promise;
			return { columns: [], rows: [], rowsAffected: 0, notices: [] };
		},
		async observe() {
			observeCalls += 1;
			return observation(observeCalls);
		},
	} as unknown as LixBinding;
	startWorkerHost(endpoint, async () => binding);

	receive({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 1, ok: true }));

	receive({
		id: 2,
		sessionId: 0,
		operation: { kind: "execute", sql: "SELECT 'held'", params: [] },
	});
	await vi.waitFor(() => expect(executeCalls).toBe(1));
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 'history'", params: [] },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ id: 3, ok: true, value: 1 }),
	);
	receive({
		id: 4,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 'empty-parent'", params: [] },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ id: 4, ok: true, value: 2 }),
	);
	receive({ kind: "observe.close", observeId: 2 });
	expect(closedObservations).toEqual([2]);
	receive({
		id: 5,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 'real-parent'", params: [] },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ id: 5, ok: true, value: 3 }),
	);
	for (const [id, observeId] of [
		[6, 1],
		[7, 3],
	] as const) {
		receive({
			id,
			sessionId: 0,
			operation: { kind: "observe.next", observeId },
		});
	}
	await vi.waitFor(() => {
		expect(responses).toContainEqual(expect.objectContaining({ id: 6, ok: true }));
		expect(responses).toContainEqual(expect.objectContaining({ id: 7, ok: true }));
	});

	// Finite operations remain serialized with each other.
	receive({
		id: 8,
		sessionId: 0,
		operation: { kind: "execute", sql: "SELECT 'queued'", params: [] },
	});
	await Promise.resolve();
	expect(executeCalls).toBe(1);
	firstExecute.resolve();
	await vi.waitFor(() => expect(executeCalls).toBe(2));
});
