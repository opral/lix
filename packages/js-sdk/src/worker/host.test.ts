import { expect, test, vi } from "vitest";
import type { LixBinding, ObserveEventsBinding } from "../binding-types.js";
import type {
	WorkerHostEndpoint,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";
import { startWorkerHost } from "./host.js";
import { LixWorkerClient } from "./client.js";
import { WorkerOperationScheduler } from "./operation-scheduler.js";

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
	receive({
		id: 9,
		sessionId: 0,
		operation: { kind: "observe.close", observeId: 2 },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 9, ok: true }));
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

test("independent sessions progress while each session keeps FIFO order", async () => {
	const held = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const calls: string[] = [];
	const result = { columns: [], rows: [], rowsAffected: 0, notices: [] };
	const child = {
		setTelemetryParent() {},
		close: async () => {},
		async execute(sql: string) {
			calls.push(`child:${sql}`);
			return result;
		},
	} as unknown as LixBinding;
	const root = {
		setTelemetryParent() {},
		async openAnotherSession() {
			return child;
		},
		async execute(sql: string) {
			calls.push(`root:${sql}`);
			if (sql === "held") await held.promise;
			return result;
		},
	} as unknown as LixBinding;
	startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => root,
	);
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
		operation: { kind: "openAnotherSession", options: {} },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "execute", sql: "held", params: [] },
	});
	await vi.waitFor(() => expect(calls).toEqual(["root:held"]));
	receive({
		id: 4,
		sessionId: 0,
		operation: { kind: "execute", sql: "same-session-next", params: [] },
	});
	receive({
		id: 5,
		sessionId: 1,
		operation: { kind: "execute", sql: "resident-session", params: [] },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 5, ok: true, value: result }));
	expect(calls).toEqual(["root:held", "child:resident-session"]);
	expect(responses.some((message) => "ok" in message && message.id === 4)).toBe(false);
	held.resolve();
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 4, ok: true, value: result }));
	expect(calls).toEqual([
		"root:held",
		"child:resident-session",
		"root:same-session-next",
	]);
});

test("transaction IDs are bound to their owning session", async () => {
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const executeTransaction = vi.fn(async () => ({
		columns: [],
		rows: [],
		rowsAffected: 0,
		notices: [],
	}));
	const rollback = vi.fn(async () => {});
	const child = { setTelemetryParent() {}, close: async () => {} } as unknown as LixBinding;
	const root = {
		setTelemetryParent() {},
		close: async () => {},
		openAnotherSession: async () => child,
		beginTransaction: async () => ({ execute: executeTransaction, rollback }),
	} as unknown as LixBinding;
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => root,
	);
	receive({ id: 1, sessionId: 0, operation: { kind: "open", storage: { kind: "memory" }, telemetryEnabled: false, progressEnabled: false } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 1, ok: true }));
	receive({ id: 2, sessionId: 0, operation: { kind: "openAnotherSession", options: {} } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({ id: 3, sessionId: 0, operation: { kind: "beginTransaction" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true, value: 1 }));
	receive({
		id: 4,
		sessionId: 1,
		operation: { kind: "transaction.execute", transactionId: 1, sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({ id: 4, ok: false, error: expect.objectContaining({ code: "LIX_TRANSACTION_OWNER_MISMATCH" }) }),
		),
	);
	expect(executeTransaction).not.toHaveBeenCalled();
	receive({ kind: "transaction.abandon", transactionId: 1 });
	await vi.waitFor(() => expect(rollback).toHaveBeenCalledOnce());
	await host.close();
});

test("a duplicate terminal request cannot consume another transaction reservation", async () => {
	const held = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const commitA = vi.fn(async () => undefined);
	const commitB = vi.fn(async () => undefined);
	let transactionIndex = 0;
	const transactions = [
		{ commit: commitA, rollback: async () => {}, execute: async () => undefined },
		{ commit: commitB, rollback: async () => {}, execute: async () => undefined },
	];
	const binding = {
		setTelemetryParent() {},
		close: async () => {},
		async execute(sql: string) {
			if (sql === "hold") await held.promise;
			return { columns: [], rows: [], rowsAffected: 0, notices: [] };
		},
		async beginTransaction() {
			return transactions[transactionIndex++];
		},
	} as unknown as LixBinding;
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
		undefined,
		false,
		new WorkerOperationScheduler({
			maxActive: 1,
			queueWaitMs: 5_000,
		}),
	);
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
	receive({ id: 2, sessionId: 0, operation: { kind: "beginTransaction" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({ id: 3, sessionId: 0, operation: { kind: "beginTransaction" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true, value: 2 }));
	receive({
		id: 4,
		sessionId: 0,
		operation: { kind: "execute", sql: "hold", params: [] },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ kind: "request.started", id: 4 }),
	);
	receive({
		id: 5,
		sessionId: 0,
		operation: { kind: "transaction.commit", transactionId: 1 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ kind: "request.queued", id: 5 }),
	);
	receive({
		id: 6,
		sessionId: 0,
		operation: { kind: "transaction.commit", transactionId: 1 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 6,
				ok: false,
				error: expect.objectContaining({ code: "LIX_INVALID_TRANSACTION_STATE" }),
			}),
		),
	);
	receive({
		id: 7,
		sessionId: 0,
		operation: { kind: "transaction.commit", transactionId: 2 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ kind: "request.queued", id: 7 }),
	);
	held.resolve();
	await vi.waitFor(() => {
		expect(responses).toContainEqual({ id: 5, ok: true, value: undefined });
		expect(responses).toContainEqual({ id: 7, ok: true, value: undefined });
	});
	expect(commitA).toHaveBeenCalledOnce();
	expect(commitB).toHaveBeenCalledOnce();
	await host.close();
});

test("scheduler saturation returns a typed error and disconnect drains the active lane", async () => {
	const held = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const calls: string[] = [];
	const closed = vi.fn(async () => {});
	const binding = {
		setTelemetryParent() {},
		close: closed,
		async execute(sql: string) {
			calls.push(sql);
			if (sql === "active") await held.promise;
			return { columns: [], rows: [], rowsAffected: 0, notices: [] };
		},
	} as unknown as LixBinding;
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
		undefined,
		false,
		new WorkerOperationScheduler({
			maxActive: 1,
			maxQueued: 1,
			maxQueuedPerSession: 1,
			queueWaitMs: 5_000,
		}),
	);
	receive({ id: 1, sessionId: 0, operation: { kind: "open", storage: { kind: "memory" }, telemetryEnabled: false, progressEnabled: false } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 1, ok: true }));
	receive({ id: 2, sessionId: 0, operation: { kind: "execute", sql: "active", params: [] } });
	await vi.waitFor(() => expect(calls).toEqual(["active"]));
	receive({ id: 3, sessionId: 0, operation: { kind: "execute", sql: "queued", params: [] } });
	receive({ id: 4, sessionId: 0, operation: { kind: "execute", sql: "overflow", params: [] } });
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({ id: 4, ok: false, error: expect.objectContaining({ code: "LIX_WORKER_QUEUE_FULL" }) }),
		),
	);
	const shutdown = host.close();
	expect(closed).not.toHaveBeenCalled();
	held.resolve();
	await shutdown;
	expect(calls).toEqual(["active"]);
	expect(responses).toContainEqual(expect.objectContaining({ id: 3, ok: false }));
	expect(closed).toHaveBeenCalledOnce();
});

test("timed-out snapshot open aborts its queued restore writer", async () => {
	const held = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const openedSnapshots: Array<ReadableStream<Uint8Array> | undefined> = [];
	const binding = {
		setTelemetryParent() {},
		async close() {},
		async execute() {
			await held.promise;
			return { columns: [], rows: [], rowsAffected: 0, notices: [] };
		},
	} as unknown as LixBinding;
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async (_storage, _telemetry, _parent, _server, _progress, snapshot) => {
			openedSnapshots.push(snapshot);
			return binding;
		},
		undefined,
		false,
		new WorkerOperationScheduler({
			maxActive: 1,
			maxQueued: 1,
			maxQueuedPerSession: 1,
			queueWaitMs: 100,
		}),
	);
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
		operation: { kind: "execute", sql: "hold", params: [] },
	});
	receive({
		id: 3,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
			snapshotId: 44,
		},
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ kind: "request.queued", id: 3 }));
	receive({
		id: 4,
		sessionId: 0,
		operation: {
			kind: "openSnapshot.write",
			snapshotId: 44,
			chunk: new Uint8Array([1, 2, 3]),
		},
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ kind: "request.started", id: 4 }));
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 3,
				ok: false,
				error: expect.objectContaining({ code: "LIX_WORKER_QUEUE_TIMEOUT" }),
			}),
		),
	);
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({ id: 4, ok: false }),
		),
	);
	// The failed open retired this exact input instead of leaving a writer for a
	// snapshot that can no longer be consumed.
	receive({
		id: 5,
		sessionId: 0,
		operation: { kind: "openSnapshot.finish", snapshotId: 44 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({ id: 5, ok: false }),
		),
	);
	expect(openedSnapshots).toEqual([undefined]);
	held.resolve();
	await host.close();
});

test("observation close acknowledges only after the binding drains its active read", async () => {
	const closeBarrier = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: () => closeBarrier.promise,
	};
	const binding = {
		setTelemetryParent() {},
		observe: async () => events,
	} as unknown as LixBinding;
	startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
	);
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
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "observe.close", observeId: 1 },
	});
	await Promise.resolve();
	expect(responses).not.toContainEqual({ id: 3, ok: true });
	closeBarrier.resolve();
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true }));
});

test("a concurrent observation next rejects immediately instead of waiting behind the first", async () => {
	const readStarted = deferred<void>();
	const readBarrier = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	let nextCalls = 0;
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		async next() {
			nextCalls += 1;
			if (nextCalls === 2) {
				readStarted.resolve();
				await readBarrier.promise;
			}
			return undefined as never;
		},
		async close() {
			readBarrier.resolve();
		},
	};
	const binding = {
		setTelemetryParent() {},
		observe: async () => events,
	} as unknown as LixBinding;
	startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
	);
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
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "observe.next", observeId: 1 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ id: 3, ok: true, value: undefined }),
	);
	receive({
		id: 4,
		sessionId: 0,
		operation: { kind: "observe.next", observeId: 1 },
	});
	await readStarted.promise;
	receive({
		id: 5,
		sessionId: 0,
		operation: { kind: "observe.next", observeId: 1 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 5,
				ok: false,
				error: expect.objectContaining({ code: "LIX_OBSERVE_NEXT_IN_FLIGHT" }),
			}),
		),
	);
	expect(nextCalls).toBe(2); // Initial read and the one held read only.
	receive({
		id: 6,
		sessionId: 0,
		operation: { kind: "observe.close", observeId: 1 },
	});
	await vi.waitFor(() => {
		expect(responses).toContainEqual({ id: 4, ok: true, value: undefined });
		expect(responses).toContainEqual({ id: 6, ok: true });
	});
});

test("all admitted idle observers run without consuming snapshot-pull capacity", async () => {
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	let registrationIndex = 0;
	const startedReads: number[] = [];
	const events = Array.from({ length: 32 }, (_, index) => {
		const read = deferred<void>();
		return {
			setTelemetryParent() {},
			async next() {
				startedReads.push(index);
				await read.promise;
				return undefined;
			},
			close() {
				read.resolve();
			},
		};
	});
	const snapshot = {
		next: vi.fn(async () => new Uint8Array([42])),
		cancel: vi.fn(async () => {}),
	};
	const scheduler = new WorkerOperationScheduler({
		maxActive: 1,
		maxIndependentActive: 1,
		maxObserverActive: 32,
	});
	const binding = {
		setTelemetryParent() {},
		close: async () => {},
		async observe() {
			return events[registrationIndex++];
		},
		exportSnapshot: () => snapshot,
	} as unknown as LixBinding;
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
		undefined,
		false,
		scheduler,
	);
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
	for (let index = 0; index < events.length; index++) {
		receive({
			id: index + 2,
			sessionId: 0,
			operation: { kind: "observe", sql: "SELECT 1", params: [] },
		});
		await vi.waitFor(() =>
			expect(responses).toContainEqual({ id: index + 2, ok: true, value: index + 1 }),
		);
	}
	for (let index = 0; index < events.length; index++) {
		receive({
			id: index + 20,
			sessionId: 0,
			operation: { kind: "observe.next", observeId: index + 1 },
		});
	}
	await vi.waitFor(() => expect(startedReads).toHaveLength(32));
	receive({ id: 100, sessionId: 0, operation: { kind: "exportSnapshot" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 100, ok: true, value: 1 }));
	receive({
		id: 101,
		sessionId: 0,
		operation: { kind: "exportSnapshot.next", exportId: 1 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ id: 101, ok: true, value: new Uint8Array([42]) }),
	);
	expect(snapshot.next).toHaveBeenCalledOnce();

	const secondResponses: WorkerResponse[] = [];
	let receiveSecond!: (message: WorkerInput) => void;
	const secondEvents: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: async () => {},
	};
	const secondBinding = {
		setTelemetryParent() {},
		close: async () => {},
		observe: async () => secondEvents,
	} as unknown as LixBinding;
	const secondHost = startWorkerHost(
		{
			postMessage: (message) => secondResponses.push(message),
			onMessage: (listener) => (receiveSecond = listener),
		},
		async () => secondBinding,
		undefined,
		false,
		scheduler,
	);
	receiveSecond({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 1, ok: true }));
	receiveSecond({
		id: 2,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() =>
		expect(secondResponses).toContainEqual({ kind: "request.queued", id: 2 }),
	);

	// Closing one host session releases all 32 reservations only after each
	// active read is settled, then another host sharing the scheduler can admit.
	receive({ id: 102, sessionId: 0, operation: { kind: "close" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 102, ok: true }));
	receiveSecond({
		id: 3,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 2, ok: true, value: 1 }));
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 3, ok: true, value: 2 }));
	await host.close();
	await secondHost.close();
});

test("session close fences a pending observer registration and closes its late iterator", async () => {
	const registration = deferred<ObserveEventsBinding>();
	const registrationStarted = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	let registrationSignal: AbortSignal | undefined;
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: vi.fn(async () => undefined),
		close: vi.fn(async () => {}),
	};
	const binding = {
		setTelemetryParent() {},
		close: vi.fn(async () => {}),
		observe(_sql: string, _params: unknown[], options?: { signal?: AbortSignal }) {
			registrationSignal = options?.signal;
			registrationStarted.resolve();
			return registration.promise;
		},
	} as unknown as LixBinding;
	const scheduler = new WorkerOperationScheduler({ maxObserverRegistrations: 1 });
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
		undefined,
		false,
		scheduler,
	);
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
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await registrationStarted.promise;
	const secondResponses: WorkerResponse[] = [];
	let receiveSecond!: (message: WorkerInput) => void;
	const secondEvents: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: async () => {},
	};
	const secondBinding = {
		setTelemetryParent() {},
		close: async () => {},
		observe: async () => secondEvents,
	} as unknown as LixBinding;
	const secondHost = startWorkerHost(
		{
			postMessage: (message) => secondResponses.push(message),
			onMessage: (listener) => (receiveSecond = listener),
		},
		async () => secondBinding,
		undefined,
		false,
		scheduler,
	);
	receiveSecond({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 1, ok: true }));
	receiveSecond({
		id: 2,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() =>
		expect(secondResponses).toContainEqual({ kind: "request.queued", id: 2 }),
	);
	receive({ id: 3, sessionId: 0, operation: { kind: "close" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ kind: "request.started", id: 3 }));
	expect(registrationSignal?.aborted).toBe(true);
	expect(responses).not.toContainEqual(expect.objectContaining({ id: 3, ok: true }));
	expect(binding.close).not.toHaveBeenCalled();

	// Close drains the pending registration. Its late iterator is closed before
	// the Lix binding, and no observer ID is published for the closing session.
	registration.resolve(events);
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({ id: 2, ok: false }),
		),
	);
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true }));
	expect(events.close).toHaveBeenCalledOnce();
	expect(binding.close).toHaveBeenCalledOnce();
	receive({
		id: 4,
		sessionId: 0,
		operation: { kind: "observe.next", observeId: 1 },
	});
	await vi.waitFor(() =>
		expect(responses).toContainEqual({ id: 4, ok: true, value: undefined }),
	);
	expect(events.next).not.toHaveBeenCalled();
	await host.close();
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 2, ok: true, value: 1 }));
	await secondHost.close();
	const thirdResponses: WorkerResponse[] = [];
	let receiveThird!: (message: WorkerInput) => void;
	const thirdBinding = {
		setTelemetryParent() {},
		close: async () => {},
		observe: async () => ({
			setTelemetryParent() {},
			next: async () => undefined,
			close: async () => {},
		}),
	} as unknown as LixBinding;
	const thirdHost = startWorkerHost(
		{
			postMessage: (message) => thirdResponses.push(message),
			onMessage: (listener) => (receiveThird = listener),
		},
		async () => thirdBinding,
		undefined,
		false,
		scheduler,
	);
	receiveThird({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(thirdResponses).toContainEqual({ id: 1, ok: true }));
	receiveThird({
		id: 2,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() => expect(thirdResponses).toContainEqual({ id: 2, ok: true, value: 1 }));
	await thirdHost.close();
});

test("canceling a queued observer removes only that registration and frees no active slot", async () => {
	const firstRegistration = deferred<ObserveEventsBinding>();
	const firstStarted = deferred<void>();
	const firstResponses: WorkerResponse[] = [];
	let receiveFirst!: (message: WorkerInput) => void;
	const firstBinding = {
		setTelemetryParent() {},
		close: vi.fn(async () => {}),
		observe() {
			firstStarted.resolve();
			return firstRegistration.promise;
		},
	} as unknown as LixBinding;
	const scheduler = new WorkerOperationScheduler({ maxObserverRegistrations: 1 });
	const firstHost = startWorkerHost(
		{
			postMessage: (message) => firstResponses.push(message),
			onMessage: (listener) => (receiveFirst = listener),
		},
		async () => firstBinding,
		undefined,
		false,
		scheduler,
	);
	receiveFirst({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(firstResponses).toContainEqual({ id: 1, ok: true }));
	receiveFirst({
		id: 2,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await firstStarted.promise;

	const secondResponses: WorkerResponse[] = [];
	let receiveSecond!: (message: WorkerInput) => void;
	const secondObserve = vi.fn(async () => ({
		setTelemetryParent() {},
		next: async () => undefined,
		close: async () => {},
	}));
	const secondHost = startWorkerHost(
		{
			postMessage: (message) => secondResponses.push(message),
			onMessage: (listener) => (receiveSecond = listener),
		},
		async () => ({
			setTelemetryParent() {},
			close: async () => {},
			observe: secondObserve,
		} as unknown as LixBinding),
		undefined,
		false,
		scheduler,
	);
	receiveSecond({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 1, ok: true }));
	receiveSecond({
		id: 2,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 2", params: [] },
	});
	await vi.waitFor(() =>
		expect(secondResponses).toContainEqual({ kind: "request.queued", id: 2 }),
	);
	receiveSecond({ kind: "observe.cancel", requestId: 2 });
	await vi.waitFor(() =>
		expect(secondResponses).toContainEqual(
			expect.objectContaining({
				id: 2,
				ok: false,
				error: expect.objectContaining({ code: "LIX_OBSERVER_CANCELLED" }),
			}),
		),
	);
	expect(secondObserve).not.toHaveBeenCalled();

	const firstEvents: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: async () => {},
	};
	firstRegistration.resolve(firstEvents);
	await vi.waitFor(() => expect(firstResponses).toContainEqual({ id: 2, ok: true, value: 1 }));
	await firstHost.close();
	await secondHost.close();
});

test("canceling a started observer closes its late iterator before releasing admission", async () => {
	const registration = deferred<ObserveEventsBinding>();
	const started = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	let registrationSignal: AbortSignal | undefined;
	let registrationCalls = 0;
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: vi.fn(async () => {}),
	};
	const binding = {
		setTelemetryParent() {},
		close: async () => {},
		observe(_sql: string, _params: unknown[], options?: { signal?: AbortSignal }) {
			registrationCalls++;
			registrationSignal = options?.signal;
			started.resolve();
			return registrationCalls === 1
				? registration.promise
				: Promise.resolve({
						setTelemetryParent() {},
						next: async () => undefined,
						close: async () => {},
					});
		},
	} as unknown as LixBinding;
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
	);
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
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await started.promise;
	receive({ kind: "observe.cancel", requestId: 2 });
	expect(registrationSignal?.aborted).toBe(true);
	registration.resolve(events);
	await vi.waitFor(() => expect(events.close).toHaveBeenCalledOnce());
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 2,
				ok: false,
				error: expect.objectContaining({ code: "LIX_OBSERVER_CANCELLED" }),
			}),
		),
	);
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 2", params: [] },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true, value: 1 }));
	expect(registrationCalls).toBe(2);
	await host.close();
});

test("canceling an adopted observer drains a failing close without an unhandled rejection", async () => {
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: vi.fn(async () => {
			throw new Error("late observer close failed");
		}),
	};
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => ({
			setTelemetryParent() {},
			close: async () => {},
			observe: async () => events,
		} as unknown as LixBinding),
	);
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
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	const unhandled: unknown[] = [];
	const onUnhandled = (reason: unknown) => unhandled.push(reason);
	process.on("unhandledRejection", onUnhandled);
	try {
		receive({ kind: "observe.cancel", requestId: 2 });
		await vi.waitFor(() => expect(events.close).toHaveBeenCalledOnce());
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(unhandled).toEqual([]);
		await host.close();
	} finally {
		process.removeListener("unhandledRejection", onUnhandled);
	}
});

test("observer close preserves binding errors, drains its read, and releases admission", async () => {
	const read = deferred<void>();
	const readStarted = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const closeError = Object.assign(new Error("observer close failed"), {
		code: "TEST_OBSERVER_CLOSE_FAILED",
	});
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		async next() {
			readStarted.resolve();
			await read.promise;
			return undefined;
		},
		async close() {
			throw closeError;
		},
	};
	const scheduler = new WorkerOperationScheduler({ maxObserverRegistrations: 1 });
	const host = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => ({
			setTelemetryParent() {},
			close: async () => {},
			observe: async () => events,
		} as unknown as LixBinding),
		undefined,
		false,
		scheduler,
	);
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
	receive({ id: 2, sessionId: 0, operation: { kind: "observe", sql: "SELECT 1", params: [] } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({ id: 3, sessionId: 0, operation: { kind: "observe.next", observeId: 1 } });
	await readStarted.promise;
	receive({ id: 4, sessionId: 0, operation: { kind: "observe.close", observeId: 1 } });
	await vi.waitFor(() => expect(responses).toContainEqual({ kind: "request.started", id: 4 }));
	receive({ id: 5, sessionId: 0, operation: { kind: "observe.close", observeId: 1 } });
	await vi.waitFor(() => expect(responses).toContainEqual({ kind: "request.started", id: 5 }));
	expect(responses).not.toContainEqual(expect.objectContaining({ id: 4, ok: expect.any(Boolean) }));
	expect(responses).not.toContainEqual(expect.objectContaining({ id: 5, ok: expect.any(Boolean) }));
	const secondResponses: WorkerResponse[] = [];
	let receiveSecond!: (message: WorkerInput) => void;
	const secondHost = startWorkerHost(
		{
			postMessage: (message) => secondResponses.push(message),
			onMessage: (listener) => (receiveSecond = listener),
		},
		async () => ({
			setTelemetryParent() {},
			close: async () => {},
			observe: async () => ({
				setTelemetryParent() {},
				next: async () => undefined,
				close: async () => {},
			}),
		} as unknown as LixBinding),
		undefined,
		false,
		scheduler,
	);
	receiveSecond({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 1, ok: true }));
	receiveSecond({
		id: 2,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() =>
		expect(secondResponses).toContainEqual({ kind: "request.queued", id: 2 }),
	);
	read.resolve();
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 4,
				ok: false,
				error: expect.objectContaining({ code: "TEST_OBSERVER_CLOSE_FAILED" }),
			}),
		),
	);
	await vi.waitFor(() =>
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 5,
				ok: false,
				error: expect.objectContaining({ code: "TEST_OBSERVER_CLOSE_FAILED" }),
			}),
		),
	);
	await vi.waitFor(() => expect(secondResponses).toContainEqual({ id: 2, ok: true, value: 1 }));
	await host.close();
	await secondHost.close();
});

test("worker shutdown also waits for an observation close RPC already in flight", async () => {
	const closeBarrier = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const closeSession = vi.fn(async () => {});
	const events: ObserveEventsBinding = {
		setTelemetryParent() {},
		next: async () => undefined,
		close: () => closeBarrier.promise,
	};
	const binding = {
		setTelemetryParent() {},
		close: closeSession,
		observe: async () => events,
	} as unknown as LixBinding;
	const controller = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => (receive = listener),
		},
		async () => binding,
	);
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
		operation: { kind: "observe", sql: "SELECT 1", params: [] },
	});
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "observe.close", observeId: 1 },
	});
	const shutdown = controller.close();
	await Promise.resolve();
	expect(closeSession).not.toHaveBeenCalled();
	closeBarrier.resolve();
	await shutdown;
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true }));
	expect(closeSession).toHaveBeenCalledOnce();
});

test("disconnect drains active work but rejects queued writes and closes late observers", async () => {
	const active = deferred<void>();
	const observed = deferred<ObserveEventsBinding>();
	const observationCloseBarrier = deferred<void>();
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const writes: string[] = [];
	const closed = vi.fn(async () => {});
	const observationClose = vi.fn(() => observationCloseBarrier.promise);
	const binding = {
		setTelemetryParent() {},
		close: closed,
		async execute(sql: string) {
			writes.push(sql);
			await active.promise;
			return { columns: [], rows: [], rowsAffected: 0, notices: [] };
		},
		observe: async () => observed.promise,
	} as unknown as LixBinding;
	const controller = startWorkerHost(
		{
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => {
				receive = listener;
			},
		},
		async () => binding,
	);
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
		operation: { kind: "execute", sql: "active write", params: [] },
	});
	await vi.waitFor(() => expect(writes).toEqual(["active write"]));
	receive({
		id: 3,
		sessionId: 0,
		operation: { kind: "execute", sql: "queued write", params: [] },
	});
	receive({
		id: 4,
		sessionId: 0,
		operation: { kind: "observe", sql: "SELECT value", params: [] },
	});
	const closing = controller.close();
	expect(closed).not.toHaveBeenCalled();
	let closeFinished = false;
	void closing.then(() => {
		closeFinished = true;
	});
	observed.resolve({
		setTelemetryParent() {},
		next: async () => undefined,
		close: observationClose,
	});
	active.resolve();
	await vi.waitFor(() => expect(observationClose).toHaveBeenCalledOnce());
	for (let i = 0; i < 20; i++) await Promise.resolve();
	expect(closeFinished).toBe(false);
	expect(closed).not.toHaveBeenCalled();
	observationCloseBarrier.resolve();
	await closing;
	expect(writes).toEqual(["active write"]);
	expect(responses).toContainEqual(expect.objectContaining({ id: 3, ok: false }));
	expect(responses).toContainEqual(expect.objectContaining({ id: 4, ok: false }));
	expect(observationClose).toHaveBeenCalledTimes(1);
	expect(closed).toHaveBeenCalledTimes(1);
});

test("worker forwards the durable transaction commit receipt", async () => {
	const responses: WorkerResponse[] = [];
	let receive!: (message: WorkerInput) => void;
	const span = { before: "before", after: "after" };
	const binding = {
		setTelemetryParent() {},
		beginTransaction: async () => ({ commit: async () => ({ commit: span }) }),
	} as unknown as LixBinding;
	startWorkerHost({
		postMessage: (message) => responses.push(message),
		onMessage: (listener) => { receive = listener; },
	}, async () => binding);
	receive({ id: 1, sessionId: 0, operation: { kind: "open", storage: { kind: "memory" } } });
	await vi.waitFor(() => expect(responses.some((response) => "ok" in response && response.id === 1)).toBe(true));
	receive({ id: 2, sessionId: 0, operation: { kind: "beginTransaction" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: 1 }));
	receive({ id: 3, sessionId: 0, operation: { kind: "transaction.commit", transactionId: 1 } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 3, ok: true, value: { commit: span } }));
});

test("worker host routes sync health to the local binding", async () => {
	const responses: WorkerResponse[] = [];
	let receive: (message: WorkerInput) => void = () => undefined;
	const endpoint: WorkerHostEndpoint = { postMessage: (message) => { responses.push(message); }, onMessage: (listener) => { receive = listener; } };
	const health = { state: "stalled", appliedCursor: 492, observedCursor: 543, failures: { descriptor: { code: "OFFLINE", message: "unavailable" } }, terminalError: null };
	const binding = { syncHealth: vi.fn(async () => health), setTelemetryParent() {} } as unknown as LixBinding;
	startWorkerHost(endpoint, async () => binding);
	receive({ id: 1, sessionId: 0, operation: { kind: "open", storage: { kind: "memory" } } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 1, ok: true }));
	receive({ id: 2, sessionId: 0, operation: { kind: "syncHealth" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: health }));
	expect(binding.syncHealth).toHaveBeenCalledOnce();
});

test("worker host routes offline editing preparation to the local binding", async () => {
	const responses: WorkerResponse[] = [];
	let receive: (message: WorkerInput) => void = () => undefined;
	const endpoint: WorkerHostEndpoint = {
		postMessage: (message) => { responses.push(message); },
		onMessage: (listener) => { receive = listener; },
	};
	const prepareOfflineEditing = vi.fn(async () => undefined);
	const binding = { prepareOfflineEditing, setTelemetryParent() {} } as unknown as LixBinding;
	startWorkerHost(endpoint, async () => binding);
	receive({ id: 1, sessionId: 0, operation: { kind: "open", storage: { kind: "memory" } } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 1, ok: true }));
	receive({ id: 2, sessionId: 0, operation: { kind: "prepareOfflineEditing" } });
	await vi.waitFor(() => expect(responses).toContainEqual({ id: 2, ok: true, value: undefined }));
	expect(prepareOfflineEditing).toHaveBeenCalledOnce();
});

test("a credential callback from a nonresponsive page cannot leave opening pending", async () => {
	vi.useFakeTimers();
	try {
		const responses: WorkerResponse[] = [];
		let receive!: (input: WorkerInput) => void;
		const host = startWorkerHost(
			{
		postMessage: (message) => responses.push(message),
		onMessage: (listener) => { receive = listener; },
	},
			async (_storage, _telemetry, _parent, server) => {
				await server!.headerProvider!();
				throw new Error("Late credentials must never reach this line");
			},
		);
		receive({
			id: 1,
			sessionId: 0,
			operation: {
				kind: "open",
				storage: { kind: "memory" },
				telemetryEnabled: false,
				progressEnabled: false,
				server: {
					url: "https://example.invalid",
					headers: [],
					dynamicHeaders: true,
				},
			},
		});
		await vi.advanceTimersByTimeAsync(15_000);
		expect(responses).toContainEqual(
			expect.objectContaining({
				id: 1,
				ok: false,
				error: expect.objectContaining({ code: "LIX_CREDENTIALS_TIMEOUT" }),
			}),
		);
		const header = responses.find(
			(message) => "kind" in message && message.kind === "sync.headers",
		);
		if (!header || !("requestId" in header))
			throw new Error("Expected credential request");
		receive({
			kind: "sync.headers.result",
			requestId: header.requestId,
			result: { ok: true, headers: [] },
		});
		await host.close();
		expect(
			responses.filter((message) => "ok" in message && message.id === 1),
		).toHaveLength(1);
	} finally {
		vi.useRealTimers();
	}
});

for (const reasonName of ["TimeoutError", "AbortError"] as const) {
  for (const streaming of [false, true]) {
    test(`worker bridge preserves ${reasonName} during ${streaming ? "body pull" : "headers"}`, async () => {
      const responses: WorkerResponse[] = [];
      let receive!: (message: WorkerInput) => void;
      let transport!: import("../http-transport.js").HttpTransport;
      const host = startWorkerHost({
        postMessage: message => {responses.push(message);},
        onMessage: listener => {receive = listener;},
      }, async (_storage, _telemetry, _parent, server) => {
        transport = server!.transport!;
        return {setTelemetryParent() {}, close: async () => {}} as unknown as LixBinding;
      });
      receive({id: 1, sessionId: 0, operation: {
        kind: "open", storage: {kind: "memory"}, server: {url: "https://example.test", headers: []},
      }});
      await vi.waitFor(() => expect(responses).toContainEqual({id: 1, ok: true}));
      const controller = new AbortController();
      const pending = transport({url: "https://example.test", init: {signal: controller.signal},
        response: streaming ? {mode: "streaming"} : {mode: "buffered", maxBytes: 8}});
      const message = responses.find(message => "kind" in message && message.kind === "sync.fetch");
      if (!message || !("requestId" in message)) throw new Error("Missing fetch request");
      let completion: Promise<unknown> = pending;
      if (streaming) {
        receive({kind: "sync.fetch.result", requestId: message.requestId, result: {
          ok: true, response: {streaming: true, status: 200, statusText: "OK", headers: []},
        }});
        completion = (await pending).text();
        await vi.waitFor(() => expect(responses).toContainEqual({kind: "sync.fetch.stream.pull", requestId: message.requestId}));
      }
      const checked = expect(completion).rejects.toMatchObject({
        code: reasonName === "TimeoutError" ? "LIX_TRANSPORT_NETWORK" : "LIX_TRANSPORT_ABORTED",
      });
      controller.abort(new DOMException("private reason", reasonName));
      await checked;
      expect(responses).toContainEqual({kind: "sync.fetch.cancel", requestId: message.requestId});
      await host.close();
    });
  }
}

for (const reasonName of ["TimeoutError", "AbortError"] as const) {
  test(`worker stream abort reaches a backpressured body (${reasonName})`, async () => {
    const responses: WorkerResponse[] = [];
    let receive!: (message: WorkerInput) => void;
    let transport!: import("../http-transport.js").HttpTransport;
    const host = startWorkerHost({
      postMessage: message => {responses.push(message);},
      onMessage: listener => {receive = listener;},
    }, async (_storage, _telemetry, _parent, server) => {
      transport = server!.transport!;
      return {setTelemetryParent() {}, close: async () => {}} as unknown as LixBinding;
    });
    try {
      receive({id: 1, sessionId: 0, operation: {
        kind: "open", storage: {kind: "memory"}, server: {url: "https://example.test", headers: []},
      }});
      await vi.waitFor(() => expect(responses).toContainEqual({id: 1, ok: true}));
      const controller = new AbortController();
      const pending = transport({url: "https://example.test", init: {signal: controller.signal}, response: {mode: "streaming"}});
      const message = responses.find(message => "kind" in message && message.kind === "sync.fetch");
      if (!message || !("requestId" in message)) throw new Error("Missing fetch request");
      receive({kind: "sync.fetch.result", requestId: message.requestId, result: {
        ok: true, response: {streaming: true, status: 200, statusText: "OK", headers: []},
      }});
      const response = await pending;
      await vi.waitFor(() => expect(responses).toContainEqual({kind: "sync.fetch.stream.pull", requestId: message.requestId}));
      receive({kind: "sync.fetch.stream.result", requestId: message.requestId, result: {
        ok: true, done: false, chunk: new Uint8Array([7]),
      }});
      // With one queued chunk the producer is backpressured: no RPC pull is pending.
      for (let i = 0; i < 10; i++) await Promise.resolve();
      const pulls = responses.filter(message => "kind" in message && message.kind === "sync.fetch.stream.pull").length;
      expect(pulls).toBe(1);
      controller.abort(new DOMException("cancelled while backpressured", reasonName));
      await expect(response.body!.getReader().read()).rejects.toMatchObject({
        code: reasonName === "TimeoutError" ? "LIX_TRANSPORT_NETWORK" : "LIX_TRANSPORT_ABORTED",
      });
      expect(responses.filter(message => "kind" in message && message.kind === "sync.fetch.stream.pull")).toHaveLength(pulls);
    } finally {await host.close();}
  });
}

  test("worker disconnect errors a backpressured response body", async () => {
    const responses: WorkerResponse[] = [];
    let receive!: (message: WorkerInput) => void;
    let transport!: import("../http-transport.js").HttpTransport;
    const host = startWorkerHost({
      postMessage: message => {responses.push(message);},
      onMessage: listener => {receive = listener;},
    }, async (_storage, _telemetry, _parent, server) => {
      transport = server!.transport!;
      return {setTelemetryParent() {}, close: async () => {}} as unknown as LixBinding;
    });
    try {
      receive({id: 1, sessionId: 0, operation: {
        kind: "open", storage: {kind: "memory"}, server: {url: "https://example.test", headers: []},
      }});
      await vi.waitFor(() => expect(responses).toContainEqual({id: 1, ok: true}));
      const controller = new AbortController();
      const pending = transport({url: "https://example.test", init: {signal: controller.signal}, response: {mode: "streaming"}});
      const message = responses.find(message => "kind" in message && message.kind === "sync.fetch");
      if (!message || !("requestId" in message)) throw new Error("Missing fetch request");
      receive({kind: "sync.fetch.result", requestId: message.requestId, result: {
        ok: true, response: {streaming: true, status: 200, statusText: "OK", headers: []},
      }});
      const response = await pending;
      await vi.waitFor(() => expect(responses).toContainEqual({kind: "sync.fetch.stream.pull", requestId: message.requestId}));
      receive({kind: "sync.fetch.stream.result", requestId: message.requestId, result: {
        ok: true, done: false, chunk: new Uint8Array([7]),
      }});
      // With one queued chunk the producer is backpressured: no RPC pull is pending.
      for (let i = 0; i < 10; i++) await Promise.resolve();
      const pulls = responses.filter(message => "kind" in message && message.kind === "sync.fetch.stream.pull").length;
      expect(pulls).toBe(1);
      await host.close();
      expect(responses).toContainEqual({kind: "sync.fetch.cancel", requestId: message.requestId});
      await expect(response.body!.getReader().read()).rejects.toMatchObject({
        code: "LIX_TRANSPORT_ABORTED",
      });
      expect(responses.filter(message => "kind" in message && message.kind === "sync.fetch.stream.pull")).toHaveLength(pulls);
    } finally {await host.close();}
  });

test("worker disconnect cancels the paired client's retained fetch and reader", async () => {
  let receiveHost!: (message: WorkerInput) => void;
  let receiveClient!: (message: WorkerResponse) => void;
  let transport!: import("../http-transport.js").HttpTransport;
  let fetchSignal: AbortSignal | undefined;
  let cancelled = false;
  const replies: WorkerResponse[] = [];
  const client = new LixWorkerClient({
    postMessage: message => {queueMicrotask(() => receiveHost(message));},
    onMessage: listener => {receiveClient = listener;},
    onFatal() {}, ref() {}, unref() {}, async terminate() {},
  });
  client.beginLease(undefined, undefined, {
    url: "https://example.test",
    fetch: async (_input, init) => {
      fetchSignal = init?.signal ?? undefined;
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {controller.enqueue(new Uint8Array([7]));},
        cancel() {cancelled = true;},
      }));
    },
  });
  const host = startWorkerHost({
    postMessage: message => {replies.push(message); queueMicrotask(() => receiveClient(message));},
    onMessage: listener => {receiveHost = listener;},
  }, async (_storage, _telemetry, _parent, server) => {
    transport = server!.transport!;
    return {setTelemetryParent() {}, close: async () => {}} as unknown as LixBinding;
  });
  try {
    receiveHost({id: 1, sessionId: 0, operation: {
      kind: "open", storage: {kind: "memory"}, server: {url: "https://example.test", headers: []},
    }});
    await vi.waitFor(() => expect(replies).toContainEqual({id: 1, ok: true}));
    const response = await transport({url: "https://example.test", init: {}, response: {mode: "streaming"}});
    await vi.waitFor(() => expect(replies.some(message => "kind" in message && message.kind === "sync.fetch.stream.pull")).toBe(true));
    for (let index = 0; index < 10; index++) await Promise.resolve();
    expect(fetchSignal?.aborted).toBe(false);
    expect(cancelled).toBe(false);
    await host.close();
    await vi.waitFor(() => expect(cancelled).toBe(true));
    expect(fetchSignal?.aborted).toBe(true);
    await expect(response.body!.getReader().read()).rejects.toMatchObject({code: "LIX_TRANSPORT_ABORTED"});
  } finally {await host.close();}
});

test("worker disconnect during header handoff retires the paired client fetch", async () => {
  let receiveHost!: (message: WorkerInput) => void;
  let receiveClient!: (message: WorkerResponse) => void;
  let transport!: import("../http-transport.js").HttpTransport;
  let host!: ReturnType<typeof startWorkerHost>;
  let closing: Promise<void> | undefined;
  let fetchSignal: AbortSignal | undefined;
  let cancelled = false;
  const replies: WorkerResponse[] = [];
  const client = new LixWorkerClient({
    postMessage: message => {
      receiveHost(message);
      if ("kind" in message && message.kind === "sync.fetch.result" && message.result.ok) {
        closing = host.close();
      }
    },
    onMessage: listener => {receiveClient = listener;},
    onFatal() {}, ref() {}, unref() {}, async terminate() {},
  });
  client.beginLease(undefined, undefined, {
    url: "https://example.test",
    fetch: async (_input, init) => {
      fetchSignal = init?.signal ?? undefined;
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {controller.enqueue(new Uint8Array([7]));},
        cancel() {cancelled = true;},
      }));
    },
  });
  host = startWorkerHost({
    postMessage: message => {replies.push(message); queueMicrotask(() => receiveClient(message));},
    onMessage: listener => {receiveHost = listener;},
  }, async (_storage, _telemetry, _parent, server) => {
    transport = server!.transport!;
    return {setTelemetryParent() {}, close: async () => {}} as unknown as LixBinding;
  });
  try {
    receiveHost({id: 1, sessionId: 0, operation: {
      kind: "open", storage: {kind: "memory"}, server: {url: "https://example.test", headers: []},
    }});
    await vi.waitFor(() => expect(replies).toContainEqual({id: 1, ok: true}));
    await expect(transport({url: "https://example.test", init: {}, response: {mode: "streaming"}}))
      .rejects.toMatchObject({code: "LIX_TRANSPORT_ABORTED"});
    await closing;
    await vi.waitFor(() => expect(cancelled).toBe(true));
    expect(fetchSignal?.aborted).toBe(true);
    expect(replies.some(message => "kind" in message && message.kind === "sync.fetch.stream.pull"))
      .toBe(false);
  } finally {await host.close();}
});

test("worker teardown permits only the scoped session DELETE until owner detach completes", async () => {
  const repositoryId = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
  const replies: WorkerResponse[] = [];
  const forwarded: string[] = [];
  let receive!: (message: WorkerInput) => void;
  let transport!: import("../http-transport.js").HttpTransport;
  const host = startWorkerHost({
    postMessage(message) {
      replies.push(message);
      if ("kind" in message && message.kind === "sync.fetch") {
        forwarded.push(`${message.request.method}:${message.request.url}`);
        queueMicrotask(() => receive({
          kind: "sync.fetch.result",
          requestId: message.requestId,
          result: { ok: true, response: { status: 204, statusText: "No Content", headers: [], body: new Uint8Array() } },
        }));
      }
    },
    onMessage(listener) { receive = listener; },
  }, async (_storage, _telemetry, _parent, server) => {
    transport = server!.transport!;
    return {
      setTelemetryParent() {},
      async close() {},
    } as unknown as LixBinding;
  });
  receive({ id: 1, sessionId: 0, operation: {
    kind: "open",
    storage: { kind: "memory" },
    telemetryEnabled: false,
    progressEnabled: false,
    server: { url: `https://example.test/lix/${repositoryId}`, headers: [] },
  } });
  await vi.waitFor(() => expect(replies).toContainEqual({ id: 1, ok: true }));
  await host.close(async () => {
    await expect(transport({
      url: `https://example.test/lix/v1/${repositoryId}/sync/pull`,
      init: { method: "POST" },
      response: { mode: "buffered", maxBytes: 128 },
    })).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
    const response = await transport({
      url: `https://example.test/lix/v1/${repositoryId}/session`,
      init: { method: "DELETE", headers: [["lix-session-id", "session-a"]] },
      response: { mode: "buffered", maxBytes: 128 },
    });
    expect(response.status).toBe(204);
  });
  expect(forwarded).toEqual([`DELETE:https://example.test/lix/v1/${repositoryId}/session`]);
  await expect(transport({
    url: `https://example.test/lix/v1/${repositoryId}/session`,
    init: { method: "DELETE", headers: [["lix-session-id", "session-a"]] },
    response: { mode: "buffered", maxBytes: 128 },
  })).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
});

test("worker teardown disables its session DELETE lane after owner detach fails", async () => {
  const repositoryId = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
  const replies: WorkerResponse[] = [];
  let receive!: (message: WorkerInput) => void;
  let transport!: import("../http-transport.js").HttpTransport;
  const host = startWorkerHost({
    postMessage(message) {
      replies.push(message);
      if ("kind" in message && message.kind === "sync.fetch") {
        queueMicrotask(() => receive({
          kind: "sync.fetch.result",
          requestId: message.requestId,
          result: { ok: true, response: { status: 204, statusText: "No Content", headers: [], body: new Uint8Array() } },
        }));
      }
    },
    onMessage(listener) { receive = listener; },
  }, async (_storage, _telemetry, _parent, server) => {
    transport = server!.transport!;
    return { setTelemetryParent() {}, async close() {} } as unknown as LixBinding;
  });
  receive({ id: 1, sessionId: 0, operation: {
    kind: "open",
    storage: { kind: "memory" },
    telemetryEnabled: false,
    progressEnabled: false,
    server: { url: `https://example.test/lix/${repositoryId}`, headers: [] },
  } });
  await vi.waitFor(() => expect(replies).toContainEqual({ id: 1, ok: true }));
  const closeError = new Error("owner detach failed");
  await expect(host.close(async () => {
    await transport({
      url: `https://example.test/lix/v1/${repositoryId}/session`,
      init: { method: "DELETE", headers: [["lix-session-id", "session-a"]] },
      response: { mode: "buffered", maxBytes: 128 },
    });
    throw closeError;
  })).rejects.toBe(closeError);
  await expect(transport({
    url: `https://example.test/lix/v1/${repositoryId}/session`,
    init: { method: "DELETE", headers: [["lix-session-id", "session-a"]] },
    response: { mode: "buffered", maxBytes: 128 },
  })).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
});
