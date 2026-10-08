import { expect, test, vi } from "vitest";
import {
	WorkerOperationScheduler,
	type ScheduledWorkerOperation,
} from "./operation-scheduler.js";

function deferred<T = void>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	const promise = new Promise<T>((done) => (resolve = done));
	return { promise, resolve };
}

function work(
	scope: ReturnType<WorkerOperationScheduler["createScope"]>,
	lane: string,
	run: () => Promise<void>,
	onRejected: (error: Error) => void = () => {},
	sessionId?: number,
	pool?: "finite" | "independent" | "observer",
): ScheduledWorkerOperation {
	return { scope, lane, run, onRejected, sessionId, pool };
}

test("an independent session can start around a same-session FIFO backlog", async () => {
	const scheduler = new WorkerOperationScheduler({ maxActive: 2 });
	const scope = scheduler.createScope();
	const first = deferred();
	const calls: string[] = [];
	scheduler.schedule(
		work(scope, "session:1", async () => {
			calls.push("a1");
			await first.promise;
		}),
	);
	scheduler.schedule(
		work(scope, "session:1", async () => {
			calls.push("a2");
		}),
	);
	scheduler.schedule(
		work(scope, "session:2", async () => {
			calls.push("b1");
		}, () => {}, 2),
	);
	await vi.waitFor(() => expect(calls).toEqual(["a1", "b1"]));
	first.resolve();
	await vi.waitFor(() => expect(calls).toEqual(["a1", "b1", "a2"]));
	await scheduler.drainScope(scope);
});

test("independent observation work does not consume finite operation slots", async () => {
	const scheduler = new WorkerOperationScheduler({
		maxActive: 1,
		maxIndependentActive: 1,
	});
	const scope = scheduler.createScope();
	const longPoll = deferred();
	const finite = deferred();
	const calls: string[] = [];
	scheduler.schedule(
		work(scope, "observer:1", async () => {
			calls.push("observer");
			await longPoll.promise;
		}, () => {}, 1, "independent"),
	);
	scheduler.schedule(
		work(scope, "session:1", async () => {
			calls.push("finite");
			await finite.promise;
		}, () => {}, 1),
	);
	await vi.waitFor(() => expect(calls).toEqual(["observer", "finite"]));
	longPoll.resolve();
	finite.resolve();
	await scheduler.drainScope(scope);
});

test("observer admission reservations are global across scopes and reusable", () => {
	const scheduler = new WorkerOperationScheduler({ maxObserverRegistrations: 2 });
	const firstScope = scheduler.createScope();
	const secondScope = scheduler.createScope();
	expect(firstScope).not.toBe(secondScope);
	const first = scheduler.reserveObserverSlot();
	const second = scheduler.reserveObserverSlot();
	expect(first).toBeDefined();
	expect(second).toBeDefined();
	expect(scheduler.reserveObserverSlot()).toBeUndefined();

	first!.release();
	const replacement = scheduler.reserveObserverSlot();
	expect(replacement).toBeDefined();
	second!.release();
	replacement!.release();
	// Release is idempotent so teardown and a late response can both retire it.
	replacement!.release();
	const afterTeardown = scheduler.reserveObserverSlot();
	expect(afterTeardown).toBeDefined();
	afterTeardown!.release();
});

test("observer admission waiters use a separate bounded quota and cannot starve finite work", async () => {
	const scheduler = new WorkerOperationScheduler({
		maxActive: 1,
		maxIndependentActive: 1,
		maxObserverRegistrations: 1,
		maxQueued: 1,
		maxQueuedPerSession: 1,
		maxQueuedObserverRegistrations: 1,
		maxQueuedObserverRegistrationsPerSession: 1,
		queueWaitMs: null,
	});
	const scope = scheduler.createScope();
	const blocker = deferred<void>();
	const started: string[] = [];
	scheduler.schedule(
		work(scope, "finite:blocker", async () => {
			started.push("blocker");
			await blocker.promise;
		}, () => {}, 1),
	);
	await vi.waitFor(() => expect(started).toEqual(["blocker"]));

	const occupied = scheduler.reserveObserverSlot();
	expect(occupied).toBeDefined();
	const observerQueued = vi.fn();
	let promotedSlot: import("./operation-scheduler.js").ObserverSlot | undefined;
	expect(
		scheduler.schedule({
			scope,
			lane: "observe:1",
			sessionId: 1,
			pool: "independent",
			queueWaitMs: null,
			onQueued: observerQueued,
			observerRegistration: {
				reserve: () => scheduler.reserveObserverSlot(),
				onReserved: (slot) => {
					promotedSlot = slot;
				},
			},
			run: async () => {
				started.push("observer");
				promotedSlot?.release();
			},
			onRejected: () => {},
		}),
	).toBe(true);
	expect(observerQueued).toHaveBeenCalledOnce();

	const finiteQueued = vi.fn();
	expect(
		scheduler.schedule({
			...work(scope, "finite:followup", async () => {
				started.push("finite");
			}, () => {}, 1),
			queueWaitMs: null,
			onQueued: finiteQueued,
		}),
	).toBe(true);
	expect(finiteQueued).toHaveBeenCalledOnce();
	blocker.resolve();
	await vi.waitFor(() => expect(started).toContain("finite"));
	expect(started).not.toContain("observer");
	occupied!.release();
	await vi.waitFor(() => expect(started).toContain("observer"));
	await scheduler.drainScope(scope);
});

test("a lifecycle barrier fences its host scope without blocking other hosts", async () => {
	const scheduler = new WorkerOperationScheduler();
	const scopeA = scheduler.createScope();
	const scopeB = scheduler.createScope();
	const barrier = deferred();
	const calls: string[] = [];
	scheduler.schedule({
		...work(scopeA, "lifecycle", async () => {
			calls.push("open-a");
			await barrier.promise;
		}),
		barrier: true,
	});
	scheduler.schedule(
		work(scopeA, "session:1", async () => {
			calls.push("after-open-a");
		}),
	);
	scheduler.schedule(
		work(scopeB, "session:1", async () => {
			calls.push("resident-b");
		}),
	);
	await vi.waitFor(() => expect(calls).toEqual(["open-a", "resident-b"]));
	barrier.resolve();
	await vi.waitFor(() => expect(calls).toEqual(["open-a", "resident-b", "after-open-a"]));
	await Promise.all([scheduler.drainScope(scopeA), scheduler.drainScope(scopeB)]);
});

test("queue bounds and timeout reject waiting work without starting it", async () => {
	const scheduler = new WorkerOperationScheduler({
		maxActive: 1,
		maxQueued: 1,
		maxQueuedPerSession: 1,
		queueWaitMs: 15,
	});
	const scope = scheduler.createScope();
	const first = deferred();
	const started: string[] = [];
	let timedOut: (Error & { code?: string }) | undefined;
	scheduler.schedule(
		work(scope, "session:1", async () => {
			started.push("active");
			await first.promise;
		}, () => {}, 1),
	);
	scheduler.schedule(
		work(
			scope,
			"session:1",
			async () => {
				started.push("must-not-start");
			},
			(error) => (timedOut = error),
			1,
		),
	);
	const overflowAccepted = scheduler.schedule(
		work(scope, "session:2", async () => {
			started.push("overflow");
		}),
	);
	expect(overflowAccepted).toBe(false);
	await vi.waitFor(() => expect(timedOut?.code).toBe("LIX_WORKER_QUEUE_TIMEOUT"));
	expect(started).toEqual(["active"]);
	first.resolve();
	await scheduler.drainScope(scope);
});

test("drain resolves when the final queued operation times out behind another scope", async () => {
	const scheduler = new WorkerOperationScheduler({
		maxActive: 1,
		queueWaitMs: 15,
	});
	const activeScope = scheduler.createScope();
	const waitingScope = scheduler.createScope();
	const active = deferred();
	let rejected = false;
	let drained = false;
	scheduler.schedule(
		work(activeScope, "session:1", async () => await active.promise),
	);
	scheduler.schedule(
		work(
			waitingScope,
			"session:1",
			async () => {},
			(error) =>
				(rejected = (error as Error & { code?: string }).code === "LIX_WORKER_QUEUE_TIMEOUT"),
		),
	);
	void scheduler.drainScope(waitingScope).then(() => (drained = true));
	await vi.waitFor(() => expect(rejected).toBe(true));
	await vi.waitFor(() => expect(drained).toBe(true));
	active.resolve();
	await scheduler.drainScope(activeScope);
});
