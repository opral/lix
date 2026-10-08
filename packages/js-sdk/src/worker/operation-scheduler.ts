export const WORKER_OPERATION_MAX_ACTIVE = 4;
export const WORKER_OPERATION_MAX_INDEPENDENT_ACTIVE = 4;
export const WORKER_OPERATION_MAX_OBSERVERS = 32;
export const WORKER_OPERATION_MAX_OBSERVER_ACTIVE = WORKER_OPERATION_MAX_OBSERVERS;
export const WORKER_OPERATION_MAX_QUEUED = 128;
export const WORKER_OPERATION_MAX_QUEUED_PER_SESSION = 32;
export const WORKER_OPERATION_QUEUE_WAIT_MS = 30_000;
// Independent resource classes keep observer/control cleanup admissible when
// ordinary operations have filled their bounded request budget.
export const WORKER_CLIENT_MAX_PENDING = 160;
export const WORKER_CLIENT_MAX_ORDINARY_PENDING = 120;
export const WORKER_CLIENT_MAX_CONTROL_PENDING = 8;
export const WORKER_CLIENT_MAX_OBSERVER_CLOSE_PENDING = WORKER_OPERATION_MAX_OBSERVERS;

export function workerQueueFullError(): Error & { code: string } {
	return Object.assign(new Error("Worker operation queue is full"), {
		code: "LIX_WORKER_QUEUE_FULL",
	});
}

export function workerQueueTimeoutError(): Error & { code: string } {
	return Object.assign(new Error("Worker operation waited too long to start"), {
		code: "LIX_WORKER_QUEUE_TIMEOUT",
	});
}

export type WorkerOperationScope = object;

export type TransactionSlot = { adopt(): void };
export type ObserverSlot = { release(): void };

export type ScheduledWorkerOperation = {
	scope: WorkerOperationScope;
	lane: string;
	pool?: "finite" | "independent" | "observer";
	sessionId?: number;
	barrier?: boolean;
	reserveTransactionSlot?: boolean;
	queueWaitMs?: number | null;
	onQueued?(): void;
	run(transactionSlot?: TransactionSlot): Promise<void>;
	onRejected(error: Error): void;
};

export type WorkerOperationSchedulerLimits = {
	maxActive: number;
	maxIndependentActive: number;
	maxObserverActive: number;
	maxObserverRegistrations: number;
	maxQueued: number;
	maxQueuedPerSession: number;
	queueWaitMs: number;
};

/** Shared admission and per-scope FIFO scheduling for worker-host operations. */
export class WorkerOperationScheduler {
	private readonly limits: WorkerOperationSchedulerLimits;
	private readonly queue: ScheduledWorkerOperation[] = [];
	private readonly queueTimers = new Map<
		ScheduledWorkerOperation,
		ReturnType<typeof setTimeout>
	>();
	private readonly activeLanes = new Map<WorkerOperationScope, Set<string>>();
	private readonly activeByScope = new Map<WorkerOperationScope, number>();
	private readonly activeBarriers = new Set<WorkerOperationScope>();
	private readonly queuedByScopeSession = new Map<
		WorkerOperationScope,
		Map<number, number>
	>();
	private readonly transactionSlotsByScopeSession = new Map<
		WorkerOperationScope,
		Map<number, number>
	>();
	private readonly sessionCounts = new Map<WorkerOperationScope, number>();
	private readonly drainWaiters = new Set<{
		scope: WorkerOperationScope;
		resolve(): void;
	}>();
	private activeFinite = 0;
	private activeIndependent = 0;
	private activeObservers = 0;
	private reservedObserverSlots = 0;
	private transactionSlots = 0;
	private pumping = false;

	constructor(limits: Partial<WorkerOperationSchedulerLimits> = {}) {
		const maxObserverRegistrations =
			limits.maxObserverRegistrations ?? WORKER_OPERATION_MAX_OBSERVERS;
		this.limits = {
			maxActive: WORKER_OPERATION_MAX_ACTIVE,
			maxIndependentActive: WORKER_OPERATION_MAX_INDEPENDENT_ACTIVE,
			maxQueued: WORKER_OPERATION_MAX_QUEUED,
			maxQueuedPerSession: WORKER_OPERATION_MAX_QUEUED_PER_SESSION,
			queueWaitMs: WORKER_OPERATION_QUEUE_WAIT_MS,
			...limits,
			// Every admitted observer must be able to hold a read concurrently.
			maxObserverActive: Math.max(
				limits.maxObserverActive ?? WORKER_OPERATION_MAX_OBSERVER_ACTIVE,
				maxObserverRegistrations,
			),
			maxObserverRegistrations,
		};
	}

	createScope(): WorkerOperationScope {
		return {};
	}

	/** Reserve one global observer slot, including while registration is pending. */
	reserveObserverSlot(): ObserverSlot | undefined {
		if (this.reservedObserverSlots >= this.limits.maxObserverRegistrations)
			return undefined;
		this.reservedObserverSlots++;
		let active = true;
		return {
			release: () => {
				if (!active) return;
				active = false;
				this.reservedObserverSlots--;
				this.pump();
			},
		};
	}

	/** Returns false only when the bounded waiting budget is exhausted. */
	schedule(work: ScheduledWorkerOperation): boolean {
		if (this.canStartImmediately(work)) {
			if (work.reserveTransactionSlot && !this.canReserveTransactionSlot(work))
				return false;
			this.start(work);
			return true;
		}
		if (!this.hasQueueCapacity(work)) return false;
		this.enqueue(work);
		this.pump();
		return true;
	}

	/** Transfer an open transaction's reserved cleanup slot into a queued action. */
	scheduleUsingTransactionSlot(work: ScheduledWorkerOperation): boolean {
		if (work.sessionId === undefined || this.transactionSlotsFor(work.scope, work.sessionId) === 0)
			return false;
		this.changeTransactionSlots(work.scope, work.sessionId, -1);
		this.enqueue(work);
		this.pump();
		return true;
	}

	releaseTransactionSlot(scope: WorkerOperationScope, sessionId: number): void {
		if (this.transactionSlotsFor(scope, sessionId) > 0)
			this.changeTransactionSlots(scope, sessionId, -1);
	}

	restoreTransactionSlot(scope: WorkerOperationScope, sessionId: number): void {
		this.changeTransactionSlots(scope, sessionId, 1);
	}

	setSessionCount(scope: WorkerOperationScope, count: number): void {
		if (count > 0) this.sessionCounts.set(scope, count);
		else this.sessionCounts.delete(scope);
	}

	totalOpenSessions(): number {
		let total = 0;
		for (const count of this.sessionCounts.values()) total += count;
		return total;
	}

	cancelQueued(
		scope: WorkerOperationScope,
		error: Error,
		predicate: (work: ScheduledWorkerOperation) => boolean = () => true,
	): void {
		for (let index = this.queue.length - 1; index >= 0; index--) {
			const work = this.queue[index];
			if (work.scope !== scope || !predicate(work)) continue;
			this.removeQueued(index);
			try {
				work.onRejected(error);
			} catch {
				// One broken response callback must not prevent draining the rest.
			}
		}
		this.pump();
		this.notifyDrained();
	}

	drainScope(scope: WorkerOperationScope): Promise<void> {
		if (
			this.activeByScope.get(scope) === undefined &&
			!this.queue.some((work) => work.scope === scope)
		)
			return Promise.resolve();
		return new Promise((resolve) => this.drainWaiters.add({ scope, resolve }));
	}

	releaseScope(scope: WorkerOperationScope): void {
		if (this.activeByScope.get(scope) !== undefined) return;
		if (this.queue.some((work) => work.scope === scope)) return;
		if (this.transactionSlotsByScopeSession.has(scope)) return;
		this.sessionCounts.delete(scope);
		this.queuedByScopeSession.delete(scope);
		this.transactionSlotsByScopeSession.delete(scope);
		this.activeLanes.delete(scope);
		this.activeBarriers.delete(scope);
	}

	private hasQueueCapacity(work: ScheduledWorkerOperation): boolean {
		if (this.queue.length + this.transactionSlots >= this.limits.maxQueued) return false;
		if (work.sessionId === undefined) return true;
		return (
			this.queuedFor(work.scope, work.sessionId) +
			this.transactionSlotsFor(work.scope, work.sessionId) <
			this.limits.maxQueuedPerSession
		);
	}

	private canStartImmediately(work: ScheduledWorkerOperation): boolean {
		if (!this.hasPoolCapacity(work)) return false;
		if (this.activeBarriers.has(work.scope)) return false;
		if (work.barrier) {
			return (
				(this.activeByScope.get(work.scope) ?? 0) === 0 &&
				!this.queue.some((queued) => queued.scope === work.scope)
			);
		}
		if (this.activeLanes.get(work.scope)?.has(work.lane)) return false;
		return !this.queue.some(
			(queued) =>
				queued.scope === work.scope &&
				(queued.barrier || (!work.barrier && queued.lane === work.lane)),
		);
	}

	private canReserveTransactionSlot(work: ScheduledWorkerOperation): boolean {
		if (work.sessionId === undefined) return false;
		return (
			this.queue.length + this.transactionSlots < this.limits.maxQueued &&
			this.queuedFor(work.scope, work.sessionId) +
				this.transactionSlotsFor(work.scope, work.sessionId) <
				this.limits.maxQueuedPerSession
		);
	}

	private enqueue(work: ScheduledWorkerOperation): void {
		this.queue.push(work);
		if (work.sessionId !== undefined) this.changeQueued(work.scope, work.sessionId, 1);
		try {
			work.onQueued?.();
		} catch {
			// The bounded queue remains authoritative if a transport event is lost.
		}
		const waitMs = work.queueWaitMs === undefined ? this.limits.queueWaitMs : work.queueWaitMs;
		if (waitMs !== null) {
			const timer = setTimeout(() => {
				const index = this.queue.indexOf(work);
				if (index < 0) return;
				this.removeQueued(index);
				try {
					work.onRejected(workerQueueTimeoutError());
				} catch {
					// A timed-out caller may already have disconnected.
				}
				this.pump();
				this.notifyDrained();
			}, waitMs);
			this.queueTimers.set(work, timer);
		}
	}

	private removeQueued(index: number): ScheduledWorkerOperation {
		const [work] = this.queue.splice(index, 1);
		const timer = this.queueTimers.get(work);
		if (timer !== undefined) clearTimeout(timer);
		this.queueTimers.delete(work);
		if (work.sessionId !== undefined) this.changeQueued(work.scope, work.sessionId, -1);
		return work;
	}

	private pump(): void {
		if (this.pumping) return;
		this.pumping = true;
		try {
			while (
				this.activeFinite < this.limits.maxActive ||
				this.activeIndependent < this.limits.maxIndependentActive ||
				this.activeObservers < this.limits.maxObserverActive
			) {
				let started = false;
				for (let index = 0; index < this.queue.length; index++) {
					const work = this.queue[index];
					if (!this.hasPoolCapacity(work)) continue;
					if (this.activeBarriers.has(work.scope)) continue;
					const hasEarlierScopeBarrier = this.queue
						.slice(0, index)
						.some((earlier) => earlier.scope === work.scope && earlier.barrier);
					if (hasEarlierScopeBarrier) continue;
					if (work.barrier) {
						const hasEarlierScopeWork = this.queue
							.slice(0, index)
							.some((earlier) => earlier.scope === work.scope);
						if (hasEarlierScopeWork || (this.activeByScope.get(work.scope) ?? 0) > 0)
							continue;
					} else if (this.activeLanes.get(work.scope)?.has(work.lane)) {
						continue;
					}
					this.start(this.removeQueued(index));
					started = true;
					break;
				}
				if (!started) break;
			}
		} finally {
			this.pumping = false;
		}
	}

	private start(work: ScheduledWorkerOperation): void {
		if (work.pool === "independent") this.activeIndependent++;
		else if (work.pool === "observer") this.activeObservers++;
		else this.activeFinite++;
		this.activeByScope.set(work.scope, (this.activeByScope.get(work.scope) ?? 0) + 1);
		if (work.barrier) this.activeBarriers.add(work.scope);
		else {
			let lanes = this.activeLanes.get(work.scope);
			if (!lanes) this.activeLanes.set(work.scope, (lanes = new Set()));
			lanes.add(work.lane);
		}

		let transactionSlotAdopted = false;
		if (work.reserveTransactionSlot && work.sessionId !== undefined)
			this.changeTransactionSlots(work.scope, work.sessionId, 1);
		const transactionSlot = work.reserveTransactionSlot
			? { adopt: () => (transactionSlotAdopted = true) }
			: undefined;
		let operation: Promise<void>;
		try {
			operation = work.run(transactionSlot);
		} catch (error) {
			try {
				work.onRejected(error instanceof Error ? error : new Error(String(error)));
			} catch {
				// A failed operation callback cannot retain its scheduler slot.
			}
			operation = Promise.resolve();
		}
		void Promise.resolve(operation).catch((error) => {
			try {
				work.onRejected(error instanceof Error ? error : new Error(String(error)));
			} catch {
				// A failed operation callback cannot retain its scheduler slot.
			}
		}).finally(() => {
			if (work.reserveTransactionSlot && !transactionSlotAdopted && work.sessionId !== undefined)
				this.changeTransactionSlots(work.scope, work.sessionId, -1);
			if (work.pool === "independent") this.activeIndependent--;
			else if (work.pool === "observer") this.activeObservers--;
			else this.activeFinite--;
			const scopeActive = (this.activeByScope.get(work.scope) ?? 1) - 1;
			if (scopeActive === 0) this.activeByScope.delete(work.scope);
			else this.activeByScope.set(work.scope, scopeActive);
			if (work.barrier) this.activeBarriers.delete(work.scope);
			else {
				const lanes = this.activeLanes.get(work.scope);
				lanes?.delete(work.lane);
				if (lanes?.size === 0) this.activeLanes.delete(work.scope);
			}
			this.pump();
			this.notifyDrained();
		});
	}

	private notifyDrained(): void {
		for (const waiter of [...this.drainWaiters]) {
			if (this.activeByScope.get(waiter.scope) !== undefined) continue;
			if (this.queue.some((work) => work.scope === waiter.scope)) continue;
			this.drainWaiters.delete(waiter);
			waiter.resolve();
		}
	}

	private hasPoolCapacity(work: ScheduledWorkerOperation): boolean {
		if (work.pool === "independent")
			return this.activeIndependent < this.limits.maxIndependentActive;
		if (work.pool === "observer")
			return this.activeObservers < this.limits.maxObserverActive;
		return this.activeFinite < this.limits.maxActive;
	}

	private queuedFor(scope: WorkerOperationScope, sessionId: number): number {
		return this.queuedByScopeSession.get(scope)?.get(sessionId) ?? 0;
	}

	private transactionSlotsFor(scope: WorkerOperationScope, sessionId: number): number {
		return this.transactionSlotsByScopeSession.get(scope)?.get(sessionId) ?? 0;
	}

	private changeQueued(scope: WorkerOperationScope, sessionId: number, delta: number): void {
		const values = this.queuedByScopeSession.get(scope) ?? new Map<number, number>();
		const next = (values.get(sessionId) ?? 0) + delta;
		if (next > 0) values.set(sessionId, next);
		else values.delete(sessionId);
		if (values.size > 0) this.queuedByScopeSession.set(scope, values);
		else this.queuedByScopeSession.delete(scope);
	}

	private changeTransactionSlots(
		scope: WorkerOperationScope,
		sessionId: number,
		delta: number,
	): void {
		const values =
			this.transactionSlotsByScopeSession.get(scope) ?? new Map<number, number>();
		const previous = values.get(sessionId) ?? 0;
		const next = Math.max(0, previous + delta);
		this.transactionSlots += next - previous;
		if (next > 0) values.set(sessionId, next);
		else values.delete(sessionId);
		if (values.size > 0) this.transactionSlotsByScopeSession.set(scope, values);
		else this.transactionSlotsByScopeSession.delete(scope);
	}
}
