import {
	deserializeWorkerError,
	serializeWorkerError,
	type WorkerInput,
	type WorkerRequest,
	type WorkerOperation,
	type WorkerResponse,
} from "./protocol.js";
import { repositoryError, OPEN_TIMEOUT_MS } from "./repository-protocol.js";
import { lostOperationError } from "./request-lifecycle.js";
import {
	WORKER_CLIENT_MAX_PENDING,
	workerQueueFullError,
	workerQueueTimeoutError,
} from "./operation-scheduler.js";
type Context = { branchId: string; accountId: string };
type Reply = Extract<WorkerResponse, { ok: boolean }>;
/** Logical handles survive owner generations; transactions and snapshot streams do not. */
export class RepositorySession {
	private epoch = 0;
	private ready = false;
	private stopped = false;
	private recovering = false;
	private nextWire = 1;
	private nextResource = 1;
	private nextCallback = 1;
	private open?: Extract<WorkerOperation, { kind: "open" }>;
	private sessions = new Map<number, { remote: number; context: Context }>();
	private observations = new Map<
		number,
		{
			remote: number;
			session: number;
			sequence: number;
			registrationRequestId: number;
			operation: Extract<WorkerOperation, { kind: "observe" }>;
		}
	>();
	private transactions = new Map<number, number>();
	private exports = new Map<number, number>();
	private callbacks = new Map<number, number>();
	private callbackIds = new Map<number, number>();
	private requests = new Map<number, WorkerRequest>();
	private cancelledObserverRequests = new Set<number>();
	private queueTimers = new Map<number, ReturnType<typeof setTimeout>>();
	private internal = new Map<
		number,
		{ resolve(reply: Reply): void; reject(error: Error): void }
	>();
	private queue: WorkerInput[] = [];
	private deadline?: ReturnType<typeof setTimeout>;
	constructor(
		private send: (message: WorkerInput) => void,
		private output: (message: WorkerResponse) => void,
		private fatal: (error: Error) => void,
	) {}
	lost() {
		this.epoch++;
		this.ready = this.recovering = false;
		const error = repositoryError(
			"LIX_OWNER_LOST",
			"Repository owner was lost",
		);
		for (const callback of this.callbackIds.values())
			this.output({ kind: "sync.fetch.cancel", requestId: callback });
		this.callbacks.clear();
		this.callbackIds.clear();
		for (const pending of this.internal.values()) pending.reject(error);
		this.internal.clear();
		this.transactions.clear();
		this.exports.clear();
		const replay: WorkerRequest[] = [];
		const closingObservations = new Set(
			Array.from(this.requests.values()).flatMap((request) =>
				request.operation.kind === "observe.close"
					? [request.operation.observeId]
					: [],
			),
		);
		for (const request of this.requests.values()) {
			const kind = request.operation.kind;
			if (
				kind === "observe" &&
				this.cancelledObserverRequests.delete(request.id)
			) {
				this.output({
					id: request.id,
					ok: false,
					error: serializeWorkerError(observerRegistrationCancelledError()),
				});
				continue;
			}
			if (kind === "observe.close") {
				this.observations.delete(request.operation.observeId);
				this.output({ id: request.id, ok: true });
				continue;
			}
			if (
				kind === "observe.next" &&
				closingObservations.has(request.operation.observeId)
			) {
				this.output({ id: request.id, ok: true, value: undefined });
				continue;
			}
			if (
				[
					"open",
					"openAnotherSession",
					"observe",
					"observe.next",
					"activeBranchId",
					"activeAccountId",
					"syncHealth",
				].includes(kind)
			) {
				if (this.queue.length + replay.length < WORKER_CLIENT_MAX_PENDING)
					replay.push(request);
				else
					this.output({
						id: request.id,
						ok: false,
						error: serializeWorkerError(workerQueueFullError()),
					});
			}
			else if (kind === "close") {
				this.sessions.delete(request.sessionId);
				this.output({ id: request.id, ok: true });
			} else
				this.output({
					id: request.id,
					ok: false,
					error: serializeWorkerError(
						lostOperationError(
							request.operation,
							kind.startsWith("transaction.")
								? repositoryError(
										"LIX_TRANSACTION_LOST",
										"Transaction owner was lost; start a new transaction",
									)
								: error,
						),
					),
				});
		}
		this.requests.clear();
		this.queue.unshift(...replay);
		for (const request of replay) {
			this.output({ kind: "request.queued", id: request.id });
			this.startQueueTimer(request);
		}
		clearTimeout(this.deadline);
		this.deadline = setTimeout(
			() =>
				this.fatal(
					repositoryError(
						"LIX_RECOVERY_TIMEOUT",
						"Repository recovery did not finish in time",
					),
				),
			OPEN_TIMEOUT_MS,
		);
	}
	connected() {
		if (this.stopped || this.ready || this.recovering) return;
		this.recovering = true;
		const epoch = this.epoch;
		void this.restore()
			.then(() => {
				if (epoch !== this.epoch || this.stopped) return;
				this.recovering = false;
				this.ready = true;
				clearTimeout(this.deadline);
				for (const message of this.queue.splice(0)) {
					if ("id" in message) this.clearQueueTimer(message.id);
					this.post(message);
				}
			})
			.catch((error) => {
				if (epoch === this.epoch && !this.stopped) this.fatal(error);
			});
	}
	private async restore() {
		if (!this.open) return;
		await this.call(this.open, 0);
		// Recreate from acknowledged context even if the original parent closed.
		for (const session of this.sessions.values()) {
			const options = this.open.server
				? { branchId: session.context.branchId }
				: session.context;
			const reply = await this.call({ kind: "openAnotherSession", options }, 0);
			if (
				!reply.context ||
				reply.context.accountId !== session.context.accountId ||
				reply.context.branchId !== session.context.branchId
			)
				throw repositoryError(
					"LIX_RECOVERY_CONTEXT_CHANGED",
					"Repository recovery changed the session branch or account",
				);
			session.remote = reply.value as number;
		}
		await this.call({ kind: "close" }, 0);
		for (const [id, observation] of this.observations) {
			const session = this.sessions.get(observation.session);
			if (session) {
				observation.remote = (
					await this.call(observation.operation, session.remote)
				).value as number;
				if (
					this.cancelledObserverRequests.delete(observation.registrationRequestId) ||
					!this.observations.has(id)
				) {
					this.observations.delete(id);
					await this.call(
						{ kind: "observe.close", observeId: observation.remote },
						0,
					);
				}
			}
		}
	}
	private call(
		operation: WorkerOperation,
		sessionId: number,
	): Promise<Extract<Reply, { ok: true }>> {
		const id = this.nextWire++;
		return new Promise((resolve, reject) => {
			this.internal.set(id, {
				resolve: (reply) =>
					reply.ok
						? resolve(reply)
						: reject(deserializeWorkerError(reply.error)),
				reject,
			});
			this.send({ id, sessionId, operation });
		});
	}
	post(message: WorkerInput) {
		if (this.stopped) return;
		if (!("id" in message)) {
			if (message.kind === "observe.cancel") {
				this.cancelObserverRegistration(message.requestId);
				return;
			}
			if ("requestId" in message) {
				const requestId = this.callbacks.get(message.requestId);
				if (requestId !== undefined) this.send({ ...message, requestId });
				if (
					message.kind === "sync.headers.result" ||
					(message.kind === "sync.fetch.result" &&
						(!message.result.ok || !message.result.response.streaming)) ||
					(message.kind === "sync.fetch.stream.result" &&
						(!message.result.ok || message.result.done))
				) {
					this.callbacks.delete(message.requestId);
					if (requestId !== undefined) this.callbackIds.delete(requestId);
				}
				return;
			}
			if (message.kind === "transaction.abandon") {
				const transactionId = this.transactions.get(message.transactionId);
				this.transactions.delete(message.transactionId);
				if (this.ready && transactionId !== undefined)
					this.send({ ...message, transactionId });
			}
			return;
		}
		if (
			this.queue.length + this.requests.size >= WORKER_CLIENT_MAX_PENDING
		) {
			this.output({
				id: message.id,
				ok: false,
				error: serializeWorkerError(workerQueueFullError()),
			});
			return;
		}
		let observationCloseLogicalId: number | undefined;
		let trackedMessage = message;
		if (message.operation.kind === "observe.close") {
			observationCloseLogicalId = message.operation.observeId;
			const observeId = message.operation.observeId;
			const observation = this.observations.get(observeId);
			if (!this.ready) {
				if (!observation) {
					this.output({ id: message.id, ok: true });
					return;
				}
				this.queue.push(message);
				this.output({ kind: "request.queued", id: message.id });
				this.startQueueTimer(message);
				return;
			}
			if (!observation) {
				this.output({ id: message.id, ok: true });
				return;
			}
			trackedMessage = message;
			message = {
				...message,
				operation: { ...message.operation, observeId: observation.remote },
			};
		}
		if (!this.ready) {
			this.queue.push(message);
			this.output({ kind: "request.queued", id: message.id });
			this.startQueueTimer(message);
			return;
		}
		try {
			const operation = { ...message.operation };
			if ("transactionId" in operation) {
				const remote = this.transactions.get(operation.transactionId);
				if (remote === undefined)
					throw repositoryError(
						"LIX_TRANSACTION_LOST",
						"Transaction belonged to a previous repository owner; start a new transaction",
					);
				operation.transactionId = remote;
			}
			if ("exportId" in operation) {
				const remote = this.exports.get(operation.exportId);
				if (remote === undefined)
					throw repositoryError(
						"LIX_SNAPSHOT_LOST",
						"Snapshot stream belonged to a previous repository owner; start a new export",
					);
				operation.exportId = remote;
			}
			if ("observeId" in operation) {
				if (operation.kind !== "observe.close") {
					const remote = this.observations.get(operation.observeId);
					if (!remote) {
						this.output({ id: message.id, ok: true, value: undefined });
						return;
					}
					operation.observeId = remote.remote;
				}
			}
			const session = this.sessions.get(message.sessionId);
			if (
				!session &&
				![
					"open",
					"replica.convert",
					"replica.cleanup",
					"hosted.create",
					"hosted.delete",
				].includes(operation.kind)
			)
				throw repositoryError(
					"LIX_ERROR_CLOSED",
					"Repository session is closed",
				);
			const sessionId = session?.remote ?? 0;
			const id = this.nextWire++;
			this.requests.set(id, trackedMessage);
			this.send({ ...message, id, sessionId, operation });
			if (observationCloseLogicalId !== undefined)
				this.observations.delete(observationCloseLogicalId);
		} catch (error) {
			this.output({
				id: message.id,
				ok: false,
				error: serializeWorkerError(error),
			});
		}
	}
	private cancelObserverRegistration(requestId: number): void {
		const queuedIndex = this.queue.findIndex(
			(message) =>
				"id" in message &&
				message.id === requestId &&
				message.operation.kind === "observe",
		);
		if (queuedIndex >= 0) {
			const [message] = this.queue.splice(queuedIndex, 1);
			if ("id" in message) {
				this.clearQueueTimer(message.id);
				this.output({
					id: message.id,
					ok: false,
					error: serializeWorkerError(observerRegistrationCancelledError()),
				});
			}
			return;
		}
		for (const [wireId, request] of this.requests) {
			if (request.id !== requestId || request.operation.kind !== "observe") continue;
			this.cancelledObserverRequests.add(requestId);
			this.send({ kind: "observe.cancel", requestId: wireId });
			return;
		}
		for (const [logicalId, observation] of this.observations) {
			if (observation.registrationRequestId !== requestId) continue;
			if (!this.ready) {
				this.cancelledObserverRequests.add(requestId);
				return;
			}
			this.observations.delete(logicalId);
			const session = this.sessions.get(observation.session);
			if (session) {
				void this.call(
					{ kind: "observe.close", observeId: observation.remote },
					0,
				).catch(() => undefined);
			}
			return;
		}
	}
	receive(message: WorkerResponse) {
		if (this.stopped) return;
		if (
			"kind" in message &&
			(message.kind === "request.started" || message.kind === "request.queued")
		) {
			if (this.internal.has(message.id)) return;
			const request = this.requests.get(message.id);
			if (request) this.output({ ...message, id: request.id });
			return;
		}
		if (!("id" in message)) {
			if ("requestId" in message) {
				let requestId = this.callbackIds.get(message.requestId);
				if (requestId === undefined) {
					requestId = this.nextCallback++;
					this.callbackIds.set(message.requestId, requestId);
					this.callbacks.set(requestId, message.requestId);
				}
				this.output({ ...message, requestId });
				if (message.kind === "sync.fetch.cancel") {
					this.callbackIds.delete(message.requestId);
					this.callbacks.delete(requestId);
				}
			} else this.output(message);
			return;
		}
		const internal = this.internal.get(message.id);
		if (internal) {
			this.internal.delete(message.id);
			internal.resolve(message);
			return;
		}
		const request = this.requests.get(message.id);
		if (!request) return;
		this.requests.delete(message.id);
		const operation = request.operation;
		if (
			operation.kind === "observe" &&
			this.cancelledObserverRequests.delete(request.id)
		) {
			if (message.ok)
				this.send({ kind: "observe.cancel", requestId: message.id });
			this.output({
				id: request.id,
				ok: false,
				error: serializeWorkerError(observerRegistrationCancelledError()),
			});
			return;
		}
		if (!message.ok) {
			this.output({ ...message, id: request.id });
			return;
		}
		let value = message.value;
		if (operation.kind === "open" || operation.kind === "openAnotherSession") {
			if (!message.context) {
				this.fatal(
					repositoryError(
						"LIX_RECOVERY_CONTEXT_MISSING",
						"Repository did not provide session context",
					),
				);
				return;
			}
			if (operation.kind === "open") {
				this.open = operation;
				this.sessions.set(0, { remote: 0, context: message.context });
			} else {
				value = this.nextResource++;
				this.sessions.set(value as number, {
					remote: message.value as number,
					context: message.context,
				});
			}
		} else if (operation.kind === "switchBranch" && message.context) {
			const session = this.sessions.get(request.sessionId);
			if (session) session.context = message.context;
		} else if (operation.kind === "observe") {
			value = this.nextResource++;
			this.observations.set(value as number, {
				remote: message.value as number,
				session: request.sessionId,
				sequence: 0,
				registrationRequestId: request.id,
				operation,
			});
		} else if (operation.kind === "observe.next") {
			const observation = this.observations.get(operation.observeId);
			if (
				observation &&
				value &&
				typeof value === "object" &&
				"sequence" in value
			)
				value = { ...value, sequence: ++observation.sequence };
			if (value == null) this.observations.delete(operation.observeId);
		} else if (operation.kind === "beginTransaction") {
			value = this.nextResource++;
			this.transactions.set(value as number, message.value as number);
		} else if (operation.kind === "exportSnapshot") {
			value = this.nextResource++;
			this.exports.set(value as number, message.value as number);
		} else if (operation.kind === "close")
			this.sessions.delete(request.sessionId);
		else if (
			operation.kind === "transaction.commit" ||
			operation.kind === "transaction.rollback"
		)
			this.transactions.delete(operation.transactionId);
		else if (operation.kind === "exportSnapshot.cancel")
			this.exports.delete(operation.exportId);
		this.output({ ...message, id: request.id, value });
	}
	close() {
		this.stopped = true;
		this.epoch++;
		clearTimeout(this.deadline);
		for (const pending of this.internal.values())
			pending.reject(repositoryError("LIX_ERROR_CLOSED", "Repository closed"));
		this.internal.clear();
		this.queue.length = 0;
		this.cancelledObserverRequests.clear();
		for (const timer of this.queueTimers.values()) clearTimeout(timer);
		this.queueTimers.clear();
	}
	private startQueueTimer(request: WorkerRequest): void {
		if (this.queueTimers.has(request.id)) return;
		const timer = setTimeout(() => {
			this.queueTimers.delete(request.id);
			const index = this.queue.findIndex(
				(message) => "id" in message && message.id === request.id,
			);
			if (index < 0) return;
			this.queue.splice(index, 1);
			this.output({
				id: request.id,
				ok: false,
				error: serializeWorkerError(workerQueueTimeoutError()),
			});
		}, OPEN_TIMEOUT_MS);
		this.queueTimers.set(request.id, timer);
	}
	private clearQueueTimer(id: number): void {
		const timer = this.queueTimers.get(id);
		if (timer !== undefined) clearTimeout(timer);
		this.queueTimers.delete(id);
	}
}

function observerRegistrationCancelledError(): Error & { code: string } {
	const error = new Error("Observer registration was cancelled") as Error & { code: string };
	error.name = "AbortError";
	error.code = "LIX_OBSERVER_CANCELLED";
	return error;
}
