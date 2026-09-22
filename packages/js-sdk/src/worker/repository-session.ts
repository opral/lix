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
type Context = { branchId: string; accountId: string };
type Reply = Extract<WorkerResponse, { id: number }>;
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
			operation: Extract<WorkerOperation, { kind: "observe" }>;
		}
	>();
	private transactions = new Map<number, number>();
	private exports = new Map<number, number>();
	private callbacks = new Map<number, number>();
	private callbackIds = new Map<number, number>();
	private requests = new Map<number, WorkerRequest>();
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
		const replay: WorkerInput[] = [];
		const closingObservations = new Set(
			Array.from(this.requests.values()).flatMap((request) =>
				request.operation.kind === "observe.close"
					? [request.operation.observeId]
					: [],
			),
		);
		for (const request of this.requests.values()) {
			const kind = request.operation.kind;
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
			)
				replay.push(request);
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
				for (const message of this.queue.splice(0)) this.post(message);
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
				if (!this.observations.has(id))
					await this.call(
						{ kind: "observe.close", observeId: observation.remote },
						0,
					);
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
	receive(message: WorkerResponse) {
		if (this.stopped) return;
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
	}
}
