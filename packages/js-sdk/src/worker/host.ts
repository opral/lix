import { CALLBACK_TIMEOUT_MS } from "./repository-protocol.js";
import { transportAbortFailure, validateHttpRequest, type HttpRequest, type HttpTransport } from "../http-transport.js";
import {
	openLixBinding,
	convertReplicaBinding,
	retryReplicaMigrationCleanupBinding,
	createHostedBinding,
	deleteHostedBinding,
} from "#binding";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
	SnapshotExportBinding,
} from "../binding-types.js";
import type {
	LixOpenProgress,
	LixOpenReport,
} from "../types.js";
import {
	deserializeWorkerError,
	serializeWorkerError,
	type WorkerHostEndpoint,
	type WorkerInput,
	type WorkerOperation,
	type WorkerRequest,
	type WorkerSyncFetchRequest,
	type WorkerSyncFetchResponse,
	type WorkerSyncServerOptions,
	isSessionCloseRequest,
} from "./protocol.js";
import {
	WorkerOperationScheduler,
	workerQueueFullError,
	type ObserverSlot,
	type TransactionSlot,
} from "./operation-scheduler.js";

export function startWorkerHost(
	endpoint: WorkerHostEndpoint,
	openBinding: typeof openLixBinding = openLixBinding,
	convertBinding: typeof convertReplicaBinding = convertReplicaBinding,
	checkpointSessions = false,
	operationScheduler: WorkerOperationScheduler = new WorkerOperationScheduler(),
): { close(afterSessionsClosed?: () => Promise<void>): Promise<void> } {
	let closed = false;
	let closing = false;
	const schedulerScope = operationScheduler.createScope();
	type ObserverAdmission = { sessionId: number; slot: ObserverSlot; active: boolean };
	type ObservationRecord = {
		binding: ObserveEventsBinding;
		sessionId: number;
		admission: ObserverAdmission;
	};
	const sessions = new Map<number, LixBinding>();
	const sessionLifetimes = new Map<number, { closing: boolean; generation: number }>();
	let nextSessionId = 1;
	let nextTransactionId = 1;
	let nextObserveId = 1;
	const transactions = new Map<
		number,
		{
			binding: LixTransactionBinding;
			sessionId: number;
			reservationActive: boolean;
			terminalQueued: boolean;
		}
	>();
	const observations = new Map<number, ObservationRecord>();
	let nextSnapshotExportId = 1;
	const snapshotExports = new Map<number, SnapshotExportBinding>();
	const snapshotInputs = new Map<
		number,
		{
			readable: ReadableStream<Uint8Array>;
			writer: WritableStreamDefaultWriter<Uint8Array>;
		}
	>();
	let nextSyncRequestId = 1;
	const pendingSyncHeaders = new Map<
		number,
		{ resolve(headers: [string, string][]): void; reject(error: unknown): void }
	>();
	const pendingSyncFetch = new Map<
		number,
		{
			resolve(response: WorkerSyncFetchResponse): void;
			reject(error: unknown): void;
		}
	>();
	const pendingSyncStreamPulls = new Map<
		number,
		{
			controller: ReadableStreamDefaultController<Uint8Array>;
			resolve(): void;
			reject(error: unknown): void;
		}
	>();
	const syncStreamCleanup = new Map<number, (failure?: unknown) => void>();
	// Fetch ownership spans the header/body handoff; waiter maps only describe
	// the currently pending callback, not the lifetime of the peer's reader.
	const activeSyncFetches = new Set<number>();
	const observationNextReads = new Set<number>();
	const observationReadsBySession = new Map<number, Set<Promise<void>>>();
	const observationReadsById = new Map<number, Set<Promise<void>>>();
	const observationClosePromises = new Map<number, Promise<void>>();
	const registrations = new Set<Promise<void>>();
	const registrationsBySession = new Map<number, Set<Promise<void>>>();
	const observationClosures = new Set<Promise<void>>();
	const observationClosuresBySession = new Map<number, Set<Promise<void>>>();
	const observerAdmissions = new Set<ObserverAdmission>();
	const observerAdmissionsBySession = new Map<number, Set<ObserverAdmission>>();
	const directOperations = new Set<Promise<void>>();

	function releaseObserverAdmission(admission: ObserverAdmission): void {
		if (!admission.active) return;
		admission.active = false;
		admission.slot.release();
		observerAdmissions.delete(admission);
		const sessionAdmissions = observerAdmissionsBySession.get(admission.sessionId);
		sessionAdmissions?.delete(admission);
		if (sessionAdmissions?.size === 0)
			observerAdmissionsBySession.delete(admission.sessionId);
	}

	function postStarted(request: WorkerRequest): void {
		endpoint.postMessage({ kind: "request.started", id: request.id });
	}

	function postQueued(request: WorkerRequest): void {
		endpoint.postMessage({ kind: "request.queued", id: request.id });
	}

	function trackDirect(request: WorkerRequest, operation: () => Promise<unknown>): Promise<void> {
		postStarted(request);
		const completion = respond(request, operation);
		directOperations.add(completion);
		void completion.finally(() => directOperations.delete(completion));
		return completion;
	}

	function scheduleLaneRequest(
		request: WorkerRequest,
		lane: string,
		sessionId: number | undefined,
		operation: () => Promise<unknown>,
		pool: "independent" | "observer" = "independent",
	): Promise<void> {
		let resolveCompletion!: () => void;
		const completion = new Promise<void>((resolve) => {
			resolveCompletion = resolve;
		});
		const rejectRequest = (error: Error) => {
			void respond(request, async () => {
				throw error;
			}).finally(resolveCompletion);
		};
		const accepted = operationScheduler.schedule({
			scope: schedulerScope,
			lane,
			pool,
			sessionId,
		onQueued: () => postQueued(request),
			run: () => {
				postStarted(request);
				return respond(request, operation).finally(resolveCompletion);
			},
			onRejected: rejectRequest,
		});
		if (!accepted) rejectRequest(workerQueueFullError());
		return completion;
	}

	function scheduleRequest(
		request: WorkerRequest,
		options: {
			lane: string;
			pool?: "finite" | "independent";
			sessionId?: number;
			barrier?: boolean;
			runOperation?: () => Promise<unknown>;
			reserveTransactionSlot?: boolean;
			useTransactionSlot?: boolean;
			queueWaitMs?: number | null;
			onTimeout?: (error: Error) => void;
			onNotAccepted?: () => void;
			onRejected?: (error: Error) => void;
		},
	): Promise<void> {
		let resolveCompletion!: () => void;
		const completion = new Promise<void>((resolve) => {
			resolveCompletion = resolve;
		});
		const finishWithError = (error: Error) => {
			void respond(request, async () => {
				throw error;
			}).finally(resolveCompletion);
		};
		const acceptedWork = {
			scope: schedulerScope,
			lane: options.lane,
			pool: options.pool,
			sessionId: options.sessionId,
			barrier: options.barrier,
			reserveTransactionSlot: options.reserveTransactionSlot,
			queueWaitMs: options.queueWaitMs,
			onQueued: () => postQueued(request),
			run: async (transactionSlot?: TransactionSlot) => {
				postStarted(request);
				const needsSession = ![
					"open",
					"observe",
					"hosted.create",
					"hosted.delete",
					"replica.convert",
					"replica.cleanup",
				].includes(request.operation.kind);
				const binding = needsSession ? requiredLix(request.sessionId) : undefined;
				return respond(request, async () => {
					if (closed) throw workerStateError("Worker client disconnected");
					binding?.setTelemetryParent(request.telemetryParent);
					try {
						const value = options.runOperation
							? await options.runOperation()
							: await handleFiniteOperation(
									request.sessionId,
									request.operation,
									request.telemetryParent,
								);
						if (request.operation.kind === "beginTransaction")
							transactionSlot?.adopt();
						return value;
					} finally {
						// Clear the exact carrier captured before this await. A child close
						// may remove its map entry while this operation is still unwinding.
						binding?.setTelemetryParent();
					}
				}).finally(resolveCompletion);
			},
			onRejected: (error: Error) => {
				options.onTimeout?.(error);
				options.onRejected?.(error);
				finishWithError(error);
			},
		};
		let accepted: boolean;
		if (options.useTransactionSlot) {
			accepted = operationScheduler.scheduleUsingTransactionSlot(acceptedWork);
		} else {
			accepted = operationScheduler.schedule(acceptedWork);
		}
		if (!accepted) {
			options.onNotAccepted?.();
			const error = workerQueueFullError();
			options.onRejected?.(error);
			finishWithError(error);
		}
		return completion;
	}

	endpoint.onMessage((message: WorkerInput) => {
		if (closed && "id" in message) return;
		if (!("id" in message)) {
			handleNotification(message);
			return;
		}
		if (
			message.operation.kind === "openSnapshot.write" ||
			message.operation.kind === "openSnapshot.finish"
		) {
			const operation = message.operation;
			void trackDirect(message, () => handleSnapshotInput(operation));
			return;
		}
		if (
			message.operation.kind === "open" &&
			message.operation.snapshotId !== undefined
		) {
			ensureSnapshotInput(message.operation.snapshotId);
		}
		if (
			message.operation.kind === "observe.next" ||
			message.operation.kind === "observe.close" ||
			message.operation.kind === "exportSnapshot.next" ||
			message.operation.kind === "exportSnapshot.cancel"
		) {
			if (message.operation.kind === "observe.next") {
				const observeId = message.operation.observeId;
				const observation = observations.get(observeId);
				if (observation && !isSessionLive(observation.sessionId)) {
					void respond(message, async () => {
						throw workerStateError("Lix session is closing");
					});
				} else if (observationNextReads.has(observeId)) {
					void respond(message, async () => {
						throw observationNextInFlightError();
					});
				} else {
					observationNextReads.add(observeId);
					const next = scheduleLaneRequest(
						message,
					`observer:${observeId}`,
						observation?.sessionId,
						() => handleObserveNext(observeId, message.telemetryParent),
						"observer",
					);
					let reads: Set<Promise<void>> | undefined;
					if (observation) {
						reads = observationReadsBySession.get(observation.sessionId);
						if (!reads) {
							reads = new Set();
							observationReadsBySession.set(observation.sessionId, reads);
						}
						reads.add(next);
						let readsForObserver = observationReadsById.get(observeId);
						if (!readsForObserver) {
							readsForObserver = new Set();
							observationReadsById.set(observeId, readsForObserver);
						}
						readsForObserver.add(next);
					}
					void next.finally(() => {
						observationNextReads.delete(observeId);
						if (!observation || !reads) return;
						reads.delete(next);
						if (reads.size === 0) observationReadsBySession.delete(observation.sessionId);
						const readsForObserver = observationReadsById.get(observeId);
						readsForObserver?.delete(next);
						if (readsForObserver?.size === 0) observationReadsById.delete(observeId);
					});
				}
			} else if (message.operation.kind === "observe.close") {
				const observeId = message.operation.observeId;
				const sessionId = observations.get(observeId)?.sessionId;
				const closure = trackDirect(message, () => handleObserveClose(observeId));
				observationClosures.add(closure);
				if (sessionId !== undefined) {
					let closures = observationClosuresBySession.get(sessionId);
					if (!closures) {
						closures = new Set();
						observationClosuresBySession.set(sessionId, closures);
					}
					closures.add(closure);
				}
				void closure.finally(() => {
					observationClosures.delete(closure);
					if (sessionId === undefined) return;
					const closures = observationClosuresBySession.get(sessionId);
					closures?.delete(closure);
					if (closures?.size === 0) observationClosuresBySession.delete(sessionId);
				});
			} else if (message.operation.kind === "exportSnapshot.next") {
				const exportId = message.operation.exportId;
				void scheduleLaneRequest(
					message,
					`snapshot:${exportId}`,
					undefined,
					() => handleSnapshotNext(exportId),
				);
			} else {
				const exportId = message.operation.exportId;
				void trackDirect(message, () => handleSnapshotCancel(exportId));
			}
			return;
		}
		if (message.operation.kind === "observe") {
			const observation = message.operation;
			const slot = operationScheduler.reserveObserverSlot();
			if (!slot) {
				const error = Object.assign(
					new Error("The worker has reached its active observer limit"),
					{ code: "LIX_WORKER_OBSERVER_LIMIT" },
				);
				void respond(message, async () => {
					throw error;
				});
				return;
			}
			const admission: ObserverAdmission = {
				sessionId: message.sessionId,
				slot,
				active: true,
			};
			observerAdmissions.add(admission);
			let sessionAdmissions = observerAdmissionsBySession.get(message.sessionId);
			if (!sessionAdmissions) {
				sessionAdmissions = new Set();
				observerAdmissionsBySession.set(message.sessionId, sessionAdmissions);
			}
			sessionAdmissions.add(admission);
			const registration = scheduleRequest(message, {
				lane: `observe:${message.sessionId}`,
				pool: "independent",
				sessionId: message.sessionId,
				onRejected: () => releaseObserverAdmission(admission),
				onNotAccepted: () => releaseObserverAdmission(admission),
				runOperation: () =>
					handleObserveRegistration(
						message.sessionId,
						observation.sql,
						observation.params,
						admission,
					),
			});
			registrations.add(registration);
			let sessionRegistrations = registrationsBySession.get(message.sessionId);
			if (!sessionRegistrations) {
				sessionRegistrations = new Set();
				registrationsBySession.set(message.sessionId, sessionRegistrations);
			}
			sessionRegistrations.add(registration);
			void registration.finally(() => registrations.delete(registration));
			void registration.finally(() => {
				const values = registrationsBySession.get(message.sessionId);
				values?.delete(registration);
				if (values?.size === 0) registrationsBySession.delete(message.sessionId);
			});
			return;
		}
		const operation = message.operation;
		const transactionId =
			"transactionId" in operation ? operation.transactionId : undefined;
		let transaction:
			| ReturnType<typeof requiredTransaction>
			| undefined;
		try {
			transaction = transactionId === undefined
				? undefined
				: requiredTransaction(
						transactionId,
						message.sessionId,
					);
		} catch (error) {
			void respond(message, async () => {
				throw error;
			});
			return;
		}
		const terminal =
			operation.kind === "transaction.commit" ||
			operation.kind === "transaction.rollback";
		const lane =
			operation.kind === "hosted.create" || operation.kind === "hosted.delete"
				? "hosted:mutation"
				: `session:${message.sessionId}`;
		const options = {
			lane,
			sessionId:
				operation.kind === "replica.convert" ||
				operation.kind === "replica.cleanup" ||
				operation.kind === "hosted.create" ||
				operation.kind === "hosted.delete"
					? undefined
					: message.sessionId,
			barrier:
				operation.kind === "open" ||
				operation.kind === "replica.convert" ||
				operation.kind === "replica.cleanup",
			onRejected:
				operation.kind === "open" && operation.snapshotId !== undefined
					? (error: Error) => closeSnapshotInput(operation.snapshotId!, error)
					: undefined,
		};
		if (terminal && transaction) {
			transaction.terminalQueued = true;
			transaction.reservationActive = false;
		}
	void scheduleRequest(message, {
			...options,
			reserveTransactionSlot: operation.kind === "beginTransaction",
			useTransactionSlot: terminal,
				onTimeout:
				terminal && transaction
					? (error) => {
						if ((error as Error & { code?: string }).code !== "LIX_WORKER_QUEUE_TIMEOUT" || closing) return;
						transaction.terminalQueued = false;
						transaction.reservationActive = true;
						operationScheduler.restoreTransactionSlot(
							schedulerScope,
							transaction.sessionId,
						);
					}
					: undefined,
			onNotAccepted:
				terminal && transaction
					? () => {
						transaction.terminalQueued = false;
						transaction.reservationActive = true;
					}
					: undefined,
		});
	});

	function handleNotification(
		message: Exclude<WorkerInput, WorkerRequest>,
	): void {
			switch (message.kind) {
			case "openSnapshot.cancel": {
				closeSnapshotInput(message.snapshotId);
				break;
			}
			case "transaction.abandon": {
				const transaction = transactions.get(message.transactionId);
				if (!transaction || transaction.terminalQueued) break;
				transaction.terminalQueued = true;
				const hadReservation = transaction.reservationActive;
				transaction.reservationActive = false;
				const work = {
					scope: schedulerScope,
					lane: `session:${transaction.sessionId}`,
					sessionId: transaction.sessionId,
					queueWaitMs: null,
					run: async () => {
						transactions.delete(message.transactionId);
						await transaction.binding.rollback().catch(() => undefined);
					},
					onRejected: () => {
						if (!hadReservation || closing) return;
						transaction.terminalQueued = false;
						transaction.reservationActive = true;
						operationScheduler.restoreTransactionSlot(
							schedulerScope,
							transaction.sessionId,
						);
					},
				};
				const accepted = hadReservation
					? operationScheduler.scheduleUsingTransactionSlot(work)
					: operationScheduler.schedule(work);
				if (!accepted) {
					transaction.terminalQueued = false;
					transaction.reservationActive = hadReservation;
				}
				break;
			}
			case "sync.headers.result": {
				const pending = pendingSyncHeaders.get(message.requestId);
				pendingSyncHeaders.delete(message.requestId);
				if (!pending) break;
				if (message.result.ok) pending.resolve(message.result.headers);
				else pending.reject(deserializeWorkerError(message.result.error));
				break;
			}
			case "sync.fetch.result": {
				const pending = pendingSyncFetch.get(message.requestId);
				pendingSyncFetch.delete(message.requestId);
				if (!pending) break;
				if (message.result.ok) pending.resolve(message.result.response);
				else pending.reject(deserializeWorkerError(message.result.error));
				break;
			}
			case "sync.fetch.stream.result": {
				const pending = pendingSyncStreamPulls.get(message.requestId);
				pendingSyncStreamPulls.delete(message.requestId);
				if (!pending) break;
				if (!message.result.ok) {
					const error = deserializeWorkerError(message.result.error);
					pending.controller.error(error);
					pending.reject(error);
					finishSyncStream(message.requestId);
				} else if (message.result.done) {
					pending.controller.close();
					pending.resolve();
					finishSyncStream(message.requestId);
				} else {
					pending.controller.enqueue(message.result.chunk);
					pending.resolve();
				}
				break;
			}
		}
	}

	function finishSyncStream(requestId: number, failure?: unknown): void {
		activeSyncFetches.delete(requestId);
		const cleanup = syncStreamCleanup.get(requestId);
		syncStreamCleanup.delete(requestId);
		cleanup?.(failure);
	}

	function cancelSyncFetch(requestId: number, failure?: unknown): void {
		if (activeSyncFetches.delete(requestId)) {
			try { endpoint.postMessage({ kind: "sync.fetch.cancel", requestId }); }
			catch { /* Local retirement must finish after peer disconnection. */ }
		}
		finishSyncStream(requestId, failure);
	}

	async function respond(
		request: WorkerRequest,
		operation: () => Promise<unknown>,
	): Promise<void> {
		try {
			const value = await operation();
			if (request.operation.kind === "observe") {
				const registered = observations.get(value as number);
				if (
					!isSessionLive(request.sessionId) ||
					registered?.sessionId !== request.sessionId
				) {
					throw workerStateError("Lix session closed during observer registration");
				}
			}
			const kind = request.operation.kind;
			const checkpoint =
				checkpointSessions &&
				(kind === "open" ||
					kind === "openAnotherSession" ||
					kind === "switchBranch");
			const session = checkpoint
				? sessions.get(
						kind === "openAnotherSession"
							? (value as number)
							: request.sessionId,
					)
				: undefined;
			const context = session
				? {
						branchId: await session.activeBranchId(),
						accountId: await session.activeAccountId(),
					}
				: undefined;
			endpoint.postMessage({
				id: request.id,
				ok: true,
				value,
				...(context ? { context } : {}),
			});
		} catch (error) {
			endpoint.postMessage({
				id: request.id,
				ok: false,
				error: serializeWorkerError(error),
			});
		}
	}

	async function handleFiniteOperation(
		sessionId: number,
		operation: WorkerOperation,
		telemetryParent: WorkerRequest["telemetryParent"],
	): Promise<unknown> {
		switch (operation.kind) {
            case "replica.cleanup":
                if (operationScheduler.totalOpenSessions() > 0) throw workerStateError("Migration cleanup requires closed storage");
                return retryReplicaMigrationCleanupBinding(operation.storage,createSyncServerBridge(operation.server)!);
            case "replica.convert":
                if (operationScheduler.totalOpenSessions() > 0) throw workerStateError("Conversion requires closed storage");
                return convertBinding(operation.storage,createSyncServerBridge(operation.server)!,operation.branchId);
			case "hosted.create":
				return createHostedBinding(operation.server);
			case "hosted.delete":
				return deleteHostedBinding(operation.server);
			case "hosted.createFrom": {
				const binding = requiredLix(sessionId);
				if (!binding.createHosted)
					throw new TypeError("createLix() from requires a local Lix");
				return binding.createHosted(operation.server);
			}
			case "open":
				if (sessions.size > 0)
					throw workerStateError("Lix worker is already open");
				{
					const snapshot =
						operation.snapshotId === undefined
							? undefined
							: requiredSnapshotInput(operation.snapshotId).readable;
					try {
						const opened = await openBinding(
							operation.storage,
							operation.telemetryEnabled
								? (request: Uint8Array) =>
										endpoint.postMessage({ kind: "telemetry", request })
								: undefined,
							telemetryParent,
							createSyncServerBridge(operation.server),
							operation.progressEnabled
								? (progress: LixOpenProgress) =>
										endpoint.postMessage({ kind: "open.progress", progress })
								: undefined,
							snapshot,
						);
						sessions.set(0, opened);
						sessionLifetimes.set(0, { closing: false, generation: 0 });
						operationScheduler.setSessionCount(schedulerScope, sessions.size);
						return opened.openReport?.() satisfies LixOpenReport | undefined;
					} finally {
						if (operation.snapshotId !== undefined) {
							closeSnapshotInput(operation.snapshotId);
						}
					}
				}
			case "openSnapshot.write":
			case "openSnapshot.finish":
				throw workerStateError("snapshot input uses the restore lane");
			case "openAnotherSession": {
				const opened = await requiredLix(sessionId).openAnotherSession(
					operation.options,
				);
				const openedSessionId = nextSessionId++;
				sessions.set(openedSessionId, opened);
				sessionLifetimes.set(openedSessionId, { closing: false, generation: 0 });
				operationScheduler.setSessionCount(schedulerScope, sessions.size);
				return openedSessionId;
			}
			case "execute":
				return requiredLix(sessionId).execute(
					operation.sql,
					operation.params,
					operation.options,
				);
			case "executeBatch":
				return requiredLix(sessionId).executeBatch(
					operation.statements,
					operation.options,
				);
			case "beginTransaction": {
				const binding = await requiredLix(sessionId).beginTransaction();
				const transactionId = nextTransactionId++;
				transactions.set(transactionId, {
					binding,
					sessionId,
					reservationActive: true,
					terminalQueued: false,
				});
				return transactionId;
			}
			case "transaction.execute":
				return requiredTransaction(operation.transactionId, sessionId).binding.execute(
					operation.sql,
					operation.params,
					operation.options,
				);
			case "transaction.commit": {
				const transaction = requiredTransaction(operation.transactionId, sessionId, true);
				transactions.delete(operation.transactionId);
				try {
					return await transaction.binding.commit();
				} finally {
					if (transaction.reservationActive)
						operationScheduler.releaseTransactionSlot(
							schedulerScope,
							transaction.sessionId,
						);
				}
			}
			case "transaction.rollback": {
				const transaction = requiredTransaction(operation.transactionId, sessionId, true);
				transactions.delete(operation.transactionId);
				try {
					await transaction.binding.rollback();
				} finally {
					if (transaction.reservationActive)
						operationScheduler.releaseTransactionSlot(
							schedulerScope,
							transaction.sessionId,
						);
				}
				return undefined;
			}
			case "replicaRecoverySources":
				return requiredLix(sessionId).replicaRecoverySources();
			case "exportReplicaRecovery":
				return requiredLix(sessionId).exportReplicaRecovery(operation.id);
			case "recoverReplica":
                return requiredLix(sessionId).recoverReplica(operation.id);
            case "recoverReplicaWithServer":
                return requiredLix(sessionId).recoverReplicaWithServer(operation.id, createSyncServerBridge(operation.server, operation.transportScope)!);
			case "syncHealth":
				return requiredLix(sessionId).syncHealth();
			case "prepareOfflineEditing":
				{
					const binding = requiredLix(sessionId);
					if (!binding.prepareOfflineEditing) {
						const error = workerStateError(
							"offline editing preparation is unavailable for this Lix binding",
						);
						error.code = "LIX_SYNC_MODE_MISMATCH";
						throw error;
					}
					return binding.prepareOfflineEditing();
				}
			case "activeBranchId":
				return requiredLix(sessionId).activeBranchId();
			case "activeAccountId":
				return requiredLix(sessionId).activeAccountId();
			case "createBranch":
				return requiredLix(sessionId).createBranch(operation.options);
			case "switchBranch":
				return requiredLix(sessionId).switchBranch(operation.options);
			case "mergeBranchPreview":
				return requiredLix(sessionId).mergeBranchPreview(operation.options);
			case "mergeBranch":
				return requiredLix(sessionId).mergeBranch(operation.options);
			case "importFilesystemPaths":
				return requiredLix(sessionId).importFilesystemPaths(operation.paths);
			case "syncDiskToLix":
				return requiredLix(sessionId).syncDiskToLix();
			case "exportSnapshot": {
				const binding = requiredLix(sessionId);
				const exportSnapshot = binding.exportSnapshot;
				if (!exportSnapshot) {
					throw workerStateError("this Lix binding cannot export snapshots");
				}
				const snapshot = exportSnapshot.call(binding);
				const exportId = nextSnapshotExportId++;
				snapshotExports.set(exportId, snapshot);
				return exportId;
			}
			case "exportSnapshot.next":
				throw workerStateError(
					"snapshot pulls bypass the finite operation queue",
				);
			case "exportSnapshot.cancel":
				throw workerStateError(
					"snapshot cancellation bypasses the finite operation queue",
				);
			case "observe":
				throw workerStateError("observe must use the observation lane");
			case "close": {
				const openLix = requiredLix(sessionId);
				const lifetime = requiredSessionLifetime(sessionId);
				lifetime.closing = true;
				lifetime.generation++;
				try {
					for (const [transactionId, transaction] of transactions) {
						if (transaction.sessionId !== sessionId) continue;
						transactions.delete(transactionId);
						await transaction.binding.rollback().catch(() => undefined);
						if (transaction.reservationActive)
							operationScheduler.releaseTransactionSlot(
								schedulerScope,
								transaction.sessionId,
							);
					}
					await closeObservationsForSession(sessionId);
					await openLix.close();
					sessions.delete(sessionId);
					sessionLifetimes.delete(sessionId);
					operationScheduler.setSessionCount(schedulerScope, sessions.size);
					return undefined;
				} catch (error) {
					if (sessions.get(sessionId) === openLix) lifetime.closing = false;
					throw error;
				}
			}
			case "observe.next":
				throw workerStateError("observe.next must use the observation lane");
			case "observe.close":
				throw workerStateError("observe.close must use the observation lane");
		}
	}

	async function handleSnapshotNext(
		exportId: number,
	): Promise<Uint8Array | undefined> {
		const snapshot = snapshotExports.get(exportId);
		if (!snapshot) return undefined;
		try {
			const chunk = await snapshot.next();
			if (chunk == null) snapshotExports.delete(exportId);
			return chunk ?? undefined;
		} catch (error) {
			snapshotExports.delete(exportId);
			await Promise.resolve(snapshot.cancel()).catch(() => undefined);
			throw error;
		}
	}

	async function handleSnapshotCancel(exportId: number): Promise<void> {
		const snapshot = snapshotExports.get(exportId);
		snapshotExports.delete(exportId);
		if (snapshot) await snapshot.cancel();
	}

	function ensureSnapshotInput(snapshotId: number): void {
		if (snapshotInputs.has(snapshotId)) return;
		const stream = new TransformStream<Uint8Array, Uint8Array>(
			undefined,
			{ highWaterMark: 0 },
			{ highWaterMark: 0 },
		);
		snapshotInputs.set(snapshotId, {
			readable: stream.readable,
			writer: stream.writable.getWriter(),
		});
	}

	function requiredSnapshotInput(snapshotId: number) {
		const input = snapshotInputs.get(snapshotId);
		if (!input) throw workerStateError("snapshot restore input is closed");
		return input;
	}

	function closeSnapshotInput(snapshotId: number, reason?: unknown): void {
		const input = snapshotInputs.get(snapshotId);
		if (!input) return;
		snapshotInputs.delete(snapshotId);
		// A queued open has no reader yet. Cancel that side to release any
		// backpressured TransformStream writes; if restore already locked it,
		// cancellation rejects and aborting the writer still signals the reader.
		void input.readable.cancel(reason).catch(() => undefined);
		void input.writer.abort(reason).catch(() => undefined);
	}

	async function handleSnapshotInput(
		operation: Extract<
			WorkerOperation,
			{ kind: "openSnapshot.write" | "openSnapshot.finish" }
		>,
	): Promise<void> {
		const input = requiredSnapshotInput(operation.snapshotId);
		if (operation.kind === "openSnapshot.write") {
			await input.writer.write(operation.chunk);
			return;
		}
		await input.writer.close();
	}

	return { async close(afterSessionsClosed) {
        if (closed) return;
        closed = true;
        closing = true;
        let closeFailure: unknown;
        let closeFailed = false;
        const capture = async (action: () => void | Promise<void>) => {
          try { await action(); }
          catch (error) {
            if (!closeFailed) closeFailure = error;
            closeFailed = true;
          }
        };
        const captureSync = (action: () => void) => {
          try { action(); }
          catch (error) {
            if (!closeFailed) closeFailure = error;
            closeFailed = true;
          }
        };
        try {
          const failure = workerStateError("Worker client disconnected");
          const fetchFailure = transportAbortFailure();
          operationScheduler.cancelQueued(schedulerScope, failure);
          for (const requestId of Array.from(activeSyncFetches)) {
            // The peer may still own a fetch/reader even with no pull pending.
            // A disconnected channel cannot receive cancellation; local close
            // must still retire all streams and database handles in that case.
            captureSync(() => cancelSyncFetch(requestId, fetchFailure));
          }
          for (const pending of pendingSyncHeaders.values())
            captureSync(() => pending.reject(failure));
          pendingSyncHeaders.clear();
          for (const pending of pendingSyncFetch.values())
            captureSync(() => pending.reject(fetchFailure));
          pendingSyncFetch.clear();
          for (const cleanup of syncStreamCleanup.values()) captureSync(() => cleanup(fetchFailure));
          syncStreamCleanup.clear();
          for (const pending of pendingSyncStreamPulls.values()) {
            captureSync(() => pending.controller.error(fetchFailure));
            captureSync(() => pending.reject(fetchFailure));
          }
          pendingSyncStreamPulls.clear();
		  for (const snapshotId of [...snapshotInputs.keys()])
			captureSync(() => closeSnapshotInput(snapshotId, failure));
		  for (const lifetime of sessionLifetimes.values()) {
		    lifetime.closing = true;
		    lifetime.generation++;
		  }
		  await capture(async () => {
		    await Promise.allSettled(
		      [...sessions.keys()].map((sessionId) => closeObservationsForSession(sessionId)),
		    );
		  });
		  await Promise.allSettled([...observationClosures]);
          for (const snapshot of snapshotExports.values()) {
            await capture(() => Promise.resolve().then(() => snapshot.cancel()));
          }
          snapshotExports.clear();
          await capture(() => operationScheduler.drainScope(schedulerScope));
		  await Promise.allSettled([
		    ...registrations,
            ...observationClosures,
            ...directOperations,
		  ]);
		  for (const admission of observerAdmissions) releaseObserverAdmission(admission);
          // The active finite operation has finished; never roll back a handle
          // concurrently with its execute/commit operation.
          for (const transaction of transactions.values()) {
            await transaction.binding.rollback().catch(() => undefined);
            if (transaction.reservationActive)
              operationScheduler.releaseTransactionSlot(
                schedulerScope,
                transaction.sessionId,
              );
          }
          transactions.clear();
          for (const snapshot of snapshotExports.values()) {
            await capture(() => Promise.resolve().then(() => snapshot.cancel()));
          }
          snapshotExports.clear();
          for (const session of sessions.values()) await capture(() => session.close());
          sessions.clear();
		  sessionLifetimes.clear();
		  registrationsBySession.clear();
		  observationClosuresBySession.clear();
		  observationReadsBySession.clear();
		  observationReadsById.clear();
		  observationClosePromises.clear();
		  observerAdmissionsBySession.clear();
          operationScheduler.setSessionCount(schedulerScope, 0);
        } catch (error) {
          if (!closeFailed) closeFailure = error;
          closeFailed = true;
        } finally {
          // The owner detaches after local sessions have closed. Keep this
          // narrow lane available through that final remote DELETE, and always
          // retire it even if any local cleanup step failed unexpectedly.
          await capture(() => afterSessionsClosed?.());
          closing = false;
          operationScheduler.releaseScope(schedulerScope);
        }
        if (closeFailed) throw closeFailure;
    } };

	function createSyncServerBridge(
		server: WorkerSyncServerOptions | undefined,
		transportScope?: number,
	):
		| {
				url: string;
				headers: [string, string][];
				headerProvider?: () => Promise<[string, string][]>;
				transport?: HttpTransport;
		  }
		| undefined {
		if (!server) return undefined;
		return {
			url: server.url,
			headers: server.headers ?? [],
			headerProvider: server.dynamicHeaders
				? () => {
						if (closed && !closing) throw workerStateError("Worker client disconnected");
						const requestId = nextSyncRequestId++;
						return new Promise((resolve, reject) => {
							const timer = setTimeout(() => {
								pendingSyncHeaders.delete(requestId);
								reject(
									Object.assign(
										new Error("Credential callback did not settle"),
										{
											code: "LIX_CREDENTIALS_TIMEOUT",
										},
									),
								);
							}, CALLBACK_TIMEOUT_MS);
							pendingSyncHeaders.set(requestId, {
								resolve: (headers) => {
									clearTimeout(timer);
									resolve(headers);
								},
								reject: (error) => {
									clearTimeout(timer);
									reject(error);
								},
							});
							endpoint.postMessage({ kind: "sync.headers", requestId, transportScope });
						});
					}
				: undefined,
			transport: (request) => bridgeFetch(request, transportScope, server.url),
		};
	}

	async function bridgeFetch(
        httpRequest: HttpRequest,
        transportScope?: number,
        authorityUrl?: string,
    ): Promise<Response> {
        validateHttpRequest(httpRequest);
        const { url: input, init, response: policy } = httpRequest;
        const streaming = policy.mode === "streaming";
        const closingSession = closing && isSessionCloseRequest({ url: input, method: init?.method, headers: init?.headers }, authorityUrl);
		if (closed && !closingSession) throw workerStateError("Worker client disconnected");
                    const requestId = nextSyncRequestId++;
		const requestBase = {
            url: input,
			method: init?.method ?? "GET",
			headers: headerEntries(init?.headers),
			body: serializableBody(init?.body),
			credentials: init?.credentials,
            cache: init?.cache, redirect: init?.redirect,
		};
        const request: WorkerSyncFetchRequest = {...requestBase, response: policy};
		activeSyncFetches.add(requestId);
		const response = new Promise<WorkerSyncFetchResponse>((resolve, reject) => {
			pendingSyncFetch.set(requestId, { resolve, reject });
			endpoint.postMessage({ kind: "sync.fetch", requestId, request, transportScope });
		});
		let responseController: ReadableStreamDefaultController<Uint8Array> | undefined;
		const abort = () => {
			const error = transportAbortFailure(init?.signal);
			const pending = pendingSyncFetch.get(requestId);
			pendingSyncFetch.delete(requestId);
			if (pending) {
				pending.reject(error);
			}
			const pull = pendingSyncStreamPulls.get(requestId);
			pendingSyncStreamPulls.delete(requestId);
			// Cancellation belongs to the response, including when backpressure
			// means there is no active pull RPC to reject.
			responseController?.error(error);
			if (pull) {
				pull.reject(error);
			}
			cancelSyncFetch(requestId, error);
		};
		if (init?.signal?.aborted) abort();
		else init?.signal?.addEventListener("abort", abort, { once: true });
		let streamEstablished = false;
		try {
			const resolved = await response;
			if (closed && !closingSession) throw transportAbortFailure();
			if (resolved.streaming) {
				const signal = init?.signal;
				if (signal?.aborted) {
					abort();
					throw transportAbortFailure(signal);
				}
				if (
					resolved.status === 204 ||
					resolved.status === 205 ||
					resolved.status === 304
				) {
					cancelSyncFetch(requestId);
					return new Response(null, {
						status: resolved.status,
						statusText: resolved.statusText,
						headers: resolved.headers,
					});
				}
				syncStreamCleanup.set(requestId, (failure) => {
					signal?.removeEventListener("abort", abort);
					if (failure !== undefined) responseController?.error(failure);
				});
				const body = new ReadableStream<Uint8Array>({
					start: (controller) => { responseController = controller; },
					pull: (controller) =>
						new Promise<void>((resolve, reject) => {
							pendingSyncStreamPulls.set(requestId, {
								controller,
								resolve,
								reject,
							});
							endpoint.postMessage({
								kind: "sync.fetch.stream.pull",
								requestId,
							});
						}),
					cancel: abort,
				});
				const streamedResponse = new Response(body, {
					status: resolved.status,
					statusText: resolved.statusText,
					headers: resolved.headers,
				});
				streamEstablished = true;
				return streamedResponse;
			}
			return responseFromSyncFetch(resolved);
		} catch (error) {
			if (streaming && !streamEstablished) {
				cancelSyncFetch(requestId, error);
			}
			throw error;
		} finally {
			pendingSyncFetch.delete(requestId);
			if (!streamEstablished) {
				activeSyncFetches.delete(requestId);
				init?.signal?.removeEventListener("abort", abort);
			}
		}
	}

	async function handleObserveNext(
		observeId: number,
		telemetryParent: WorkerRequest["telemetryParent"],
	): Promise<unknown> {
		const events = observations.get(observeId);
		if (!events) return undefined;
		events.binding.setTelemetryParent(telemetryParent);
		return events.binding.next();
	}

	async function handleObserveClose(observeId: number): Promise<void> {
		const events = observations.get(observeId);
		observations.delete(observeId);
		return closeObservation(observeId, events);
	}

	function closeObservation(
		observeId: number,
		events: ObservationRecord | undefined,
	): Promise<void> {
		const existing = observationClosePromises.get(observeId);
		if (existing) return existing;
		let closeFailure: unknown;
		let closeFailed = false;
		const closing = Promise.resolve().then(async () => {
			try {
				try {
					await events?.binding.close();
				} catch (error) {
					closeFailure = error;
					closeFailed = true;
				}
				await Promise.allSettled([...(observationReadsById.get(observeId) ?? [])]);
				if (closeFailed) throw closeFailure;
			} finally {
				events && releaseObserverAdmission(events.admission);
				observationReadsById.delete(observeId);
			}
		});
		observationClosePromises.set(observeId, closing);
		const forget = () => {
			if (observationClosePromises.get(observeId) === closing)
				observationClosePromises.delete(observeId);
		};
		void closing.then(forget, forget);
		return closing;
	}

	async function closeObservationsForSession(sessionId: number): Promise<void> {
		const pending: Promise<void>[] = [];
		operationScheduler.cancelQueued(
			schedulerScope,
			workerStateError("Lix session is closing"),
			(work) => work.lane === `observe:${sessionId}`,
		);
		for (const [observeId, events] of observations) {
			if (events.sessionId !== sessionId) continue;
			observations.delete(observeId);
			pending.push(closeObservation(observeId, events));
		}
		pending.push(...(observationClosuresBySession.get(sessionId) ?? []));
		pending.push(...(observationReadsBySession.get(sessionId) ?? []));
		pending.push(...(registrationsBySession.get(sessionId) ?? []));
		await Promise.allSettled(pending);
		for (const admission of observerAdmissionsBySession.get(sessionId) ?? [])
			releaseObserverAdmission(admission);
	}

	async function handleObserveRegistration(
		sessionId: number,
		sql: string,
		params: Parameters<LixBinding["observe"]>[1],
		admission: ObserverAdmission,
	): Promise<number> {
		// Do not touch the mutable Lix telemetry carrier here: it belongs to the
		// serialized finite lane. Each `observe.next` supplies telemetry directly
		// to its observation binding.
		const lifetime = requiredSessionLifetime(sessionId);
		const generation = lifetime.generation;
		let adopted = false;
		try {
			const events = await requiredLix(sessionId).observe(sql, params);
			if (
				closed ||
				lifetime.closing ||
				lifetime.generation !== generation ||
				sessionLifetimes.get(sessionId) !== lifetime
			) {
				await Promise.resolve(events.close()).catch(() => undefined);
				throw workerStateError("Lix session closed during observer registration");
			}
			const observeId = nextObserveId++;
			observations.set(observeId, { binding: events, sessionId, admission });
			adopted = true;
			return observeId;
		} finally {
			if (!adopted) releaseObserverAdmission(admission);
		}
	}

	function requiredLix(sessionId: number): LixBinding {
		const lix = sessions.get(sessionId);
		if (!lix || !isSessionLive(sessionId)) throw workerStateError("Lix session is closed");
		return lix;
	}

	function requiredSessionLifetime(
		sessionId: number,
	): { closing: boolean; generation: number } {
		const lifetime = sessionLifetimes.get(sessionId);
		if (!lifetime || lifetime.closing || !sessions.has(sessionId))
			throw workerStateError("Lix session is closed");
		return lifetime;
	}

	function isSessionLive(sessionId: number): boolean {
		return sessions.has(sessionId) && sessionLifetimes.get(sessionId)?.closing === false;
	}

	function requiredTransaction(
		transactionId: number,
		sessionId: number,
		allowTerminal = false,
	) {
		const transaction = transactions.get(transactionId);
		if (!transaction) {
			const error = workerStateError("Lix transaction is closed");
			error.code = "LIX_INVALID_TRANSACTION_STATE";
			throw error;
		}
		if (transaction.sessionId !== sessionId) {
			const error = workerStateError("Lix transaction belongs to another session");
			error.code = "LIX_TRANSACTION_OWNER_MISMATCH";
			throw error;
		}
		if (transaction.terminalQueued && !allowTerminal) {
			const error = workerStateError("Lix transaction is completing");
			error.code = "LIX_INVALID_TRANSACTION_STATE";
			throw error;
		}
		return transaction;
	}
}

export function responseFromSyncFetch(
	resolved: WorkerSyncFetchResponse,
): Response {
	if (resolved.streaming) {
		throw new TypeError(
			"Streaming sync responses require the worker stream bridge",
		);
	}
	const body =
		resolved.status === 204 ||
		resolved.status === 205 ||
		resolved.status === 304
			? null
			: resolved.body.slice().buffer;
	return new Response(body, {
		status: resolved.status,
		statusText: resolved.statusText,
		headers: resolved.headers,
	});
}

function workerStateError(message: string): Error & { code?: string } {
	const error = new Error(message) as Error & { code?: string };
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}

function observationNextInFlightError(): Error & { code?: string } {
	const error = workerStateError("An observation next call is already in flight");
	error.code = "LIX_OBSERVE_NEXT_IN_FLIGHT";
	return error;
}

function headerEntries(headers: HeadersInit | undefined): [string, string][] {
	const entries: [string, string][] = [];
	new Headers(headers).forEach((value, name) => entries.push([name, value]));
	return entries;
}

function serializableBody(
	body: BodyInit | null | undefined,
): string | Uint8Array | undefined {
	if (body === undefined || body === null) return undefined;
	if (typeof body === "string") return body;
	if (body instanceof Uint8Array) return body;
	if (body instanceof ArrayBuffer) return new Uint8Array(body);
	if (ArrayBuffer.isView(body)) {
		return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
	}
	throw new TypeError("Browser sync fetch body is not structured-cloneable");
}
