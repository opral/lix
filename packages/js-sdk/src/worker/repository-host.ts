/// <reference lib="webworker" />
import { openLixBinding, convertReplicaBinding } from "#binding";
import { fetchTransport, HttpTransportError } from "../http-transport.js";
import {
	SharedAdmissionCache,
	requestAdmission,
	type AdmissionIdentity,
	sharedCredentialKey,
	sameAdmission,
} from "./shared-admission.js";
import { DurableLocalAdmission } from "./durable-local-admission.js";
import { startWorkerHost } from "./host.js";
import { SharedEngineOwner, type SharedEngineClient } from "./shared-engine.js";
import type { LixOpenReport } from "../types.js";
import type { SyncServerBindingOptions } from "../binding-types.js";
import {
	serializeWorkerError,
	type WorkerInput,
	type WorkerResponse,
	type SerializedWorkerError,
} from "./protocol.js";

export type RepositoryHostOutput = WorkerResponse | {
	kind: "repository.disconnected";
	error?: SerializedWorkerError;
};
export type RepositoryHostConnection = {
	receive(message: WorkerInput): void;
	disconnect(): Promise<void>;
};

export function createRepositoryHost() {
	let owner: SharedEngineOwner | undefined;
	let configuration: string | undefined;
	// Same-origin callers own the local store. Durable routing proofs can reopen
	// its cached data after worker shutdown, but never authorize remote requests.
	// Raw credentials remain in memory only.
	const admitted = new SharedAdmissionCache();
	let rootIdentity: AdmissionIdentity | undefined;
	let localRoot: Promise<import("../binding-types.js").LixBinding> | undefined;

	return {
		connect(send: (message: RepositoryHostOutput) => void): RepositoryHostConnection {
			let client: SharedEngineClient | undefined;
			let disconnected = false;
			let disconnecting: Promise<void> | undefined;
			let input: ((message: WorkerInput) => void) | undefined;
			const prepareClient = async (
				...args: Parameters<typeof openLixBinding>
			) => {
				const [storage, telemetry, parent, server, progress, snapshot] = args;
				if (!server || snapshot)
					throw new Error(
						"Shared partial engines require a server and existing storage",
					);
				const config = JSON.stringify([storage, server.url]);
				if (configuration !== undefined && config !== configuration)
					throw new Error("Shared engine configuration mismatch");
				configuration = config;
				const raw = server;
				const readHeaders = async () =>
					raw.headerProvider ? await raw.headerProvider() : raw.headers;
				let verifiedKey: string | undefined;
				let verifiedGeneration: number | undefined;
				let teardownCredential: {
					identity: AdmissionIdentity;
					headers: [string, string][];
				} | undefined;
				let candidateIdentity: AdmissionIdentity | undefined;
				let candidateHeaders: [string, string][] | undefined;
				let candidateOnline = false;
				let candidateGeneration = 0;
				let candidateCredentialGeneration = 0;
				let admissionReport: LixOpenReport | undefined;
				const providerOptions =
					storage.kind === "jsStorage" ? storage.options : undefined;
				const physicalScope =
					providerOptions &&
					typeof providerOptions === "object" &&
					"sharedEngineKey" in providerOptions &&
					typeof providerOptions.sharedEngineKey === "string"
						? providerOptions.sharedEngineKey
						: undefined;
				if (!physicalScope?.startsWith("lix:opfs:"))
					throw new Error("Missing physical shared storage identity");
				const localAdmission = new DurableLocalAdmission(
					physicalScope,
					raw.url,
				);
				const transport = raw.transport ?? fetchTransport();
				const authenticate = async (
					headers: [string, string][],
					allowOffline: boolean,
				) => {
					const credentialGeneration = admitted.generation(raw.url, headers);
					let result: { identity: AdmissionIdentity; online: boolean };
					try {
						result = await admitted.verify(
							raw.url,
							headers,
							rootIdentity,
							() =>
								requestAdmission(
									raw.url,
									headers,
									transport,
									allowOffline
										? {
												onProgress: progress,
												onReport: (report) => {
													admissionReport = report;
												},
											}
										: undefined,
								),
							allowOffline,
						);
					} catch (error) {
						const code = (error as { code?: string })?.code;
						if (code === "LIX_ADMISSION_AUTH_REJECTED") {
							if (
								teardownCredential &&
								sharedCredentialKey(raw.url, teardownCredential.headers) ===
									sharedCredentialKey(raw.url, headers)
							) teardownCredential = undefined;
							if (
								candidateHeaders &&
								sharedCredentialKey(raw.url, candidateHeaders) ===
									sharedCredentialKey(raw.url, headers)
							) {
								candidateOnline = false;
								candidateGeneration++;
							}
							admitted.remove(raw.url, headers);
							await localAdmission.remove(headers).catch(() => undefined);
						}
						if (!allowOffline || code !== "LIX_IDENTITY_UNVERIFIED_OFFLINE")
							throw error;
						const local = await localAdmission
							.read(headers)
							.catch(() => undefined);
						if (!local) throw error;
						if (rootIdentity && !sameAdmission(local, rootIdentity)) {
							throw new HttpTransportError(
								"LIX_SHARED_ENGINE_IDENTITY_MISMATCH",
								"Cached local repository/account does not match this owner",
							);
						}
						result = { identity: local, online: false };
					}
					if (admitted.generation(raw.url, headers) !== credentialGeneration) {
						throw new HttpTransportError(
							"LIX_ADMISSION_AUTH_REJECTED",
							"Credentials were rejected while local admission was in flight",
						);
					}
					if (result.online) {
						verifiedKey = sharedCredentialKey(raw.url, headers);
						verifiedGeneration = credentialGeneration;
						if (!rootIdentity || sameAdmission(result.identity, rootIdentity)) {
							teardownCredential = {
								identity: result.identity,
								headers: headers.map(([name, value]) => [name, value]),
							};
						}
					}
					candidateCredentialGeneration = credentialGeneration;
					candidateGeneration++;
					candidateIdentity = result.identity;
					candidateHeaders = headers.map(([name, value]) => [name, value]);
					candidateOnline = result.online;
					return result;
				};
				const routed: SyncServerBindingOptions = {
					...raw,
					teardownHeaders: async () => {
						const credential = teardownCredential;
						if (
							!credential ||
							(rootIdentity && !sameAdmission(credential.identity, rootIdentity))
						) {
							throw new HttpTransportError(
								"LIX_TRANSPORT_UNAVAILABLE",
								"No previously admitted credentials are available for session teardown",
							);
						}
						return credential.headers.map(([name, value]) => [name, value]);
					},
					transport: async (request) => {
						try {
							const response = await transport(request);
							return response;
						} catch (error) {
							// Foreground hydration routinely cancels the descriptor long poll.
							// Cancellation says nothing about the verified account identity.
							const cancelled =
								request.init.signal?.aborted ||
								(error as { code?: string })?.code ===
									"LIX_TRANSPORT_ABORTED" ||
								(error as { name?: string })?.name === "AbortError";
							if (!cancelled) verifiedKey = undefined;
							throw error;
						}
					},
					headerProvider: async () => {
						const headers = await readHeaders();
						if (
							sharedCredentialKey(raw.url, headers) !== verifiedKey ||
							admitted.generation(raw.url, headers) !== verifiedGeneration
						) {
							// Failure only suspends this remote lease; local sessions survive.
							verifiedKey = undefined;
							await authenticate(headers, false);
						}
						return headers;
					},
				};
				if (!owner) {
					owner = new SharedEngineOwner(
						async (transport, backgroundTelemetry, opener) => {
							return openLixBinding(
								storage,
								backgroundTelemetry,
								opener.parent,
								transport,
								opener.progress,
							);
						},
					);
				}
				client = {
					server: routed,
					isDisconnected: () => disconnected,
					telemetry,
					parent,
					progress,
					rejectCredentials: async (headers) => {
						const rejectedKey = sharedCredentialKey(raw.url, headers);
						if (verifiedKey === rejectedKey) verifiedKey = undefined;
						if (
							teardownCredential &&
							sharedCredentialKey(raw.url, teardownCredential.headers) === rejectedKey
						) teardownCredential = undefined;
						if (
							candidateHeaders &&
							sharedCredentialKey(raw.url, candidateHeaders) === rejectedKey
						) {
							candidateOnline = false;
							candidateGeneration++;
						}
						admitted.remove(raw.url, headers);
						await localAdmission.remove(headers).catch(() => undefined);
					},
					clearTeardownCredentials: () => {
						teardownCredential = undefined;
					},
					commitIdentity: async () => {
						if (!candidateIdentity)
							throw new Error("Missing verified owner identity");
						rootIdentity ??= candidateIdentity;
						if (candidateOnline && candidateHeaders) {
							// The owner has checked the actual stored account before this call.
							// Failure to cache only disables later offline reopening.
							const generation = candidateGeneration;
							const headers = candidateHeaders;
							const identity = candidateIdentity;
							await admitted
								.persistLocal(
									raw.url,
									headers,
									candidateCredentialGeneration,
									() => localAdmission.record(headers, identity),
									() => localAdmission.remove(headers),
								)
								.catch(() => undefined);
							// A rejection during the asynchronous write must not resurrect its proof.
							if (generation !== candidateGeneration)
								await localAdmission.remove(headers).catch(() => undefined);
						}
					},
					verifyIdentity: async () => {
						const headers = await readHeaders();
						const { identity, online } = await authenticate(headers, true);
						return {
							authorityUrl: raw.url,
							accountId: identity.principalId,
							headers,
							online,
							report: admissionReport,
						};
					},
				};
				return { owner, client };
			};
			const controller = startWorkerHost(
				{
					postMessage: (message: WorkerResponse) => send(message),
					onMessage: (listener) => {
						input = listener;
					},
				},
				async (...args) => {
					const [storage, telemetry, parent, server, progress, snapshot] = args;
					if (!server) {
						const config = JSON.stringify([storage, null]);
						if (configuration !== undefined && configuration !== config)
							throw new Error("Repository owner configuration mismatch");
						configuration = config;
						if (snapshot && localRoot)
							throw new Error(
								"Snapshot restore requires exclusive repository ownership",
							);
						const opensRoot = !localRoot;
						if (!localRoot) {
							const opening = openLixBinding(
								storage,
								telemetry,
								parent,
								undefined,
								progress,
								snapshot,
							);
							localRoot = opening;
							void opening.catch(() => {
								if (localRoot === opening) localRoot = undefined;
							});
						}
						const root = await localRoot;
						const binding = await root.openAnotherSession({}, telemetry);
						if (disconnected) {
							await binding.close();
							throw new Error("Client disconnected during open");
						}
						const report = root.openReport?.();
						return new Proxy(binding, {
							get(target, property) {
								if (property === "openReport")
									return () => {
										if (opensRoot) return report;
										if (!report) return undefined;
										return {
											format: report.format,
											initialized: false,
											migrations: [],
										};
									};
								const value = Reflect.get(target, property, target);
								return typeof value === "function" ? value.bind(target) : value;
							},
						});
					}
					const prepared = await prepareClient(...args);
					const binding = await prepared.owner.attach(prepared.client);
					if (disconnected) {
						await binding.close();
						throw new Error("Shared engine client disconnected during open");
					}
					return binding;
				},
				async (storage, server, branchId) => {
					const prepared = await prepareClient(
						storage,
						undefined,
						undefined,
						server,
					);
					await prepared.owner.convert(
						prepared.client,
						(transport) => convertReplicaBinding(storage, transport, branchId),
						branchId,
					);
				},
				true,
			);
			const disconnect = async () => {
				disconnected = true;
				let failure: unknown;
				let detachAttempted = false;
				try {
					await controller.close(async () => {
						if (client) {
							detachAttempted = true;
							await owner?.detach(client);
						}
					});
				} catch (error) {
					failure = error;
				}
				if (client && !detachAttempted) {
					try {
						await owner?.detach(client);
					} catch (error) {
						failure ??= error;
					}
				}
				try {
					send({
						kind: "repository.disconnected",
						error:
							failure === undefined ? undefined : serializeWorkerError(failure),
					});
				} finally {
					input = undefined;
				}
			};
			// The router and engine host live in the same worker. Keep their
			// call boundary direct; cross-context transport is owned by the router.
			return {
				receive(message) { input?.(message); },
				disconnect() { return disconnecting ??= disconnect(); },
			};
		},
		async close() {
			if (localRoot) await (await localRoot).close();
		},
	};
}
