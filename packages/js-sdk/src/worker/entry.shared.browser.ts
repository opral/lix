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
import { serializeWorkerError, type WorkerInput, type WorkerResponse } from "./protocol.js";

const scope = globalThis as unknown as SharedWorkerGlobalScope;
let owner: SharedEngineOwner | undefined;
let configuration: string | undefined;
// Same-origin callers own the local store. Durable routing proofs can reopen
// its cached data after worker shutdown, but never authorize remote requests.
// Raw credentials remain in memory only.
const admitted = new SharedAdmissionCache();
let rootIdentity: AdmissionIdentity | undefined;

scope.onconnect = (event) => {
  const port = event.ports[0]!;
  let client: SharedEngineClient | undefined;
  let disconnected = false;
  let input: ((message: WorkerInput) => void) | undefined;
  const prepareClient = async (...args: Parameters<typeof openLixBinding>) => {
    const [storage, telemetry, parent, server, progress, snapshot] = args;
      if (!server || snapshot)
        throw new Error("Shared partial engines require a server and existing storage");
      const config = JSON.stringify([storage, server.url]);
      if (configuration !== undefined && config !== configuration)
        throw new Error("Shared engine configuration mismatch");
      configuration = config;
      const raw = server;
      const readHeaders = async () =>
        raw.headerProvider ? await raw.headerProvider() : raw.headers;
      let verifiedKey: string | undefined;
      let verifiedGeneration: number | undefined;
      let candidateIdentity: AdmissionIdentity | undefined;
      let candidateHeaders: [string, string][] | undefined;
      let candidateOnline = false;
      let candidateGeneration = 0;
      let candidateCredentialGeneration = 0;
      let admissionReport: LixOpenReport | undefined;
      const providerOptions = storage.kind === "jsStorage" ? storage.options : undefined;
      const physicalScope = providerOptions && typeof providerOptions === "object" &&
        "sharedEngineKey" in providerOptions && typeof providerOptions.sharedEngineKey === "string"
        ? providerOptions.sharedEngineKey : undefined;
      if (!physicalScope?.startsWith("lix:opfs:")) throw new Error("Missing physical shared storage identity");
      const localAdmission = new DurableLocalAdmission(physicalScope, raw.url);
      const transport = raw.transport ?? fetchTransport();
      const authenticate = async (headers: [string, string][], allowOffline: boolean) => {
        const credentialGeneration = admitted.generation(raw.url, headers);
        let result: { identity: AdmissionIdentity; online: boolean };
        try {
          result = await admitted.verify(raw.url, headers, rootIdentity,
            () => requestAdmission(raw.url, headers, transport, allowOffline ? {
              onProgress: progress,
              onReport: report => { admissionReport = report; },
            } : undefined), allowOffline);
        } catch (error) {
          const code = (error as {code?: string})?.code;
          if (code === "LIX_ADMISSION_AUTH_REJECTED") {
            if (candidateHeaders && sharedCredentialKey(raw.url, candidateHeaders) === sharedCredentialKey(raw.url, headers)) {
              candidateOnline = false;
              candidateGeneration++;
            }
            admitted.remove(raw.url, headers);
            await localAdmission.remove(headers).catch(() => undefined);
          }
          if (!allowOffline || code !== "LIX_IDENTITY_UNVERIFIED_OFFLINE") throw error;
          const local = await localAdmission.read(headers).catch(() => undefined);
          if (!local) throw error;
          if (rootIdentity && !sameAdmission(local, rootIdentity)) {
            throw new HttpTransportError("LIX_SHARED_ENGINE_IDENTITY_MISMATCH", "Cached local repository/account does not match this owner");
          }
          result = { identity: local, online: false };
        }
        if (admitted.generation(raw.url, headers) !== credentialGeneration) {
          throw new HttpTransportError("LIX_ADMISSION_AUTH_REJECTED", "Credentials were rejected while local admission was in flight");
        }
        if (result.online) {
          verifiedKey = sharedCredentialKey(raw.url, headers);
          verifiedGeneration = credentialGeneration;
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
        transport: async (request) => {
          try {
            const response = await transport(request);
            return response;
          } catch (error) {
            // Foreground hydration routinely cancels the descriptor long poll.
            // Cancellation says nothing about the verified account identity.
            const cancelled = request.init.signal?.aborted ||
              (error as {code?: string})?.code === "LIX_TRANSPORT_ABORTED" ||
              (error as {name?: string})?.name === "AbortError";
            if (!cancelled) verifiedKey = undefined;
            throw error;
          }
        },
        headerProvider: async () => {
          const headers = await readHeaders();
          if (sharedCredentialKey(raw.url, headers) !== verifiedKey || admitted.generation(raw.url, headers) !== verifiedGeneration) {
            // Failure only suspends this remote lease; local sessions survive.
            verifiedKey = undefined;
            await authenticate(headers, false);
          }
          return headers;
        },
      };
      if (!owner) {
        owner = new SharedEngineOwner(async (transport, backgroundTelemetry, opener) => {
          return openLixBinding(storage, backgroundTelemetry, opener.parent, transport, opener.progress);
        });
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
          if (candidateHeaders && sharedCredentialKey(raw.url, candidateHeaders) === rejectedKey) {
            candidateOnline = false;
            candidateGeneration++;
          }
          admitted.remove(raw.url, headers);
          await localAdmission.remove(headers).catch(() => undefined);
        },
        commitIdentity: async () => {
          if (!candidateIdentity) throw new Error("Missing verified owner identity");
          rootIdentity ??= candidateIdentity;
          if (candidateOnline && candidateHeaders) {
            // The owner has checked the actual stored account before this call.
            // Failure to cache only disables later offline reopening.
            const generation = candidateGeneration;
            const headers = candidateHeaders;
            const identity = candidateIdentity;
            await admitted.persistLocal(raw.url, headers, candidateCredentialGeneration,
              () => localAdmission.record(headers, identity),
              () => localAdmission.remove(headers)).catch(() => undefined);
            // A rejection during the asynchronous write must not resurrect its proof.
            if (generation !== candidateGeneration) await localAdmission.remove(headers).catch(() => undefined);
          }
        },
        verifyIdentity: async () => {
          const headers = await readHeaders();
          const {identity, online} = await authenticate(headers, true);
          return { authorityUrl: raw.url, accountId: identity.principalId, headers, online, report: admissionReport };
        },
      };
      return { owner, client };
  };
  const controller = startWorkerHost(
    {
      postMessage: (message: WorkerResponse) => port.postMessage(message),
      onMessage: (listener) => { input = listener; },
    },
    async (...args) => {
      const prepared = await prepareClient(...args);
      const binding = await prepared.owner.attach(prepared.client);
      if (disconnected) {
        await binding.close();
        throw new Error("Shared engine client disconnected during open");
      }
      return binding;
    },
    async (storage, server, branchId) => {
      const prepared = await prepareClient(storage, undefined, undefined, server);
      await prepared.owner.convert(prepared.client, transport =>
        convertReplicaBinding(storage, transport, branchId), branchId);
    },
  );
  const disconnect = async () => {
    if (disconnected) return;
    disconnected = true;
    if (client) owner?.deactivate(client);
    let failure: unknown;
    try { await controller.close(); } catch (error) { failure = error; }
    try { if (client) await owner?.detach(client); } catch (error) { failure ??= error; }
    try {
      port.postMessage({kind:"shared.disconnected", error: failure === undefined ? undefined : serializeWorkerError(failure)});
    } finally { port.close(); }
  };
  port.onmessage = (event) => {
    const message = event.data;
    if (message?.kind === "shared.disconnect") {
      void disconnect();
      return;
    }
    if (message?.kind === "shared.clientLease") {
      // The page holds this lock. Acquisition means it closed or crashed.
      void navigator.locks.request(message.name, () => disconnect());
      return;
    }
    input?.(message as WorkerInput);
  };
  port.start();
};
