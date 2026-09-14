/// <reference lib="webworker" />
import { openLixBinding, convertReplicaBinding } from "#binding";
import { fetchTransport } from "../http-transport.js";
import {
  SharedAdmissionCache,
  requestAdmission,
  type AdmissionIdentity,
  sharedCredentialKey,
} from "./shared-admission.js";
import { startWorkerHost } from "./host.js";
import { SharedEngineOwner, type SharedEngineClient } from "./shared-engine.js";
import type { SyncServerBindingOptions } from "../binding-types.js";
import { serializeWorkerError, type WorkerInput, type WorkerResponse } from "./protocol.js";

const scope = globalThis as unknown as SharedWorkerGlobalScope;
let owner: SharedEngineOwner | undefined;
let configuration: string | undefined;
// Same-origin callers already control this physical store. Exact credentials
// previously admitted in this worker may attach offline; new credentials need
// an authenticated probe. Never persist these credentials.
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
      let candidateIdentity: AdmissionIdentity | undefined;
      const transport = raw.transport ?? fetchTransport();
      const authenticate = async (headers: [string, string][], allowOffline: boolean) => {
        const result = await admitted.verify(raw.url, headers, rootIdentity,
          () => requestAdmission(raw.url, headers, transport), allowOffline);
        if (result.online) verifiedKey = sharedCredentialKey(raw.url, headers);
        candidateIdentity = result.identity;
        return result;
      };
      const routed: SyncServerBindingOptions = {
        ...raw,
        transport: async (request) => {
          try {
            const response = await transport(request);
            if (response.status === 401 || response.status === 403) verifiedKey = undefined;
            return response;
          } catch (error) {
            verifiedKey = undefined;
            throw error;
          }
        },
        headerProvider: async () => {
          const headers = await readHeaders();
          if (sharedCredentialKey(raw.url, headers) !== verifiedKey) {
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
        commitIdentity: () => {
          if (!candidateIdentity) throw new Error("Missing verified owner identity");
          rootIdentity ??= candidateIdentity;
        },
        verifyIdentity: async () => {
          const headers = await readHeaders();
          const {identity, online} = await authenticate(headers, true);
          return { authorityUrl: raw.url, accountId: identity.principalId, headers, online };
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
