/// <reference lib="webworker" />
import { openLixBinding } from "#binding";
import { openRemoteLixBinding } from "../remote/client.js";
import {
  SharedAdmissionCache,
  SharedProbeNetworkFailure,
  sharedCredentialKey,
} from "./shared-admission.js";
import { startWorkerHost } from "./host.js";
import { SharedEngineOwner, type SharedEngineClient } from "./shared-engine.js";
import type { SyncServerBindingOptions } from "../binding-types.js";
import type { WorkerInput, WorkerResponse } from "./protocol.js";

const scope = globalThis as unknown as SharedWorkerGlobalScope;
let owner: SharedEngineOwner | undefined;
let configuration: string | undefined;
// Same-origin callers already control this physical store. Exact credentials
// previously admitted in this worker may attach offline; new credentials need
// an authenticated probe. Never persist these credentials.
const admitted = new SharedAdmissionCache();
let rootAccount: string | undefined;

scope.onconnect = (event) => {
  const port = event.ports[0]!;
  let client: SharedEngineClient | undefined;
  let disconnected = false;
  let input: ((message: WorkerInput) => void) | undefined;
  const controller = startWorkerHost(
    {
      postMessage: (message: WorkerResponse) => port.postMessage(message),
      onMessage: (listener) => {
        input = listener;
      },
    },
    async (storage, telemetry, parent, server, progress, snapshot) => {
      if (!server || snapshot)
        throw new Error("Shared partial engines require a server and existing storage");
      const config = JSON.stringify([storage, server.url]);
      if (configuration !== undefined && config !== configuration)
        throw new Error("Shared engine configuration mismatch");
      configuration = config;
      const raw = server;
      const readHeaders = async () =>
        raw.headerProvider ? await raw.headerProvider() : raw.headers;
      const verified = new Map<string, string>();
      const authenticate = async (headers: [string, string][], forceProbe = false) => {
        const key = sharedCredentialKey(raw.url, headers);
        if (!forceProbe && verified.has(key)) return verified.get(key)!;
        const account = await admitted.verify(raw.url, headers, rootAccount!, async () => {
          let networkFailure: unknown;
          const fetcher = raw.fetch ?? globalThis.fetch;
          let remote;
          try {
            remote = await openRemoteLixBinding({
              url: raw.url,
              headers,
              fetch: async (input, init) => {
                try {
                  return await fetcher(input, init);
                } catch (error) {
                  if (error instanceof Error && error.name === "TypeError" && !init?.signal?.aborted) networkFailure = error;
                  throw error;
                }
              },
            });
          } catch (error) {
            if (networkFailure !== undefined) throw new SharedProbeNetworkFailure(networkFailure);
            throw error;
          }
          try {
            return await remote.activeAccountId();
          } finally {
            await remote.close();
          }
        });
        if (verified.size >= 64) verified.delete(verified.keys().next().value!);
        verified.set(key, account);
        return account;
      };
      const routed: SyncServerBindingOptions = {
        ...raw,
        headerProvider: async () => {
          const headers = await readHeaders();
          if (rootAccount !== undefined) await authenticate(headers);
          return headers;
        },
      };
      if (!owner) {
        owner = new SharedEngineOwner(async (transport, backgroundTelemetry) => {
          const root = await openLixBinding(storage, backgroundTelemetry, parent, transport, progress);
          try {
            rootAccount = await root.activeAccountId();
            return root;
          } catch (error) {
            await root.close();
            throw error;
          }
        });
      }
      client = {
        server: routed,
        telemetry,
        rootAdmitted: (headers, account) => {
          admitted.record(raw.url, headers, account);
          verified.set(sharedCredentialKey(raw.url, headers), account);
        },
        verifyIdentity: async () => ({
          authorityUrl: raw.url,
          accountId: await (async () => {
            const headers = await readHeaders();
            return authenticate(headers, !verified.has(sharedCredentialKey(raw.url, headers)));
          })(),
        }),
      };
      const binding = await owner.attach(client);
      if (disconnected) {
        await binding.close();
        throw new Error("Shared engine client disconnected during open");
      }
      return binding;
    },
  );
  const disconnect = async () => {
    if (disconnected) return;
    disconnected = true;
    if (client) owner?.deactivate(client);
    try {
      await controller.close();
    } finally {
      try {
        if (client) await owner?.detach(client);
      } finally {
        port.close();
      }
    }
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
