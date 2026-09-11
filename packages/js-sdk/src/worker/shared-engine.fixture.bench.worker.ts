/// <reference lib="webworker" />
// Injectable browser-test host: production session/RPC ownership, no authority.
import { startWorkerHost } from "./host.js";
import { SharedEngineOwner, type SharedEngineClient } from "./shared-engine.js";
import type { LixBinding } from "../binding-types.js";
import type { WorkerInput } from "./protocol.js";
const scope = globalThis as unknown as SharedWorkerGlobalScope;
let opens = 0;
let closes = 0;
let sessions = 0;
const values = new Map<string, string>();
const session = (): LixBinding =>
  ({
    setTelemetryParent() {},
    activeAccountId: async () => "account",
    activeBranchId: async () => "branch",
    openAnotherSession: async () => session(),
    close: async () => {},
    execute: async (sql: string) => {
      const [operation, key, value] = sql.split(":");
      if (operation === "write" && value === "from-frame")
        await new Promise((resolve) => setTimeout(resolve, 30));
      if (operation === "write") values.set(key!, value!);
      return {
        columns: [],
        rows: [
          [
            operation === "stats"
              ? JSON.stringify({ opens, closes, sessions })
              : (values.get(key!) ?? null),
          ],
        ],
        rowsAffected: 0,
        notices: [],
      };
    },
    observe: async (sql: string) => ({
      setTelemetryParent() {},
      next: async () => ({
        sequence: 0,
        mutationSequence: 0,
        result: await session().execute(sql, []),
      }),
      close() {},
    }),
  }) as unknown as LixBinding;
const owner = new SharedEngineOwner(async () => {
  opens++;
  const root = session();
  return {
    ...root,
    openAnotherSession: async () => {
      sessions++;
      return session();
    },
    close: async () => {
      closes++;
    },
  };
});
scope.onconnect = (event) => {
  const port = event.ports[0]!;
  let receive!: (message: WorkerInput) => void;
  const client: SharedEngineClient = {
    server: { url: "https://example.test", headers: [] },
    verifyIdentity: async () => ({ authorityUrl: "https://example.test", accountId: "account" }),
  };
  const host = startWorkerHost(
    {
      postMessage: (message) => port.postMessage(message),
      onMessage: (listener) => {
        receive = listener;
      },
    },
    async () => owner.attach(client),
  );
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    owner.deactivate(client);
    try {
      await host.close();
    } finally {
      await owner.detach(client);
      port.close();
    }
  };
  port.onmessage = (event) => {
    if (event.data.kind === "lease") {
      void navigator.locks.request(event.data.name, close);
      return;
    }
    if (event.data.kind === "disconnect") {
      void close();
      return;
    }
    receive(event.data);
  };
  port.start();
};
