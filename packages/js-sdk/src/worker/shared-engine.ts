import type { LixBinding, SyncServerBindingOptions } from "../binding-types.js";

export type SharedEngineClient = {
  server: SyncServerBindingOptions;
  rootAdmitted?(headers: [string, string][], accountId: string): void;
  verifyIdentity(): Promise<{ authorityUrl: string; accountId: string }>;
};

/** One physical owner; ports receive independent sessions, never the root. */
export class SharedEngineOwner {
  private root: LixBinding | undefined;
  private readonly clients = new Set<SharedEngineClient>();
  private queue: Promise<unknown> = Promise.resolve();
  constructor(private readonly open: (server: SyncServerBindingOptions) => Promise<LixBinding>) {}

  attach(client: SharedEngineClient): Promise<LixBinding> {
    const operation = this.queue.then(async () => {
      const opensRoot = this.root === undefined;
      if (!this.root) {
        const originalServer = client.server;
        const headers = originalServer.headerProvider
          ? await originalServer.headerProvider()
          : originalServer.headers;
        // Bind native admission to the exact credentials actually used during
        // opening; a later dynamic-header read cannot authorize another principal.
        client.server = { ...originalServer, headers, headerProvider: undefined };
        this.clients.add(client);
        try {
          this.root = await this.open(this.transport());
          client.rootAdmitted?.(headers, await this.root.activeAccountId());
        } catch (error) {
          this.clients.delete(client);
          throw error;
        } finally {
          client.server = originalServer;
        }
      }
      const root = this.root;
      try {
        const identity = await client.verifyIdentity();
        if (
          client.server.url !== identity.authorityUrl ||
          (await root.activeAccountId()) !== identity.accountId
        ) {
          throw Object.assign(
            new Error("Shared engine repository/account does not match this client"),
            { code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH" },
          );
        }
        this.clients.add(client);
        const report = opensRoot ? root.openReport?.() : undefined;
        const child = await root.openAnotherSession({});
        if (report === undefined) return child;
        // Only the opening caller performed initialization/migration. Later
        // attachments must not inherit that first caller's opening report.
        return new Proxy(child, {
          get(target, property) {
            if (property === "openReport") return () => report;
            const value = Reflect.get(target, property, target) as unknown;
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
      } catch (error) {
        this.clients.delete(client);
        if (this.clients.size === 0) {
          await root.close();
          this.root = undefined;
        }
        throw error;
      }
    });
    this.queue = operation.catch(() => undefined);
    return operation;
  }

  deactivate(client: SharedEngineClient): void {
    this.clients.delete(client);
  }

  async detach(client: SharedEngineClient): Promise<void> {
    const operation = this.queue.then(async () => {
      this.clients.delete(client);
      if (this.clients.size === 0 && this.root) {
        const root = this.root;
        // Keep ownership on a failed close; never open a second engine over it.
        await root.close();
        this.root = undefined;
      }
    });
    this.queue = operation.catch(() => undefined);
    await operation;
  }

  private transport(): SyncServerBindingOptions {
    const current = () => {
      const client = this.clients.values().next().value as SharedEngineClient | undefined;
      if (!client)
        throw Object.assign(new Error("No live shared-engine transport"), {
          code: "LIX_NETWORK_ERROR",
        });
      return client.server;
    };
    return {
      url: current().url,
      headers: [],
      // Pick transport and credentials together for every HTTP request. A tab
      // departure cannot send A's dynamic credentials through B's callback.
      fetch: async (input, init) => {
        const server = current();
        const supplied = server.headerProvider ? await server.headerProvider() : server.headers;
        const headers = new Headers(init?.headers);
        for (const [name, value] of supplied) headers.set(name, value);
        return (server.fetch ?? globalThis.fetch)(input, { ...init, headers });
      },
    };
  }
}
