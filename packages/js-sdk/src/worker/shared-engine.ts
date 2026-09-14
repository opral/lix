import { fetchTransport, HttpTransportError } from "../http-transport.js";
import type { LixBinding, SyncServerBindingOptions, TelemetryDispatch, TelemetryParentContext, OpenProgressDispatch } from "../binding-types.js";

export type SharedEngineClient = {
  server: SyncServerBindingOptions;
  isDisconnected?(): boolean;
  telemetry?: TelemetryDispatch;
  parent?: TelemetryParentContext;
  progress?: OpenProgressDispatch;
  commitIdentity?(): void | Promise<void>;
  rejectCredentials?(headers: [string, string][]): void | Promise<void>;
  verifyIdentity(): Promise<{ authorityUrl: string; accountId: string; headers: [string, string][]; online?: boolean }>;
};

/** One physical owner; ports receive independent sessions, never the root. */
export class SharedEngineOwner {
  private root: LixBinding | undefined;
  private principalId: string | undefined;
  private state: "closed" | "opening" | "ready" | "closing" | "migration-exclusive" = "closed";
  get lifecycleState() { return this.state; }
  private readonly clients = new Set<SharedEngineClient>();
  private queue: Promise<unknown> = Promise.resolve();
  constructor(private readonly open: (server: SyncServerBindingOptions, telemetry: TelemetryDispatch, client: SharedEngineClient) => Promise<LixBinding>) {}

  private readonly backgroundTelemetry: TelemetryDispatch = span => {
    for (const client of this.clients) {
      try { client.telemetry?.(span); } catch { /* Host telemetry cannot interrupt engine work. */ }
    }
  };

  attach(client: SharedEngineClient): Promise<LixBinding> {
    const operation = this.queue.then(async () => {
      if (client.isDisconnected?.()) throw new Error("Shared engine client disconnected before admission");
      // Admission precedes storage opening and captures initial credentials.
      if (this.state === "closing") throw new HttpTransportError("LIX_OWNER_CLOSE_FAILED", "Storage owner has not completed closing");
      const identity = await client.verifyIdentity();
      if (client.isDisconnected?.()) throw new HttpTransportError("LIX_TRANSPORT_UNAVAILABLE", "Client disconnected during admission");
      if (identity.authorityUrl !== client.server.url) throw new HttpTransportError("LIX_SHARED_ENGINE_IDENTITY_MISMATCH", "Admission authority does not match this client");
      const opensRoot = this.root === undefined;
      if (!this.root) {
        this.state = "opening";
        const originalServer = client.server;
        const headers = identity.headers;
        // Bind native admission to the exact credentials actually used during
        // opening; a later dynamic-header read cannot authorize another principal.
        client.server = { ...originalServer, headers, headerProvider: identity.online === false
          ? async () => { throw new HttpTransportError("LIX_IDENTITY_UNVERIFIED_OFFLINE", "Cached local admission does not authorize remote requests"); }
          : undefined };
        this.clients.add(client);
        try {
          this.root = await this.open(this.transport(), this.backgroundTelemetry, client);
          const principal = await this.root.activeAccountId();
          if (principal !== identity.accountId) throw new HttpTransportError("LIX_SHARED_ENGINE_IDENTITY_MISMATCH", "Stored replica account does not match admission");
          this.principalId = principal;
          this.state = "ready";
        } catch (error) {
          this.clients.delete(client);
          if (this.root) {
            this.state = "closing";
            await this.root.close();
            this.root = undefined;
            this.principalId = undefined;
          }
          this.state = "closed";
          throw error;
        } finally {
          client.server = originalServer;
        }
      }
      const root = this.root;
      try {
        if (
          client.server.url !== identity.authorityUrl ||
          this.principalId !== identity.accountId
        ) {
          throw Object.assign(
            new Error("Shared engine repository/account does not match this client"),
            { code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH" },
          );
        }
        await client.commitIdentity?.();
        this.clients.add(client);
        const report = opensRoot ? root.openReport?.() : undefined;
        const child = await root.openAnotherSession({}, client.telemetry ?? (() => {}));
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
          this.state = "closing";
          await root.close();
          this.root = undefined;
          this.principalId = undefined;
          this.state = "closed";
        }
        throw error;
      }
    });
    this.queue = operation.catch(() => undefined);
    return operation;
  }

  /** Serialize closed-storage conversion with root admission across all ports. */
  convert(client: SharedEngineClient, conversion: (server: SyncServerBindingOptions) => Promise<void>, branchId?: string): Promise<void> {
    const operation = this.queue.then(async () => {
      if (client.isDisconnected?.()) throw new Error("Shared engine client disconnected before conversion");
      if (this.root) {
        const identity = await client.verifyIdentity();
        if (client.server.url !== identity.authorityUrl ||
            this.principalId !== identity.accountId) {
          throw Object.assign(new Error("Shared engine repository/account does not match this client"),
            { code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH" });
        }
        if (branchId !== undefined && branchId !== await this.root.activeBranchId()) {
          throw Object.assign(new Error("The converted replica selected a different branch"),
            { code: "LIX_PARTIAL_CONVERSION_BRANCH_MISMATCH" });
        }
        // A competing caller already admitted the converted partial store.
        // Never close its live sessions just to repeat an explicit conversion.
        return;
      }
      if (this.state !== "closed") throw new HttpTransportError("LIX_OWNER_NOT_CLOSED", "Migration requires a closed storage owner");
      this.state = "migration-exclusive";
      this.clients.add(client);
      try {
        await conversion(this.transport());
      } finally {
        this.clients.delete(client);
        this.state = "closed";
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
        this.state = "closing";
        await root.close();
        this.root = undefined;
        this.principalId = undefined;
        this.state = "closed";
      }
    });
    this.queue = operation.catch(() => undefined);
    await operation;
  }

  private transport(): SyncServerBindingOptions {
    const first = this.clients.values().next().value as SharedEngineClient | undefined;
    if (!first) throw new HttpTransportError("LIX_TRANSPORT_UNAVAILABLE", "No live shared-engine transport");
    return {
      url: first.server.url,
      headers: [],
      transport: async (request) => {
        let unavailable: unknown;
        // Credentials and callback remain paired when a tab leaves or suspends.
        for (const client of this.clients) {
          if (client.isDisconnected?.()) continue;
          const server = client.server;
          let supplied: [string, string][];
          try { supplied = (server.headerProvider ? await server.headerProvider() : server.headers).map(([name, value]) => [name, value]); }
          catch (error) { unavailable = error; continue; }
          if (!this.clients.has(client) || client.isDisconnected?.()) continue;
          const headers = new Headers(request.init.headers);
          for (const [name, value] of supplied) headers.set(name, value);
          const response = await (server.transport ?? fetchTransport())({ ...request, init: { ...request.init, headers, credentials: "omit" } });
          if (response.status === 401 || response.status === 403) await client.rejectCredentials?.(supplied);
          return response;
        }
        throw unavailable ?? new HttpTransportError("LIX_TRANSPORT_UNAVAILABLE", "No verified live shared-engine transport");
      },
    };
  }
}
