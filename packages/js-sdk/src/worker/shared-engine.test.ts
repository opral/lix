import { expect, test, vi } from "vitest";
import type { LixBinding, SyncServerBindingOptions } from "../binding-types.js";
import { SharedEngineOwner, type SharedEngineClient } from "./shared-engine.js";

function fixture() {
  const values = new Map<string, string>();
  const sessions: LixBinding[] = [];
  const rootClose = vi.fn(async () => {});
  const root = {
    activeAccountId: async () => "account-a",
    close: rootClose,
    openAnotherSession: vi.fn(async () => {
      const session = {
        execute: async (sql: string) => {
          if (sql.startsWith("write:")) values.set("value", sql.slice(6));
          return values.get("value");
        },
        close: vi.fn(async () => {}),
      } as unknown as LixBinding;
      sessions.push(session);
      return session;
    }),
  } as unknown as LixBinding;
  let transport: SyncServerBindingOptions | undefined;
  const open = vi.fn(async (server: SyncServerBindingOptions) => {
    transport = server;
    return root;
  });
  const owner = new SharedEngineOwner(open);
  const client = (account = "account-a"): SharedEngineClient => ({
    server: {
      url: "https://example.test/lix/repository",
      headers: [["authorization", account]],
      fetch: vi.fn(async () => new Response(account)),
    },
    verifyIdentity: async () => ({
      authorityUrl: "https://example.test/lix/repository",
      accountId: account,
    }),
  });
  return { owner, open, rootClose, client, transport: () => transport!, sessions };
}

test("concurrent clients share one root and independent local sessions", async () => {
  const f = fixture();
  const a = f.client();
  const b = f.client();
  const [sa, sb] = await Promise.all([f.owner.attach(a), f.owner.attach(b)]);
  expect(f.open).toHaveBeenCalledTimes(1);
  expect(sa).not.toBe(sb);
  await sa.execute("write:offline", []);
  expect(await sb.execute("read", [])).toBe("offline");
  await sa.close();
  await f.owner.detach(a);
  expect(f.rootClose).not.toHaveBeenCalled();
  expect(await sb.execute("read", [])).toBe("offline");
  await sb.close();
  await f.owner.detach(b);
  expect(f.rootClose).toHaveBeenCalledTimes(1);
});

test("a different authenticated account cannot receive a session or replace transport", async () => {
  const f = fixture();
  const a = f.client();
  await f.owner.attach(a);
  await expect(f.owner.attach(f.client("account-b"))).rejects.toMatchObject({
    code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH",
  });
  expect(f.sessions).toHaveLength(1);
  await f.transport().fetch!("https://example.test", {});
  expect(a.server.fetch).toHaveBeenCalledTimes(1);
});

test("transport hands off only to attached clients and rejects when none remain", async () => {
  const f = fixture();
  const a = f.client();
  const b = f.client();
  await f.owner.attach(a);
  await f.owner.attach(b);
  f.owner.deactivate(a);
  await f.transport().fetch!("https://example.test", {});
  expect(a.server.fetch).not.toHaveBeenCalled();
  expect(b.server.fetch).toHaveBeenCalledTimes(1);
  f.owner.deactivate(b);
  await expect(f.transport().fetch!("https://example.test", {})).rejects.toMatchObject({
    code: "LIX_NETWORK_ERROR",
  });
  await f.owner.detach(b);
});

test("root admission records the exact frozen opening credentials", async () => {
  let value = "old-token";
  const admitted = vi.fn();
  const child = {} as LixBinding;
  const root = {
    activeAccountId: async () => "account-a",
    openAnotherSession: async () => child,
    close: async () => {},
  } as unknown as LixBinding;
  const sent: string[] = [];
  const owner = new SharedEngineOwner(async (server) => {
    value = "new-token";
    await server.fetch!("https://example.test/handshake", {});
    return root;
  });
  await owner.attach({
    server: {
      url: "https://example.test",
      headers: [],
      headerProvider: async () => [["Authorization", value]],
      fetch: async (_input, init) => {
        sent.push(new Headers(init?.headers).get("authorization")!);
        return new Response();
      },
    },
    verifyIdentity: async () => ({ authorityUrl: "https://example.test", accountId: "account-a" }),
    rootAdmitted: admitted,
  });
  expect(sent).toEqual(["old-token"]);
  expect(admitted).toHaveBeenCalledWith([["Authorization", "old-token"]], "account-a");
});

test("only the first attachment receives the root initialization report", async () => {
  const report = { format: 79, initialized: true };
  const close = vi.fn(async () => {});
  const child = { close, activeAccountId: async () => "account-a" } as unknown as LixBinding;
  const root = {
    activeAccountId: async () => "account-a",
    openReport: () => report,
    openAnotherSession: async () => child,
    close: async () => {},
  } as unknown as LixBinding;
  const owner = new SharedEngineOwner(async () => root);
  const client = (): SharedEngineClient => ({
    server: { url: "https://example.test", headers: [] },
    verifyIdentity: async () => ({ authorityUrl: "https://example.test", accountId: "account-a" }),
  });
  const first = await owner.attach(client());
  const second = await owner.attach(client());
  expect(first.openReport?.()).toBe(report);
  expect(second.openReport?.()).toBeUndefined();
  expect(await first.activeAccountId()).toBe("account-a");
  await first.close();
  expect(close).toHaveBeenCalledTimes(1);
});
