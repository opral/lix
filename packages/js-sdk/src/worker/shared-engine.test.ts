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

test("telemetry joins after a disabled opener and remains after its departure", async () => {
  const a = vi.fn();
  const b = vi.fn();
  const children: Array<import("../binding-types.js").TelemetryDispatch | undefined> = [];
  let background!: import("../binding-types.js").TelemetryDispatch;
  const root = {
    activeAccountId: async () => "account",
    close: async () => {},
    openAnotherSession: async (_options: unknown, sink: import("../binding-types.js").TelemetryDispatch) => {
      children.push(sink);
      return {close: async () => {}} as unknown as LixBinding;
    },
  } as unknown as LixBinding;
  const owner = new SharedEngineOwner(async (_server, sink) => {background=sink;return root;});
  const make = (telemetry?: import("../binding-types.js").TelemetryDispatch): SharedEngineClient => ({
    server:{url:"https://example.test",headers:[]},telemetry,
    verifyIdentity:async()=>({authorityUrl:"https://example.test",accountId:"account"}),
  });
  const first=make(); const second=make(a); const third=make(b);
  await owner.attach(first); await owner.attach(second); await owner.attach(third);
  const span={} as Parameters<typeof background>[0];
  children[1]!(span);
  expect(a).toHaveBeenCalledTimes(1); expect(b).not.toHaveBeenCalled();
  await owner.detach(first); await owner.detach(second);
  background(span);
  expect(a).toHaveBeenCalledTimes(1); expect(b).toHaveBeenCalledTimes(1);
  children[2]!(span);
  expect(b).toHaveBeenCalledTimes(2);
  await owner.detach(third);
});

test("explicit conversion serializes with opening and never closes live clients", async () => {
  const f = fixture();
  let complete!: () => void;
  const held = new Promise<void>(resolve => { complete = resolve; });
  const convert = vi.fn(async () => { await held; });
  const first = f.owner.convert(f.client(), convert);
  await Promise.resolve();
  const attached = f.owner.attach(f.client());
  await Promise.resolve();
  expect(f.open).not.toHaveBeenCalled();
  complete();
  await first;
  await attached;
  expect(f.open).toHaveBeenCalledTimes(1);
  await f.owner.convert(f.client(), convert);
  expect(convert).toHaveBeenCalledTimes(1);
  expect(f.rootClose).not.toHaveBeenCalled();
});

test("concurrent explicit converters never overlap physical storage ownership", async () => {
  const f = fixture();
  let active = 0;
  let maximum = 0;
  const convert = vi.fn(async () => {
    active += 1;
    maximum = Math.max(maximum, active);
    await Promise.resolve();
    active -= 1;
  });
  await Promise.all([f.owner.convert(f.client(), convert), f.owner.convert(f.client(), convert)]);
  expect(maximum).toBe(1);
  expect(convert).toHaveBeenCalledTimes(2);
  expect(f.open).not.toHaveBeenCalled();
});

test("failed conversion propagates without opening or initializing missing storage", async () => {
  const f = fixture();
  const missing = Object.assign(new Error("missing source"), { code: "LIX_NOT_FOUND" });
  await expect(f.owner.convert(f.client(), async () => { throw missing; })).rejects.toBe(missing);
  expect(f.open).not.toHaveBeenCalled();
  await f.owner.attach(f.client());
  expect(f.open).toHaveBeenCalledTimes(1);
});

test("conversion cannot bypass the identity of an already admitted partial root", async () => {
  const f = fixture();
  await f.owner.attach(f.client());
  const convert = vi.fn(async () => {});
  await expect(f.owner.convert(f.client("account-b"), convert)).rejects.toMatchObject({
    code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH",
  });
  expect(convert).not.toHaveBeenCalled();
  expect(f.rootClose).not.toHaveBeenCalled();
});

test("conversion rejects a different requested branch on a live partial root", async () => {
  const root = {
    activeAccountId: async () => "account",
    activeBranchId: async () => "selected",
    openAnotherSession: async () => ({ close: async () => {} }),
    close: vi.fn(async () => {}),
  } as unknown as LixBinding;
  const owner = new SharedEngineOwner(async () => root);
  const client = (): SharedEngineClient => ({
    server: { url: "https://example.test", headers: [] },
    verifyIdentity: async () => ({ authorityUrl: "https://example.test", accountId: "account" }),
  });
  await owner.attach(client());
  const conversion = vi.fn(async () => {});
  await expect(owner.convert(client(), conversion, "different")).rejects.toMatchObject({ code: "LIX_PARTIAL_CONVERSION_BRANCH_MISMATCH" });
  await owner.convert(client(), conversion, "selected");
  expect(conversion).not.toHaveBeenCalled();
  expect(root.close).not.toHaveBeenCalled();
});

test("a disconnected queued converter cannot revive a dead transport", async () => {
  const f = fixture();
  let release!: () => void;
  const first = f.owner.convert(f.client(), async () => {
    await new Promise<void>(resolve => { release = resolve; });
  });
  await Promise.resolve();
  let disconnected = false;
  const second = { ...f.client(), isDisconnected: () => disconnected };
  const conversion = vi.fn(async () => {});
  const queued = f.owner.convert(second, conversion);
  disconnected = true;
  f.owner.deactivate(second);
  release();
  await first;
  await expect(queued).rejects.toThrow("disconnected before conversion");
  expect(conversion).not.toHaveBeenCalled();
  await f.owner.attach(f.client());
  expect(f.open).toHaveBeenCalledTimes(1);
});

test("an aborted active conversion releases admission for the next live client", async () => {
  const f = fixture();
  let reject!: (error: Error) => void;
  const client = f.client();
  const conversion = f.owner.convert(client, async () => {
    await new Promise<void>((_resolve, fail) => { reject = fail; });
  });
  await Promise.resolve();
  f.owner.deactivate(client);
  reject(new Error("transport closed"));
  await expect(conversion).rejects.toThrow("transport closed");
  await f.owner.attach(f.client());
  expect(f.open).toHaveBeenCalledTimes(1);
});

test("conversion-first and reopened roots use the current opener's context", async () => {
  const root = {
    activeAccountId: async () => "account-a",
    openAnotherSession: async () => ({ close: async () => {} }),
    close: async () => {},
  } as unknown as LixBinding;
  const parents: unknown[] = [];
  const owner = new SharedEngineOwner(async (_server, _telemetry, opener) => {
    parents.push(opener.parent);
    opener.progress?.({ phase: "complete", toFormat: 78 });
    return root;
  });
  const f = fixture();
  let release!: () => void;
  const conversion = owner.convert(f.client(), async () => {
    await new Promise<void>(resolve => { release = resolve; });
  });
  await Promise.resolve();
  const first = { ...f.client(), parent: { traceId: "first", spanId: "first", traceFlags: 1 }, progress: vi.fn() };
  const queued = owner.attach(first);
  release();
  await conversion;
  await queued;
  expect(parents).toEqual([first.parent]);
  expect(first.progress).toHaveBeenCalledTimes(1);
  await owner.detach(first);
  const second = { ...f.client(), parent: { traceId: "second", spanId: "second", traceFlags: 1 }, progress: vi.fn() };
  await owner.attach(second);
  expect(parents).toEqual([first.parent, second.parent]);
  expect(second.progress).toHaveBeenCalledTimes(1);
  expect(first.progress).toHaveBeenCalledTimes(1);
  await owner.detach(second);
});
