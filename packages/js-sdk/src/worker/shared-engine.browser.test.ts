import { expect, test } from "vitest";
import type { WorkerOperation, WorkerResponse } from "./protocol.js";

test("actual shared-worker ports isolate IDs and survive abrupt client-context loss", async () => {
  const name = `shared-engine-test-${crypto.randomUUID()}`;
  const first = new SharedWorker(
    new URL("./shared-engine.fixture.bench.worker.ts", import.meta.url),
    { type: "module", name },
  );
  const second = new SharedWorker(
    new URL("./shared-engine.fixture.bench.worker.ts", import.meta.url),
    { type: "module", name },
  );
  let nextId = 0;
  const pending = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: unknown) => void }
  >();
  first.port.onmessage = (event: MessageEvent<WorkerResponse>) => {
    const response = event.data;
    if ("kind" in response) return;
    const request = pending.get(response.id);
    pending.delete(response.id);
    if (response.ok) request?.resolve(response.value);
    else request?.reject(response.error);
  };
  first.port.start();
  const call = (operation: WorkerOperation) =>
    new Promise<unknown>((resolve, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve, reject });
      first.port.postMessage({ id, sessionId: 0, operation });
    });
  const open: WorkerOperation = {
    kind: "open",
    storage: { kind: "memory" },
    telemetryEnabled: false,
    progressEnabled: false,
  };
  const frame = document.createElement("iframe");
  const token = crypto.randomUUID();
  frame.srcdoc = `<script>onmessage=async(event)=>{const port=event.ports[0];const token=event.data.token;let id=0;const pending=new Map();port.onmessage=e=>{const r=e.data;const p=pending.get(r.id);if(p){pending.delete(r.id);r.ok?p.resolve(r.value):p.reject(r.error)}};port.start();const call=operation=>new Promise((resolve,reject)=>{const n=++id;pending.set(n,{resolve,reject});port.postMessage({id:n,sessionId:0,operation})});navigator.locks.request(token,async()=>{port.postMessage({kind:'lease',name:token});await call(event.data.open);parent.postMessage({token,ready:true},'*');await call({kind:'execute',sql:'write:key:from-frame',params:[]});parent.postMessage({token,done:true},'*');await new Promise(()=>{})}).catch(error=>parent.postMessage({token,error:String(error)},'*'))};</script>`;
  const done = new Promise<void>((resolve, reject) => {
    const listener = (event: MessageEvent) => {
      if (
        event.source !== frame.contentWindow ||
        event.data?.token !== token ||
        (!event.data?.done && !event.data?.error)
      )
        return;
      window.removeEventListener("message", listener);
      event.data.error ? reject(new Error(event.data.error)) : resolve();
    };
    window.addEventListener("message", listener);
  });
  const ready = new Promise<void>((resolve, reject) => {
    const listener = (event: MessageEvent) => {
      if (event.source !== frame.contentWindow || event.data?.token !== token) return;
      window.removeEventListener("message", listener);
      event.data.error ? reject(new Error(event.data.error)) : resolve();
    };
    window.addEventListener("message", listener);
  });
  try {
    await call(open);
    const loaded = new Promise<void>((resolve) => {
      frame.onload = () => resolve();
    });
    document.body.append(frame);
    await loaded;
    frame.contentWindow!.postMessage({ token, open }, "*", [second.port]);
    await ready;
    const read = async (sql: string) =>
      (await call({ kind: "execute", sql, params: [] })) as { rows: unknown[][] };
    // Both clients now have request ID 2 in flight on their own ports.
    expect((await read("write:parent:independent")).rows).toEqual([["independent"]]);
    await done;
    expect((await read("read:key")).rows).toEqual([["from-frame"]]);
    expect(JSON.parse(String((await read("stats")).rows[0]![0]))).toEqual({
      opens: 1,
      closes: 0,
      sessions: 2,
    });
    // Removing the client document releases its Web Lock without graceful RPC.
    frame.remove();
    await navigator.locks.request(token, () => undefined);
    await read("write:key:survivor-offline");
    expect((await read("read:key")).rows).toEqual([["survivor-offline"]]);
    expect(JSON.parse(String((await read("stats")).rows[0]![0])).closes).toBe(0);
    const observation = (await call({ kind: "observe", sql: "read:key", params: [] })) as number;
    const event = (await call({ kind: "observe.next", observeId: observation })) as {
      result: { rows: unknown[][] };
    };
    expect(event.result.rows).toEqual([["survivor-offline"]]);
    first.port.postMessage({ kind: "observe.close", observeId: observation });
    await call({ kind: "close" });
  } finally {
    frame.remove();
    first.port.postMessage({ kind: "disconnect" });
    first.port.close();
  }
}, 30000);
