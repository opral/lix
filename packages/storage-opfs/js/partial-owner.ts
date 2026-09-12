import { physicalName } from "./physical-name.js";

/** A synchronous acquisition handle makes cancellation safe before ready settles. */
export function acquirePartialOwner(name: string): { ready: Promise<void>; close(): void } {
 let closed = false;
 let release!: () => void;
 const lifetime = new Promise<void>(resolve => { release = resolve; });
 let resolveReady!: () => void;
 let rejectReady!: (error: unknown) => void;
 const ready = new Promise<void>((resolve, reject) => { resolveReady=resolve; rejectReady=reject; });
 // Avoid an unhandled rejection if the caller closes before awaiting ready.
 void ready.catch(() => undefined);
 const close = () => {
  if (closed) return;
  closed = true;
  release();
  rejectReady(Object.assign(new Error("Partial owner acquisition closed"), {code:"LIX_STORAGE_CLOSED"}));
 };
 if (!navigator.locks) {
  rejectReady(Object.assign(new Error("Partial OPFS ownership requires Web Locks"), {code:"LIX_STORAGE_UNSUPPORTED",details:{capability:"partialReplicaOwner"}}));
  return {ready,close};
 }
 // The OPFS filename is derived from TextEncoder bytes. Use those same bytes
 // so lone-surrogate aliases cannot acquire distinct locks for one file.
 void navigator.locks.request(`lix:partial-engine:${physicalName(name)}`, {mode:"exclusive",ifAvailable:true}, async lock => {
  if (closed) return;
  if (!lock) {
   rejectReady(Object.assign(new Error("This OPFS database already has a partial engine owner"), {code:"LIX_STORAGE_IN_USE"}));
   return;
  }
  resolveReady();
  await lifetime;
 }).catch(rejectReady);
 return {ready,close};
}

/** Keep the provider live while detached native publication owns its lock. */
export class PartialOwnerLifetimes {
 #closing = false;
 readonly #pending = new Set<Promise<void>>();
 acquire(name: string) {
  if (this.#closing) throw Object.assign(new Error("Storage provider is closing"), {code:"LIX_STORAGE_CLOSED"});
  const owner = acquirePartialOwner(name);
  let finish!: () => void;
  const done = new Promise<void>(resolve => { finish=resolve; });
  this.#pending.add(done);
  let released = false;
  return { ready: owner.ready, close: () => {
   if (released) return;
   released=true;
   owner.close();
   this.#pending.delete(done);
   finish();
  } };
 }
 async close(): Promise<void> {
  this.#closing=true;
  await Promise.all([...this.#pending]);
 }
}
