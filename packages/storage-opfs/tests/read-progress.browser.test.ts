import { expect, test } from "vitest";
test("coherent paged OPFS reads progress while every page races a commit", async () => {
 const worker = new Worker(new URL("./read-progress.worker.ts", import.meta.url), {type: "module"});
 try {
  const result = await new Promise<any>((resolve, reject) => {
   worker.onmessage = event => resolve(event.data);
   worker.onerror = reject;
   worker.postMessage({});
  });
  console.log("OPFS coherent read progress", JSON.stringify(result));
  expect(result.error).toBeUndefined();
  expect(result.expiry).toBeUndefined();
  expect(result.completed).toBe(20);
  expect(result.maxReadMs).toBeLessThan(1000);
  expect(result.maxWriteMs).toBeLessThan(100);
 } finally {worker.terminate();}
}, 30000);


test("OPFS retained reads preserve inserts/deletes/ranges and enforce both retention limits", async () => {
 const worker = new Worker(new URL("./read-progress.worker.ts", import.meta.url), {type:"module"});
 try {
  const result = await new Promise<any>((resolve,reject) => {worker.onmessage=e=>resolve(e.data);worker.onerror=reject;worker.postMessage({scenario:"semantics"});});
  expect(result.error).toBeUndefined();
  expect(result.semantics).toBe(true);
  expect(result.usage.bytes).toBeLessThanOrEqual(32*1024*1024);
  expect(result.usage.generation-result.usage.oldestGeneration).toBeLessThanOrEqual(512);
 } finally {worker.terminate();}
},30000);
