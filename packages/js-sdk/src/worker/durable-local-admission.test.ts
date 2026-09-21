import { expect, test } from "vitest";
import { DurableLocalAdmission, type DurableLocalAdmissionStore } from "./durable-local-admission.js";
import { ADMISSION_PROTOCOL_EPOCH, type AdmissionIdentity } from "./shared-admission.js";

const id = "00000000-0000-7000-8000-000000000004";
const url = `https://example.test/lix/${id}`;
const headers: [string, string][] = [["Authorization", "Bearer secret-token"]];
const identity: AdmissionIdentity = { repositoryId: id, principalId: "user-A", protocolEpoch: ADMISSION_PROTOCOL_EPOCH, storageEpoch: 81 };
function memory() {
  const values = new Map<string, unknown>();
  const store: DurableLocalAdmissionStore = {
    async get(key) { return structuredClone(values.get(key)); },
    async put(key, value) { values.set(key, structuredClone(value)); },
    async delete(key) { values.delete(key); },
  };
  return { store, values };
}

test("cold instance restores only exact scoped routing evidence and stores no credentials", async () => {
  const { store, values } = memory();
  await new DurableLocalAdmission("physical-A", url, store).record(headers, identity);
  expect(await new DurableLocalAdmission("physical-A", url + "/", store).read([["authorization", "Bearer secret-token"]])).toEqual(identity);
  expect(await new DurableLocalAdmission("physical-B", url, store).read(headers)).toBeUndefined();
  expect(await new DurableLocalAdmission("physical-A", url.replace("example.test", "other.test"), store).read(headers)).toBeUndefined();
  expect(await new DurableLocalAdmission("physical-A", url, store).read([["Authorization", "Bearer other-token"]])).toBeUndefined();
  expect(JSON.stringify([...values])).not.toContain("secret-token");
  expect([...values.keys()][0]).toMatch(/^[0-9a-f]{64}$/);
});

test("canonical header order matches while additional credentials remain bound", async () => {
  const { store } = memory();
  const proof = new DurableLocalAdmission("physical", url, store);
  await proof.record([...headers, ["X-Account", "a"]], identity);
  expect(await proof.read([["x-account", "a"], ...headers])).toEqual(identity);
  expect(await proof.read(headers)).toBeUndefined();
});

test.each([
  (v: any) => { v.schemaVersion = 2; },
  (v: any) => { v.key = "wrong"; },
  (v: any) => { v.identity.storageEpoch = 80; },
  (v: any) => { v.identity.protocolEpoch = 15; },
  (v: any) => { v.identity.repositoryId = "other"; },
  (v: any) => { v.identity.principalId = "bad\nidentity"; },
  (v: any) => { v.identity.unrecognized = true; },
  (v: any) => { v.extra = true; },
])("malformed or stale persisted records cannot restore identity", async mutate => {
  const { store, values } = memory();
  const proof = new DurableLocalAdmission("physical", url, store);
  await proof.record(headers, identity);
  mutate([...values.values()][0]);
  expect(await proof.read(headers)).toBeUndefined();
});

test("revocation persists across instances and invisible credentials are not recorded", async () => {
  const { store, values } = memory();
  const proof = new DurableLocalAdmission("physical", url, store);
  await proof.record([], identity);
  expect(values.size).toBe(0);
  await proof.record(headers, identity);
  await proof.remove(headers);
  expect(await new DurableLocalAdmission("physical", url, store).read(headers)).toBeUndefined();
  await expect(proof.record(headers, { ...identity, storageEpoch: 80 })).rejects.toThrow();
});

test("storage failures are explicit rather than authorizing an offline open", async () => {
  const store: DurableLocalAdmissionStore = { async get() { throw new Error("unavailable"); }, async put() { throw new Error("quota"); }, async delete() { throw new Error("unavailable"); } };
  const proof = new DurableLocalAdmission("physical", url, store);
  await expect(proof.read(headers)).rejects.toThrow("unavailable");
  await expect(proof.record(headers, identity)).rejects.toThrow("quota");
  await expect(proof.remove(headers)).rejects.toThrow("unavailable");
});

test("adding replica routing preserves an existing durable offline proof", async () => {
  const { store } = memory();
  await new DurableLocalAdmission("physical-A", url, store).record(headers, identity);
  const reopened = new DurableLocalAdmission("physical-A", url, store);
  expect(await reopened.read([...headers, ["lix-replica-id", "replica-a"]])).toEqual(identity);
  expect(await reopened.read([["Authorization", "Bearer rotated"], ["lix-replica-id", "replica-a"]])).toBeUndefined();
});
