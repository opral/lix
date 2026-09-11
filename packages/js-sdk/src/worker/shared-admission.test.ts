import { expect, test } from "vitest";
import { SharedAdmissionCache, SharedProbeNetworkFailure } from "./shared-admission.js";
const url = "https://example.test/lix/repo";
const headers: [string, string][] = [["Authorization", "Bearer exact-token"]];
test("a new port probes and rejects changed account despite matching headers", async () => {
  const cache = new SharedAdmissionCache();
  cache.record(url, headers, "a");
  await expect(cache.verify(url, headers, "a", async () => "b")).rejects.toMatchObject({
    code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH",
  });
});
test("only network failure can reuse exact known authenticated credentials offline", async () => {
  const cache = new SharedAdmissionCache();
  cache.record(url, headers, "a");
  expect(
    await cache.verify(url, headers, "a", async () => {
      throw new SharedProbeNetworkFailure(new TypeError("Failed to fetch"));
    }),
  ).toBe("a");
  const auth = Object.assign(new Error("denied"), { code: "AUTH_DENIED" });
  await expect(
    cache.verify(url, headers, "a", async () => {
      throw auth;
    }),
  ).rejects.toBe(auth);
  await expect(
    cache.verify(url, [["Authorization", "other-token"]], "a", async () => {
      throw new SharedProbeNetworkFailure(new TypeError());
    }),
  ).rejects.toBeInstanceOf(SharedProbeNetworkFailure);
});
test("invisible custom-fetch credentials cannot authorize a new port offline", async () => {
  const cache = new SharedAdmissionCache();
  cache.record(url, [], "authenticated-account");
  await expect(
    cache.verify(url, [], "authenticated-account", async () => {
      throw new SharedProbeNetworkFailure(new TypeError());
    }),
  ).rejects.toBeInstanceOf(SharedProbeNetworkFailure);
});
