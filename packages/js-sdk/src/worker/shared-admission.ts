/** Only a fetch rejection, not an HTTP/protocol rejection, permits offline reuse. */
export class SharedProbeNetworkFailure extends Error {
  constructor(readonly original: unknown) {
    super("Shared identity probe could not reach the authority");
  }
}
const ANONYMOUS_ACCOUNT_ID = "00000000-0000-7000-8000-000000000002";
export function sharedCredentialKey(url: string, headers: [string, string][]): string {
  const entries: [string, string][] = [];
  new Headers(headers).forEach((value, key) => entries.push([key, value]));
  return JSON.stringify([url, entries]);
}

/** Bounded, memory-only proofs; no credentials are written to storage. */
export class SharedAdmissionCache {
  private readonly proofs = new Map<string, string>();
  record(url: string, headers: [string, string][], account: string): void {
    const normalized = new Headers(headers);
    // A custom fetch can inject credentials invisible here. An empty/telemetry-
    // only header set is not an offline proof of a non-anonymous principal.
    if (
      !normalized.get("authorization") &&
      !normalized.get("cookie") &&
      account !== ANONYMOUS_ACCOUNT_ID
    )
      return;
    if (this.proofs.size >= 64) this.proofs.delete(this.proofs.keys().next().value!);
    this.proofs.set(sharedCredentialKey(url, headers), account);
  }
  async verify(
    url: string,
    headers: [string, string][],
    expected: string,
    probe: () => Promise<string>,
  ): Promise<string> {
    let account: string;
    try {
      account = await probe();
    } catch (error) {
      const known = this.proofs.get(sharedCredentialKey(url, headers));
      if (!(error instanceof SharedProbeNetworkFailure) || known === undefined) throw error;
      account = known;
    }
    if (account !== expected)
      throw Object.assign(new Error("Shared engine account mismatch"), {
        code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH",
      });
    this.record(url, headers, account);
    return account;
  }
}
