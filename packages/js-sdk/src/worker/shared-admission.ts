import { HttpTransportError, type HttpTransport } from "../http-transport.js";

export const ADMISSION_PROTOCOL_EPOCH = 16;
export const ADMISSION_STORAGE_EPOCH = 81;
export type AdmissionIdentity = {
  repositoryId: string;
  principalId: string;
  protocolEpoch: number;
  storageEpoch: number;
};
const ANONYMOUS_ACCOUNT_ID = "00000000-0000-7000-8000-000000000002";

export function sharedCredentialKey(url: string, headers: [string, string][]): string {
  const entries: [string, string][] = [];
  new Headers(headers).forEach((value, key) => entries.push([key, value]));
  return JSON.stringify([url, entries]);
}
export function sameAdmission(a: AdmissionIdentity, b: AdmissionIdentity): boolean {
  return a.repositoryId === b.repositoryId && a.principalId === b.principalId &&
    a.protocolEpoch === b.protocolEpoch && a.storageEpoch === b.storageEpoch;
}

/** Authorize attachment, awaiting any server-owned repository upgrade. */
export async function requestAdmission(url: string, credentials: [string, string][], transport: HttpTransport): Promise<AdmissionIdentity> {
  const locator = new URL(url);
  const repositoryId = locator.pathname.match(/\/lix\/([0-9a-f-]{36})\/?$/i)?.[1];
  if (!repositoryId || locator.search || locator.hash || locator.username || locator.password) {
    throw new HttpTransportError("LIX_TRANSPORT_CONTRACT", "Admission requires a repository protocol URL");
  }
  const headers = new Headers(credentials);
  headers.set("lix-sync-protocol-version", String(ADMISSION_PROTOCOL_EPOCH));
  locator.pathname = `/lix/v1/${repositoryId}/admission`;
  let response: Response;
  for (;;) {
    response = await transport({ url: locator.toString(),
      init: { method: "GET", headers, signal: AbortSignal.timeout(10_000), cache: "no-store", redirect: "error", credentials: "omit" },
      response: { mode: "buffered", maxBytes: 16 * 1024 } });
    if (response.status !== 503) break;
    const body = await response.clone().json().catch(() => null);
    if (body?.error?.code !== "LIX_REPOSITORY_MIGRATING") break;
    // The authority owns the migration independently of this request. Poll only
    // its explicit in-progress response; other failures retain their semantics.
    await new Promise(resolve => setTimeout(resolve, 1_000));
  }
  if (response.status === 401 || response.status === 403) {
    throw new HttpTransportError("LIX_ADMISSION_AUTH_REJECTED", "Authority rejected repository admission");
  }
  if (response.status === 409 || response.status === 426) {
    throw new HttpTransportError("LIX_ADMISSION_EPOCH", "Repository is incompatible with this client version");
  }
  if (!response.ok) throw new HttpTransportError("LIX_ADMISSION_HTTP", `Authority admission returned HTTP ${response.status}`);
  let result: AdmissionIdentity;
  try { result = await response.json(); }
  catch { throw new HttpTransportError("LIX_ADMISSION_PROTOCOL", "Authority returned invalid admission metadata"); }
  if (!result || result.repositoryId !== repositoryId ||
      typeof result.principalId !== "string" || !/^[\x21-\x7e]{1,255}$/.test(result.principalId)) {
    throw new HttpTransportError("LIX_ADMISSION_PROTOCOL", "Authority admission identity does not match the repository");
  }
  if (result.protocolEpoch !== ADMISSION_PROTOCOL_EPOCH || result.storageEpoch !== ADMISSION_STORAGE_EPOCH) {
    throw new HttpTransportError("LIX_ADMISSION_EPOCH", "Repository is incompatible with this client version");
  }
  return result;
}

/** Memory-only proofs grant cached local attachment, never remote authorization. */
export class SharedAdmissionCache {
  private readonly revocations = new Map<string, number>();
  private readonly proofs = new Map<string, AdmissionIdentity>();
  record(url: string, headers: [string, string][], identity: AdmissionIdentity): void {
    // Invisible cookies/custom-fetch identity cannot be a credential proof.
    if (!new Headers(headers).get("authorization") && identity.principalId !== ANONYMOUS_ACCOUNT_ID) return;
    if (this.proofs.size >= 64) this.proofs.delete(this.proofs.keys().next().value!);
    this.proofs.set(sharedCredentialKey(url, headers), { ...identity });
  }
  generation(url: string, headers: [string, string][]): number {
    return this.revocations.get(sharedCredentialKey(url, headers)) ?? 0;
  }
  remove(url: string, headers: [string, string][]): void {
    const key = sharedCredentialKey(url, headers);
    this.revocations.set(key, this.generation(url, headers) + 1);
    this.proofs.delete(key);
  }
  async persistLocal(url: string, headers: [string, string][], generation: number,
    write: () => Promise<void>, remove: () => Promise<void>): Promise<void> {
    if (this.generation(url, headers) !== generation) return;
    await write();
    if (this.generation(url, headers) !== generation) await remove();
  }
  async verify(url: string, headers: [string, string][], expected: AdmissionIdentity | undefined,
    probe: () => Promise<AdmissionIdentity>, allowOffline = true): Promise<{ identity: AdmissionIdentity; online: boolean }> {
    const generation = this.generation(url, headers);
    let identity: AdmissionIdentity;
    let online = true;
    try { identity = await probe(); }
    catch (error) {
      if (!(error instanceof Error) || (error as {code?: string}).code !== "LIX_TRANSPORT_NETWORK") throw error;
      const known = this.proofs.get(sharedCredentialKey(url, headers));
      if (!allowOffline || !known || !expected || !sameAdmission(known, expected)) {
        throw new HttpTransportError("LIX_IDENTITY_UNVERIFIED_OFFLINE", "Repository identity cannot be verified while offline", { cause: error });
      }
      identity = known;
      online = false;
    }
    if (this.generation(url, headers) !== generation) {
      throw new HttpTransportError("LIX_ADMISSION_AUTH_REJECTED", "Credentials were rejected while admission was in flight");
    }
    if (expected && !sameAdmission(identity, expected)) {
      throw new HttpTransportError("LIX_SHARED_ENGINE_IDENTITY_MISMATCH", "Shared engine repository/account does not match this client");
    }
    if (online) this.record(url, headers, identity);
    return { identity, online };
  }
}
