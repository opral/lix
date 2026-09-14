import { ADMISSION_PROTOCOL_EPOCH, ADMISSION_STORAGE_EPOCH, type AdmissionIdentity } from "./shared-admission.js";

export interface DurableLocalAdmissionStore {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
}

const anonymous = "00000000-0000-7000-8000-000000000002";
const databaseName = "lix-local-admission-v1";
const storeName = "proofs";

/** Each operation closes its connection and resolves only after transaction commit. */
class IndexedDbProofStore implements DurableLocalAdmissionStore {
  private async operation(key: string, mode: "get" | "put" | "delete", value?: unknown): Promise<unknown> {
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(databaseName, 1);
      request.onupgradeneeded = () => request.result.createObjectStore(storeName);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    try {
      return await new Promise((resolve, reject) => {
        const transaction = db.transaction(storeName, mode === "get" ? "readonly" : "readwrite");
        const store = transaction.objectStore(storeName);
        const request = mode === "get" ? store.get(key) : mode === "put" ? store.put(value, key) : store.delete(key);
        transaction.oncomplete = () => resolve(request.result);
        transaction.onabort = () => reject(transaction.error ?? new Error("Local admission transaction aborted"));
        transaction.onerror = () => reject(transaction.error ?? new Error("Local admission transaction failed"));
      });
    } finally { db.close(); }
  }
  get(key: string): Promise<unknown> { return this.operation(key, "get"); }
  async put(key: string, value: unknown): Promise<void> { await this.operation(key, "put", value); }
  async delete(key: string): Promise<void> { await this.operation(key, "delete"); }
}

/** Durable local routing evidence only. Never grants or restores a remote lease. */
export class DurableLocalAdmission {
  private readonly url: string;
  private readonly repositoryId: string;
  constructor(private readonly scope: string, url: string, private readonly store: DurableLocalAdmissionStore = new IndexedDbProofStore()) {
    const parsed = new URL(url);
    const id = parsed.pathname.match(/^\/lix\/([0-9a-f-]{36})\/?$/i)?.[1];
    if (!scope || scope.length > 4096 || !id || !["https:", "http:"].includes(parsed.protocol) || parsed.search || parsed.hash || parsed.username || parsed.password) {
      throw new Error("Invalid durable local admission scope");
    }
    parsed.pathname = `/lix/${id}`;
    this.url = parsed.toString();
    this.repositoryId = id;
  }
  private async key(headers: [string, string][]): Promise<string> {
    const entries: [string, string][] = [];
    new Headers(headers).forEach((value, name) => entries.push([name, value]));
    const bytes = new TextEncoder().encode(JSON.stringify([1, this.scope, this.url, entries, ADMISSION_PROTOCOL_EPOCH, ADMISSION_STORAGE_EPOCH]));
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
  }
  private validIdentity(value: unknown): value is AdmissionIdentity {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const v = value as Record<string, unknown>;
    return Object.keys(v).sort().join(",") === "principalId,protocolEpoch,repositoryId,storageEpoch" &&
      v.repositoryId === this.repositoryId && typeof v.principalId === "string" && /^[\x21-\x7e]{1,255}$/.test(v.principalId) &&
      v.protocolEpoch === ADMISSION_PROTOCOL_EPOCH && v.storageEpoch === ADMISSION_STORAGE_EPOCH;
  }
  async read(headers: [string, string][]): Promise<AdmissionIdentity | undefined> {
    const key = await this.key(headers);
    const value = await this.store.get(key);
    if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
    const record = value as Record<string, unknown>;
    if (Object.keys(record).sort().join(",") !== "identity,key,schemaVersion" || record.schemaVersion !== 1 || record.key !== key || !this.validIdentity(record.identity)) return undefined;
    if (!new Headers(headers).get("authorization") && record.identity.principalId !== anonymous) return undefined;
    return { ...record.identity };
  }
  /** Call only after actual storage/account validation, never after metadata admission alone. */
  async record(headers: [string, string][], identity: AdmissionIdentity): Promise<void> {
    if (!this.validIdentity(identity)) throw new Error("Invalid durable local admission identity");
    if (!new Headers(headers).get("authorization") && identity.principalId !== anonymous) return;
    const key = await this.key(headers);
    await this.store.put(key, { schemaVersion: 1, key, identity: { ...identity } });
  }
  async remove(headers: [string, string][]): Promise<void> { await this.store.delete(await this.key(headers)); }
}
