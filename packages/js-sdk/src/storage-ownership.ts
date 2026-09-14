import type { LixStorage } from "./storage-adapter.js";
export const openStorages = new WeakSet<LixStorage>();
export function storageAlreadyOpen(): Error & { code: string } {
 return Object.assign(new Error("Storage is already open; close its sessions before migration or reopening"), {name:"LixError",code:"LIX_STORAGE_IN_USE"});
}
