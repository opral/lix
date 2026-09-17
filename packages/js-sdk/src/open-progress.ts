import type { LixOpenProgress } from "./types.js";

/** Notifications cannot change an opening operation's result. */
export function emitOpenProgress(callback: ((progress: LixOpenProgress) => void) | undefined,
  progress: LixOpenProgress): void {
  try { callback?.(progress); } catch { /* Observational only. */ }
}
