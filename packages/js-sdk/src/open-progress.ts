import { boundedResponseBody, type HttpTransport } from "./http-transport.js";
import type { LixOpenProgress } from "./types.js";
import { ADMISSION_STORAGE_EPOCH } from "./worker/shared-admission.js";

/** Notifications cannot change an opening operation's result. */
export function emitOpenProgress(callback: ((progress: LixOpenProgress) => void) | undefined,
  progress: LixOpenProgress): void {
  try { callback?.(progress); } catch { /* Observational only. */ }
}

/** Observe bounded admission responses without taking ownership of retries. */
export function observeOpenProgress(callback?: (progress: LixOpenProgress) => void) {
  let active = callback !== undefined;
  let migrating = false;
  let observed = false;
  let fromFormat: number | undefined;
  const emit = (phase: LixOpenProgress["phase"]) => active && emitOpenProgress(callback,
    { phase, scope: "authority", ...(fromFormat === undefined ? {} : { fromFormat }), toFormat: ADMISSION_STORAGE_EPOCH });
  return {
    transport(transport: HttpTransport): HttpTransport {
      if (!callback) return transport;
      return async request => {
        const response = await transport(request);
        if (!active || request.response.mode !== "buffered" ||
            (request.init.method ?? "GET").toUpperCase() !== "GET" ||
            !/\/lix\/v1\/[^/]+(?:\/admission)?\/?$/.test(new URL(request.url).pathname)) return response;
        if (response.status === 503) {
          // The ABI already supplies a byte budget. Consume once and return the
          // same bounded bytes; do not tee or clone a potentially live body.
          const bytes = await boundedResponseBody(response, request.response.maxBytes);
          let body: any;
          try { body = JSON.parse(new TextDecoder().decode(bytes)); } catch { /* Preserve malformed errors. */ }
          if (body?.error?.code === "LIX_REPOSITORY_MIGRATING" || body?.error?.code === "LIX_ERROR_MIGRATING") {
            const from = body.error.details?.fromVersion ?? body.error.details?.fromFormat;
            if (Number.isSafeInteger(from) && from >= 0 && from <= 0xffff_ffff) fromFormat = from;
            observed = true;
            if (!migrating) { migrating = true; emit("migrating"); }
          }
          return new Response(bytes, { status: response.status, statusText: response.statusText, headers: response.headers });
        }
        if (response.ok && migrating) { migrating = false; emit("opening"); }
        return response;
      };
    },
    complete() { if (active && observed) emit("complete"); active = false; },
    stop() { active = false; },
  };
}
