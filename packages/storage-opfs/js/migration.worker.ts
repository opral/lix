/// <reference lib="webworker" />
import { OpfsBackend } from "./provider.js";
import { inspectStorageProvider, migrateStorageProvider, convertStorageProviderToPartial, fetchTransport } from "@lix-js/sdk/migration";

let busy = false;
self.onmessage = async ({ data }: MessageEvent<{
  sourceName: string; destinationName: string;
  server: {url: string; headers: [string, string][]};
  expectedSourceDigest?: string;
}>) => {
  if (busy) return;
  busy = true;
  let source: OpfsBackend | undefined;
  let destination: OpfsBackend | undefined;
  let outcome: unknown;
  try {
    if (!data.sourceName || !data.destinationName || data.sourceName === data.destinationName) throw new Error("Migration requires distinct source and destination names");
    source = await OpfsBackend.open(data.sourceName);
    const sourceDigest = await source.migrationDigest();
    if (data.expectedSourceDigest && data.expectedSourceDigest !== sourceDigest) {
      throw Object.assign(new Error("An older tab changed the retained replica; reconcile those edits before continuing."), {code: "LIX_MIGRATION_SOURCE_CHANGED"});
    }
    if (source.isEmptyForMigration()) {
      outcome = {ok: true, sourceDigest, destinationName: data.destinationName, empty: true};
      return;
    }
    destination = await OpfsBackend.open(data.destinationName);
    self.postMessage({phase: "copying"});
    await source.copyForMigration(destination);
    self.postMessage({phase: "migrating"});
    const report = await migrateStorageProvider(destination);
    if (!report.semantic_preservation_verified) throw Object.assign(new Error("Migration preservation could not be verified; the source remains intact."), {code: "LIX_MIGRATION_UNVERIFIED"});
    const server = {...data.server, transport: fetchTransport()};
    if (report.after.role === "full_replica") {
      self.postMessage({phase: "converting"});
      await convertStorageProviderToPartial(destination, server);
    } else if (report.after.role !== "partial_replica") {
      throw new Error("A hosted browser replica must be a partial replica");
    }
    const after = await inspectStorageProvider(destination);
    if (!after.current || after.role !== "partial_replica") throw new Error("Migrated destination did not validate");
    if (await source.migrationDigest() !== sourceDigest) throw new Error("Migration source changed before publication");
    outcome = {ok: true, sourceDigest, destinationName: data.destinationName, report, after};
  } catch (error) {
    outcome = {ok: false, error: {
      code: error instanceof Error && "code" in error ? error.code : "LIX_MIGRATION_FAILED",
      message: error instanceof Error ? error.message : "Migration failed",
    }};
  } finally {
    const closed = await Promise.allSettled([destination?.close(), source?.close()]);
    if (closed.some(result => result.status === "rejected")) {
      outcome = {ok: false, error: {code: "LIX_MIGRATION_CLOSE_FAILED", message: "Migration storage could not close; source retained."}};
    }
    self.postMessage(outcome);
    self.close();
  }
};
