import type { FileQueueEntry } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
export declare function handleFileInsert(args: {
    lix: Pick<Lix, "db" | "plugin" | "sqlite">;
    fileQueueEntry: FileQueueEntry;
}): Promise<void>;
export declare function handleFileUpdate(args: {
    lix: Pick<Lix, "db" | "plugin" | "sqlite">;
    fileQueueEntry: FileQueueEntry;
}): Promise<void>;
/**
 * File deletions don't need to invoke a plugin to detect changes.
 *
 * Instead, we can simply query the database for all changes that are related to the file
 * and create the corresponding deletion changes for the current version.
 *
 * - simpler plugin API (because deletions don't need to be accounted for)
 * - faster file deletion (because we don't need to invoke plugins)
 */
export declare function handleFileDelete(args: {
    lix: Pick<Lix, "db" | "plugin" | "sqlite">;
    fileQueueEntry: FileQueueEntry;
}): Promise<void>;
//# sourceMappingURL=file-handlers.d.ts.map