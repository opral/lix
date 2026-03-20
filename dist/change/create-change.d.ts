import type { Account } from "../account/database-schema.js";
import type { Change, Snapshot, Version } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Programatically create a change in the database.
 *
 * Use this function to directly create a change from a lix app
 * with bypassing of file-based change detection.
 */
export declare function createChange(args: {
    lix: Pick<Lix, "db" | "sqlite">;
    authors: Array<Pick<Account, "id">>;
    version: Pick<Version, "id">;
    entityId: Change["entity_id"];
    fileId: Change["file_id"];
    pluginKey: Change["plugin_key"];
    schemaKey: Change["schema_key"];
    snapshotContent: Snapshot["content"];
}, options?: {
    /**
     * When true, the version changes will be updated.
     *
     * Defaults to true.
     */
    updateVersionChanges?: boolean;
}): Promise<Change>;
//# sourceMappingURL=create-change.d.ts.map