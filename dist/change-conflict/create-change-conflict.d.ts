import type { Version, Change, ChangeConflict } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Creates a new change conflict with the given conflicting changes.
 *
 * @param args.key - The key of the change conflict.
 * @param args.conflictingChanges - The conflicting changes.
 */
export declare function createChangeConflict(args: {
    lix: Pick<Lix, "db">;
    key: string;
    version: Pick<Version, "id">;
    conflictingChangeIds: Set<Change["id"]>;
}): Promise<ChangeConflict>;
//# sourceMappingURL=create-change-conflict.d.ts.map