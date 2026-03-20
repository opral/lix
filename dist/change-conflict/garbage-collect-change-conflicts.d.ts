import type { ChangeConflict } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Garbage collects change conflicts.
 *
 * A change conflict is considered garbage if the conflict
 * contains changes that no version change pointer points to.
 */
export declare function garbageCollectChangeConflicts(args: {
    lix: Pick<Lix, "db">;
}): Promise<{
    deletedChangeConflicts: ChangeConflict[];
}>;
//# sourceMappingURL=garbage-collect-change-conflicts.d.ts.map