import type { Change, ChangeConflict } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Resolves a conflict by selecting one of the two
 * changes in the conflict.
 */
export declare function resolveChangeConflictBySelecting(args: {
    lix: Lix;
    conflict: ChangeConflict;
    select: Change;
}): Promise<void>;
//# sourceMappingURL=resolve-conflict-by-selecting.d.ts.map