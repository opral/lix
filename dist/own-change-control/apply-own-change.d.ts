import type { Change } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Applies own changes to lix itself.
 */
export declare function applyOwnChanges(args: {
    lix: Pick<Lix, "db">;
    changes: Change[];
}): Promise<void>;
//# sourceMappingURL=apply-own-change.d.ts.map