import type { Version, Change } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Updates the changes that are part of a version.
 *
 * This function will update the change_set_element table to point to the new changes.
 */
export declare function updateChangesInVersion(args: {
    lix: Pick<Lix, "db" | "sqlite">;
    version: Pick<Version, "id">;
    changes: Change[];
}): Promise<void>;
//# sourceMappingURL=update-changes-in-version.d.ts.map