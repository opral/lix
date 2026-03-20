import type { Change } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Applies the given changes to the lix.
 *
 * Calls the `applyChanges` method of the corresponding plugin for each change.
 * **Carefull**, the changes are not validated before applying them. It is up to
 * the caller to ensure that the changes are valid. Usually, only the leaf changes
 * of a given version should be applied.
 *
 * @example
 *   ```ts
 *   const changes = await lix.db.selectFrom("change")
 *      .where(changeIsLeafInVersion(currentVersion))
 *      .selectAll()
 *      .execute();
 *
 *   await applyChanges({ lix, changes });
 *   ```
 */
export declare function applyChanges(args: {
    lix: Pick<Lix, "db" | "plugin">;
    changes: Change[];
}): Promise<void>;
//# sourceMappingURL=apply-changes.d.ts.map