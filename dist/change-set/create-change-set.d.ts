import type { Change, ChangeSet, Label } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Creates a change set with the given changes, optionally within an open transaction.
 *
 * @example
 *   ```ts
 *   const changes = await lix.db.selectFrom("change").selectAll().execute();
 *   const changeSet = await createChangeSet({ db: lix.db, changes });
 *   ```
 *
 * @example
 *   ```ts
 *   // Create a change set with labels
 *   const labels = await lix.db.selectFrom("label").selectAll().execute();
 *   const changeSet = await createChangeSet({
 *     lix,
 *     changes: [],
 *     labels
 *   });
 *   ```
 */
export declare function createChangeSet(args: {
    lix: Pick<Lix, "db">;
    changes: Pick<Change, "id">[];
    labels?: Pick<Label, "id">[];
}): Promise<ChangeSet>;
//# sourceMappingURL=create-change-set.d.ts.map