import { ExpressionWrapper, type SqlBool } from "kysely";
import type { LixDatabaseSchema } from "../database/schema.js";
import type { Change } from "../database/schema.js";
/**
 * Filter to select the last descendant of the specified change.
 *
 * @example
 *   Checking for the leaf of a change in all versiones.
 *
 *   ```ts
 *   await lix.db.selectFrom("change")
 *      .where(changeIsLeafOf(someChange))
 *      .selectAll()
 *      .execute();
 *   ```
 *
 * @example
 *   Checking for the leaf of a change in a specific version.
 *
 *   ```ts
 *   await lix.db.selectFrom("change")
 *     .where(changeIsLeafOf(someChange))
 *     .where(changeInVersion(someVersion))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export declare function changeIsLeafOf(change: Pick<Change, "id">): ExpressionWrapper<LixDatabaseSchema, "change", SqlBool>;
//# sourceMappingURL=change-is-leaf-of.d.ts.map