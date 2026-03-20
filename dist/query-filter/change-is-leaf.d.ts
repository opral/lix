import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { LixDatabaseSchema } from "../database/schema.js";
/**
 * Selects changes that are not a parent of any other change.
 *
 * **Careful**: This filter is not specific to any version.
 * If you want to filter changes in a specific version, use `changeIsLeafInversion`.
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change")
 *     .where(changeIsLeaf())
 *     .selectAll()
 *     .execute();
 *   ```
 */
export declare function changeIsLeaf(): (eb: ExpressionBuilder<LixDatabaseSchema, "change">) => ExpressionWrapper<LixDatabaseSchema, "change", SqlBool>;
//# sourceMappingURL=change-is-leaf.d.ts.map