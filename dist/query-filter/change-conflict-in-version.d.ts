import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { Version, LixDatabaseSchema } from "../database/schema.js";
/**
 * Filters if a conflict is in the given version.
 *
 * @example
 *   ```ts
 *   const conflicts = await lix.db.selectFrom("change_conflict")
 *      .where(changeConflictInVersion(currentVersion))
 *      .selectAll()
 *      .execute();
 *   ```
 */
export declare function changeConflictInVersion(version: Pick<Version, "id">): (eb: ExpressionBuilder<LixDatabaseSchema, "change_conflict">) => ExpressionWrapper<LixDatabaseSchema, "change_conflict", SqlBool>;
//# sourceMappingURL=change-conflict-in-version.d.ts.map