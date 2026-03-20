import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { Version, LixDatabaseSchema } from "../database/schema.js";
/**
 * Returns the difference between two versions for the version_change table.
 *
 * The difference is the set of changes that exist in version `a` but not in version `b`.
 * Modeled after https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Set/difference
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("version_change")
 *     .where(versionChangeInDifference(a: versionA, b: versionB))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export declare function versionChangeInDifference(a: Pick<Version, "id">, b: Pick<Version, "id">): (eb: ExpressionBuilder<LixDatabaseSchema, "version_change">) => ExpressionWrapper<LixDatabaseSchema, "version_change", SqlBool>;
//# sourceMappingURL=version-change-in-difference.d.ts.map