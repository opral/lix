import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { Version, LixDatabaseSchema } from "../database/schema.js";
/**
 * Returns the symmetric difference between two versions for the version_change table.
 *
 * The symmetric difference is the set of changes
 * that exist in either one version but not both.
 * Modeled after https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Set/symmetricDifference
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("version_change")
 *     .where(versionChangeInSymmetricDifference(a: versionA, b: versionB))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export declare function versionChangeInSymmetricDifference(a: Pick<Version, "id">, b: Pick<Version, "id">): (eb: ExpressionBuilder<LixDatabaseSchema, "version_change">) => ExpressionWrapper<LixDatabaseSchema, "version_change", SqlBool>;
//# sourceMappingURL=version-change-in-symmetric-difference.d.ts.map