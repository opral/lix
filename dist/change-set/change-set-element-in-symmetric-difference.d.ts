import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { ChangeSet, LixDatabaseSchema } from "../database/schema.js";
/**
 * Returns the symmetric difference between two change sets.
 *
 * The symmetric difference is the set of changes
 * that exist in either one version but not both.
 * Modeled after https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Set/symmetricDifference
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change_set_element")
 *     .where(changeSetElementInSymmetricDifference(a: changeSetA, b: changeSetB))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export declare function changeSetElementInSymmetricDifference(a: Pick<ChangeSet, "id">, b: Pick<ChangeSet, "id">): (eb: ExpressionBuilder<LixDatabaseSchema, "change_set_element">) => ExpressionWrapper<LixDatabaseSchema, "change_set_element", SqlBool>;
//# sourceMappingURL=change-set-element-in-symmetric-difference.d.ts.map