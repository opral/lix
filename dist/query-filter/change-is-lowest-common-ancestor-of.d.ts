import { ExpressionWrapper } from "kysely";
import type { Change, LixDatabaseSchema } from "../database/schema.js";
import type { SqlBool } from "kysely";
/**
 * Filters changes that are the lowest common ancestor of the given changes.
 *
 * @example
 *   ```ts
 *   const lowestCommonAncestor = await lix.db.selectFrom("change")
 *      .where(changeIsLowestCommonAncestorOf([change1, change2, change3]))
 *      .selectAll()
 *      .executeTakeFirst();
 *   ```
 */
export declare function changeIsLowestCommonAncestorOf(changes: Pick<Change, "id">[]): ExpressionWrapper<LixDatabaseSchema, "change", SqlBool>;
//# sourceMappingURL=change-is-lowest-common-ancestor-of.d.ts.map