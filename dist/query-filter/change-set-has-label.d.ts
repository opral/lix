import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { LixDatabaseSchema } from "../database/schema.js";
/**
 * Selects change sets that have a label with the given name.
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change_set")
 *      .where(changeSetHasLabel("checkpoint"))
 *      .selectAll()
 *      .execute();
 *   ```
 *
 * @example
 *   You can use eb.not() to negate the filter.
 *
 *   ```ts
 *   await lix.db.selectFrom("change_set")
 * 		.where((eb) => eb.not(changeSetHasLabel("checkpoint")))
 * 		.selectAll()
 * 		.execute();
 *   ```
 */
export declare function changeSetHasLabel(name: string): (eb: ExpressionBuilder<LixDatabaseSchema, "change_set">) => ExpressionWrapper<LixDatabaseSchema, "change_set", SqlBool>;
//# sourceMappingURL=change-set-has-label.d.ts.map