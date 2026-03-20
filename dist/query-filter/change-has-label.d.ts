import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { LixDatabaseSchema } from "../database/schema.js";
/**
 * Selects changes that have a label with the given name.
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change")
 *      .where(changeHasLabel("checkpoint"))
 *      .selectAll()
 *      .execute();
 *   ```
 *
 * @example
 *   You can use eb.not() to negate the filter.
 *
 *   ```ts
 *   await lix.db.selectFrom("change")
 * 		.where((eb) => eb.not(changeHasLabel("checkpoint")))
 * 		.selectAll()
 * 		.execute();
 *   ```
 */
export declare function changeHasLabel(name: string): (eb: ExpressionBuilder<LixDatabaseSchema, "change">) => ExpressionWrapper<LixDatabaseSchema, "change", SqlBool>;
//# sourceMappingURL=change-has-label.d.ts.map