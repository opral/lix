import { type ExpressionWrapper, type SqlBool } from "kysely";
import type { Version, LixDatabaseSchema } from "../database/schema.js";
/**
 * Selects changes that are not a parent of any other change within the specified version.
 *
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change")
 *     .where(changeIsLeafInVersion(currentVersion))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export declare function changeIsLeafInVersion(version: Pick<Version, "id">): ExpressionWrapper<LixDatabaseSchema, "change", SqlBool>;
//# sourceMappingURL=change-is-leaf-in-version.d.ts.map