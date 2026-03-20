import { ExpressionWrapper } from "kysely";
import type { Version, LixDatabaseSchema } from "../database/schema.js";
import type { SqlBool } from "kysely";
/**
 * Filters if a change is in the given Version.
 *
 * @example
 *   ```ts
 *   const changes = await lix.db.selectFrom("change")
 *      .where(changeInVersion(currentVersion))
 *      .selectAll()
 *      .execute();
 *   ```
 */
export declare function changeInVersion(version: Pick<Version, "id">): ExpressionWrapper<LixDatabaseSchema, "change", SqlBool>;
//# sourceMappingURL=change-in-version.d.ts.map