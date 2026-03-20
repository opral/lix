import type { Version } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Switches the current Version to the given Version.
 *
 * The Version must already exist before calling this function.
 *
 * @example
 *   ```ts
 *   await switchVersion({ lix, to: otherVersion });
 *   ```
 *
 * @example
 *   Switching Versiones to a newly created Version.
 *
 *   ```ts
 *   await lix.db.transaction().execute(async (trx) => {
 *      const newVersion = await createVersion({ lix: { db: trx }, parent: currentVersion });
 *      await switchVersion({ lix: { db: trx }, to: newVersion });
 *   });
 *   ```
 */
export declare function switchVersion(args: {
    lix: Pick<Lix, "db" | "plugin">;
    to: Pick<Version, "id">;
}): Promise<void>;
//# sourceMappingURL=switch-version.d.ts.map