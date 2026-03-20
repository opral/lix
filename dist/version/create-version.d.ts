import type { Version } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Creates a new Version.
 *
 * If `from` is provided, the new version will be identical to the from version.
 *
 * @example
 *   _Without from_
 *
 *   ```ts
 *   const version = await createVersion({ lix });
 *   ```
 *
 * @example
 *   _With from_
 *
 *   ```ts
 *   const version = await createVersion({ lix, from: otherVersion });
 *   ```
 */
export declare function createVersion(args: {
    lix: Pick<Lix, "db">;
    from?: Pick<Version, "id">;
    name?: Version["name"];
}): Promise<Version>;
//# sourceMappingURL=create-version.d.ts.map