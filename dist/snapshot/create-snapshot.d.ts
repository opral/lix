import type { Snapshot } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Creates a snapshot and inserts it or retrieves the existing snapshot from the database.
 *
 * Snapshots are content-addressed to avoid storing the same snapshot multiple times.
 * Hence, an insert might not actually insert a new snapshot but return an existing one.
 *
 * @example
 *   ```ts
 *   const snapshot = await createSnapshot({ lix, content });
 *   ```
 */
export declare function createSnapshot(args: {
    lix: Pick<Lix, "db">;
    content?: Snapshot["content"];
}): Promise<Snapshot>;
//# sourceMappingURL=create-snapshot.d.ts.map