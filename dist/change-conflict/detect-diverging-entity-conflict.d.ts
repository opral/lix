import type { Change } from "../database/schema.js";
import type { DetectedConflict, LixReadonly } from "../plugin/lix-plugin.js";
export declare const LIX_DIVERGING_ENTITY_CONFLICT_KEY = "lix-diverging-entity-conflict";
/**
 * Detects a diverging entity conflict between two changes.
 *
 * When two changes are diverging, it means that they are
 * based on the same entity state, but have different
 * changes to the entity.
 *
 * A diverging entity conflict usually happens when two users
 * are working on the same entity and both have made changes
 * to the entity but are not in sync.
 *
 * @note
 *   Filter out changes that are not based on the same entity
 *   before calling this function to reduce the number of
 *   comparisons.
 *
 * @example
 *   const detectedConflict = await detectDivergingEntityConflict({
 *      	lix: lix,
 * 				changes: [changeA, changeB],
 *   });
 *
 * @returns The detected conflict or undefined if no conflict was detected.
 */
export declare function detectDivergingEntityConflict(args: {
    lix: Pick<LixReadonly, "db">;
    changes: Pick<Change, "id" | "entity_id" | "file_id" | "schema_key">[];
}): Promise<DetectedConflict | undefined>;
//# sourceMappingURL=detect-diverging-entity-conflict.d.ts.map