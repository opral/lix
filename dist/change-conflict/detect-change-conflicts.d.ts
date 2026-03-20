import type { Change } from "../database/schema.js";
import type { DetectedConflict, LixReadonly } from "../plugin/lix-plugin.js";
/**
 * Detects conflicts in the given set of changes.
 *
 * The caller is responsible for filtering out changes
 * that should not lead to conflicts before calling this function.
 *
 * For example, detecting conflicts between two versiones should
 * only include changes that are different between the two versiones
 * when calling this function.
 *
 * @example
 *   const detectedConflicts = await detectChangeConflicts({
 *        lix: lix,
 *        changes: diffingChages,
 *   });
 */
export declare function detectChangeConflicts(args: {
    lix: Pick<LixReadonly, "db" | "plugin">;
    changes: Change[];
}): Promise<DetectedConflict[]>;
//# sourceMappingURL=detect-change-conflicts.d.ts.map