import type { Change } from "../database/schema.js";
import type { LixReadonly } from "../plugin/lix-plugin.js";
/**
 * Gets the leaf changes that only exist in the source lix.
 *
 * @example
 *   source changes = [c1, s1 (child of c1), s2 (child of s1), s3 (child of s2)]
 *   target changes = [c1, t1 (child of c1)]
 *   leaf changes only in source = [s3]
 */
export declare function getLeafChangesOnlyInSource(args: {
    sourceLix: LixReadonly;
    targetLix: LixReadonly;
}): Promise<Change[]>;
//# sourceMappingURL=merge.get-leaf-changes-only-in-source.d.ts.map