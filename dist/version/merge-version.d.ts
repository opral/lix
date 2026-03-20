import type { Version } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
export declare function mergeVersion(args: {
    lix: Lix;
    sourceVersion: Version;
    targetVersion: Version;
}): Promise<void>;
//# sourceMappingURL=merge-version.d.ts.map