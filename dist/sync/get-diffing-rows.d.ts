import type { Lix } from "../lix/open-lix.js";
import type { VectorClock } from "./merge-state.js";
export declare function getDiffingRows(args: {
    /**
     * the lix to merge their state into
     */
    lix: Pick<Lix, "db">;
    /**
     * the the vector clock of the lix to merge in
     */
    targetVectorClock: VectorClock;
}): Promise<{
    state: any;
    upsertedRows: Record<string, any[]>;
}>;
//# sourceMappingURL=get-diffing-rows.d.ts.map