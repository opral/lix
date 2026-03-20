import type { Lix } from "../lix/open-lix.js";
export type VectorClock = {
    session: string;
    time: number;
}[];
export declare function mergeTheirState(args: {
    /**
     * the lix to merge their state into
     */
    lix: Pick<Lix, "db">;
    /**
     * the the vector clock of the lix to merge in
     */
    sourceVectorClock: VectorClock;
    /**
     * the data to merge in
     */
    sourceData: Record<string, Array<any>>;
}): Promise<void>;
//# sourceMappingURL=merge-state.d.ts.map