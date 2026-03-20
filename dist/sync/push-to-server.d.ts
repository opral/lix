import type { Lix } from "../lix/open-lix.js";
import type { VectorClock } from "./merge-state.js";
/**
 * Pushes rows to the server.
 */
export declare function pushToServer(args: {
    id: string;
    serverUrl: string;
    lix: Pick<Lix, "db">;
    targetVectorClock: VectorClock;
}): Promise<void>;
//# sourceMappingURL=push-to-server.d.ts.map