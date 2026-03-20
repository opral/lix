import type { Lix } from "../lix/open-lix.js";
import { type VectorClock } from "./merge-state.js";
export declare function pullFromServer(args: {
    id: string;
    lix: Pick<Lix, "db" | "plugin">;
    serverUrl: string;
}): Promise<VectorClock>;
//# sourceMappingURL=pull-from-server.d.ts.map