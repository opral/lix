import type { Lix } from "../lix/open-lix.js";
export declare function withSkipOwnChangeControl<T>(db: Lix["db"], operation: (trx: Lix["db"]) => Promise<T>): Promise<T>;
//# sourceMappingURL=with-skip-own-change-control.d.ts.map