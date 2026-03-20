import type { Change, NewChange } from "../database/schema.js";
/**
 * Creates a mock change object with the given properties.
 *
 * @example
 *   const change = mockChange({ id: "change1" });
 *   await lix.db.insertInto("change").values(change).execute();
 */
export declare function mockChange(args: Partial<NewChange>): Change;
//# sourceMappingURL=mock-change.d.ts.map