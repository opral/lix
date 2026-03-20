import type { Lix } from "../lix/open-lix.js";
import type { Account } from "./database-schema.js";
/**
 * Switch the current account to the provided account.
 *
 * @example
 *
 *   One active account
 *
 *   ```ts
 *   await switchAccount({ lix, to: [otherAccount] });
 *   ```
 *
 * @example
 *
 *   Multiple active accounts
 *
 *   ```ts
 *   await switchAccount({ lix, to: [account1, account2] });
 *   ```
 */
export declare function switchAccount(args: {
    lix: Pick<Lix, "db">;
    to: Account[];
}): Promise<void>;
//# sourceMappingURL=switch-account.d.ts.map