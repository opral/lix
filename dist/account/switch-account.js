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
export async function switchAccount(args) {
    const executeInTransaction = async (trx) => {
        // delete all rows from the current_account table
        await trx.deleteFrom("active_account").execute();
        // insert the new account id into the current_account table
        await trx.insertInto("active_account").values(args.to).execute();
    };
    if (args.lix.db.isTransaction) {
        return executeInTransaction(args.lix.db);
    }
    else {
        return args.lix.db.transaction().execute(executeInTransaction);
    }
}
//# sourceMappingURL=switch-account.js.map