export async function createAccount(args) {
    const executeInTransaction = async (trx) => {
        const account = await trx
            .insertInto("account")
            .values({
            name: args.name,
        })
            .returningAll()
            .executeTakeFirstOrThrow();
        return account;
    };
    // user provided an open transaction
    if (args.lix.db.isTransaction) {
        return executeInTransaction(args.lix.db);
    }
    else {
        return args.lix.db.transaction().execute(executeInTransaction);
    }
}
//# sourceMappingURL=create-account.js.map