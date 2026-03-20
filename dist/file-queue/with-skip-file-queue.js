export async function withSkipFileQueue(db, operation) {
    const executeInTransaction = async (trx) => {
        const queryEntryBefore = await trx
            .selectFrom("file_queue")
            .selectAll()
            .orderBy("id desc")
            .executeTakeFirst();
        // Perform the user's operation
        const result = await operation(trx);
        // delete queue entries that came after
        // the queue entry before the transaction
        await trx
            .deleteFrom("file_queue")
            .where("id", ">", queryEntryBefore?.id ?? 0)
            .execute();
        // Return the result of the operation
        return result;
    };
    if (db.isTransaction) {
        return executeInTransaction(db);
    }
    else {
        return db.transaction().execute(executeInTransaction);
    }
}
//# sourceMappingURL=with-skip-file-queue.js.map