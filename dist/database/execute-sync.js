/**
 * Execute a query synchronously.
 *
 * WARNING: This function is not recommended for general use.
 * Only if you need sync queries, like in a trigger for exmaple,
 * you should use this function. The function is not transforming
 * the query or the result as the db API does. You get raw SQL.
 *
 * @example
 *   const query = lix.db.selectFrom("key_value").selectAll();
 *   const result = executeSync({ lix, query }) as KeyValue[];
 */
export function executeSync(args) {
    const compiledQuery = args.query.compile();
    const columnNames = [];
    try {
        const result = args.lix.sqlite.exec({
            sql: compiledQuery.sql,
            bind: compiledQuery.parameters,
            returnValue: "resultRows",
            columnNames,
        });
        return result.map((row) => {
            const obj = {};
            columnNames.forEach((columnName, index) => {
                obj[columnName] = row[index];
            });
            return obj;
        });
    }
    catch (e) {
        console.error(`Error in executeSync for query:\n\n${compiledQuery.sql}\n\n`, e);
        return [];
    }
}
//# sourceMappingURL=execute-sync.js.map