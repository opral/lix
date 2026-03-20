/**
 * Turns a `Lix` into a `LixReadonly`.
 *
 * TODO https://github.com/opral/lix-sdk/issues/123
 */
export function withTransaction(lix, trx) {
    return {
        db: {
            selectFrom: trx.selectFrom,
            withRecursive: trx.withRecursive,
        },
        plugin: lix.plugin,
    };
}
//# sourceMappingURL=with-transaction.js.map