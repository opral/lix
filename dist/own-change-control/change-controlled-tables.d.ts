export declare const changeControlledTableIds: {
    readonly account: readonly ["id"];
    readonly comment: readonly ["id"];
    readonly change_set: readonly ["id"];
    readonly change_author: readonly ["change_id", "account_id"];
    readonly change_set_element: readonly ["change_set_id", "change_id"];
    readonly change_set_label: readonly ["label_id", "change_set_id"];
    readonly discussion: readonly ["id"];
    readonly file: readonly ["id"];
    readonly key_value: readonly ["key"];
    readonly version: readonly ["id"];
    readonly version_change: readonly ["version_id", "change_id"];
};
/**
 * The result of a PRAGMA table_info call.
 *
 * @example
 *   	const tableInfo = sqlite.exec({
 *		  sql: `PRAGMA table_info(change_author);`,
 *		  returnValue: "resultRows",
 *		  rowMode: "object",
 *	  }) as PragmaTableInfo;
 */
export type PragmaTableInfo = Array<{
    /**
     * The column name
     */
    name: string;
    /**
     * 0 if not a primary key
     * 1 if primary key
     * 2... if part of a multi-column primary key
     */
    pk: number;
}>;
/**
 * Returns the entity id for a row in a change controlled table.
 */
export declare function entityIdForRow(
/**
 * The name of the table.
 */
tableName: keyof typeof changeControlledTableIds, 
/**
 * The values of the row.
 */
...values: any[]): string;
/**
 * Returns the primary keys for a row in a change controlled table.
 */
export declare function primaryKeysForEntityId(
/**
 * The name of the table.
 */
tableName: keyof typeof changeControlledTableIds, 
/**
 * The values of the row.
 */
entitiyId: string): [string, any][];
//# sourceMappingURL=change-controlled-tables.d.ts.map