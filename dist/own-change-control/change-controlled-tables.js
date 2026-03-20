export const changeControlledTableIds = {
    account: ["id"],
    comment: ["id"],
    change_set: ["id"],
    change_author: ["change_id", "account_id"],
    change_set_element: ["change_set_id", "change_id"],
    change_set_label: ["label_id", "change_set_id"],
    discussion: ["id"],
    file: ["id"],
    key_value: ["key"],
    version: ["id"],
    version_change: ["version_id", "change_id"],
};
/**
 * Returns the entity id for a row in a change controlled table.
 */
export function entityIdForRow(
/**
 * The name of the table.
 */
tableName, 
/**
 * The values of the row.
 */
...values) {
    let entityId = "";
    // only has one primary key
    if (changeControlledTableIds[tableName].length === 1) {
        const index = changeControlledTableIds[tableName].indexOf(
        // @ts-expect-error - no clue why
        changeControlledTableIds[tableName][0]);
        entityId = values[index];
    }
    // has compound primary key that are joined with a comma.
    else {
        for (const column of changeControlledTableIds[tableName]) {
            const index = changeControlledTableIds[tableName].indexOf(
            // @ts-expect-error - no clue why
            column);
            if (entityId === "") {
                entityId = values[index];
            }
            else {
                entityId = [entityId, values[index]].join(",");
            }
        }
    }
    return entityId;
}
/**
 * Returns the primary keys for a row in a change controlled table.
 */
export function primaryKeysForEntityId(
/**
 * The name of the table.
 */
tableName, 
/**
 * The values of the row.
 */
entitiyId) {
    const primaryKeys = changeControlledTableIds[tableName];
    if (primaryKeys.length === 1) {
        return [[primaryKeys[0], entitiyId]];
    }
    else {
        return entitiyId.split(",").map((id, index) => [primaryKeys[index], id]);
    }
}
//# sourceMappingURL=change-controlled-tables.js.map