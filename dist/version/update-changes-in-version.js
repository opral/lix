import { executeSync } from "../database/execute-sync.js";
/**
 * Updates the changes that are part of a version.
 *
 * This function will update the change_set_element table to point to the new changes.
 */
export async function updateChangesInVersion(args) {
    // const executeInTransaction = async (trx: Lix["db"]) => {
    for (const change of args.changes ?? []) {
        executeSync({
            lix: args.lix,
            query: args.lix.db
                .insertInto("version_change")
                .values({
                version_id: args.version.id,
                change_id: change.id,
                entity_id: change.entity_id,
                schema_key: change.schema_key,
                file_id: change.file_id,
            })
                .onConflict((oc) => oc.doUpdateSet((eb) => ({ change_id: eb.ref("excluded.change_id") }))),
        });
    }
    // await updateChangeConflicts({
    // 	lix: { ...args.lix, db: trx },
    // 	version,
    // });
    // };
    // if (args.lix.db.isTransaction) {
    // 	await executeInTransaction(args.lix.db);
    // } else {
    // 	await args.lix.db.transaction().execute(executeInTransaction);
    // }
    // await garbageCollectChangeConflicts({ lix: args.lix });
}
//# sourceMappingURL=update-changes-in-version.js.map