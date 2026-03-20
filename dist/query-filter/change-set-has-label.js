/**
 * Selects change sets that have a label with the given name.
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change_set")
 *      .where(changeSetHasLabel("checkpoint"))
 *      .selectAll()
 *      .execute();
 *   ```
 *
 * @example
 *   You can use eb.not() to negate the filter.
 *
 *   ```ts
 *   await lix.db.selectFrom("change_set")
 * 		.where((eb) => eb.not(changeSetHasLabel("checkpoint")))
 * 		.selectAll()
 * 		.execute();
 *   ```
 */
export function changeSetHasLabel(name) {
    return (eb) => eb("change_set.id", "in", (subquery) => subquery
        .selectFrom("change_set_label")
        .innerJoin("label", "label.id", "change_set_label.label_id")
        .select("change_set_label.change_set_id")
        .where("label.name", "=", name));
}
//# sourceMappingURL=change-set-has-label.js.map