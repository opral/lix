/**
 * Selects changes that are not a parent of any other change.
 *
 * **Careful**: This filter is not specific to any version.
 * If you want to filter changes in a specific version, use `changeIsLeafInversion`.
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change")
 *     .where(changeIsLeaf())
 *     .selectAll()
 *     .execute();
 *   ```
 */
export function changeIsLeaf() {
    return (eb) => eb("change.id", "not in", (subquery) => subquery.selectFrom("change_edge").select("change_edge.parent_id"));
}
//# sourceMappingURL=change-is-leaf.js.map