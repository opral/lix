import { sql, ExpressionWrapper } from "kysely";
/**
 * Filter to select the last descendant of the specified change.
 *
 * @example
 *   Checking for the leaf of a change in all versiones.
 *
 *   ```ts
 *   await lix.db.selectFrom("change")
 *      .where(changeIsLeafOf(someChange))
 *      .selectAll()
 *      .execute();
 *   ```
 *
 * @example
 *   Checking for the leaf of a change in a specific version.
 *
 *   ```ts
 *   await lix.db.selectFrom("change")
 *     .where(changeIsLeafOf(someChange))
 *     .where(changeInVersion(someVersion))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export function changeIsLeafOf(change) {
    return sql `
    change.id IN (
      WITH RECURSIVE descendants(id) AS (
        SELECT id
        FROM change
        WHERE id = ${change.id}
        UNION ALL
        SELECT change.id
        FROM change
        INNER JOIN change_edge ON change_edge.child_id = change.id
        INNER JOIN descendants ON change_edge.parent_id = descendants.id
      )
      SELECT id FROM descendants
      WHERE id NOT IN (SELECT parent_id FROM change_edge)
    )
  `;
}
//# sourceMappingURL=change-is-leaf-of.js.map