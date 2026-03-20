import { sql } from "kysely";
/**
 * Selects changes that are not a parent of any other change within the specified version.
 *
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("change")
 *     .where(changeIsLeafInVersion(currentVersion))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export function changeIsLeafInVersion(version) {
    return sql `
    change.id IN (
      WITH RECURSIVE version_changes(id) AS (
        SELECT change_id AS id
        FROM version_change
        WHERE version_id = ${version.id}
        UNION ALL
        SELECT change_edge.parent_id AS id
        FROM change_edge
        INNER JOIN version_changes ON version_changes.id = change_edge.child_id
      )
      SELECT id FROM version_changes
      WHERE id NOT IN (
        SELECT parent_id
        FROM change_edge
        WHERE child_id IN (SELECT id FROM version_changes)
      )
    )
  `;
}
//# sourceMappingURL=change-is-leaf-in-version.js.map