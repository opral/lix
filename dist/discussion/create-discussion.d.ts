import type { ChangeSet, Comment, Discussion } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
/**
 * Creates a new discussion with the first comment.
 *
 * @example
 *   ```ts
 *   const changeSet = await createChangeSet({ lix, changes: ["change1", "change2"] });
 *   const discussion = await createDiscussion({ lix, changeSet, firstComment: { content: "first comment" } });
 *   ```
 *
 * @returns the created discussion
 */
export declare function createDiscussion(args: {
    lix: Pick<Lix, "db">;
    changeSet: Pick<ChangeSet, "id">;
    firstComment: Pick<Comment, "content">;
}): Promise<Discussion & {
    firstComment: Comment;
}>;
//# sourceMappingURL=create-discussion.d.ts.map