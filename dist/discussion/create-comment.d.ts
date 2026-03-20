import type { Comment } from "../database/schema.js";
import type { Lix } from "../lix/open-lix.js";
export declare function createComment(args: {
    lix: Pick<Lix, "db">;
    parentComment: Pick<Comment, "id" | "discussion_id">;
    content: string;
}): Promise<Comment>;
//# sourceMappingURL=create-comment.d.ts.map