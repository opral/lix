export async function createComment(args) {
    return args.lix.db
        .insertInto("comment")
        .values({
        discussion_id: args.parentComment.discussion_id,
        parent_id: args.parentComment.id,
        content: args.content,
    })
        .returningAll()
        .executeTakeFirstOrThrow();
}
//# sourceMappingURL=create-comment.js.map