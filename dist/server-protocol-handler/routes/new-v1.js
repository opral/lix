import { openLixInMemory } from "../../lix/open-lix-in-memory.js";
export const route = async (context) => {
    const blob = await context.request.blob();
    let lix;
    try {
        lix = await openLixInMemory({
            blob,
            // turn off sync for server
            keyValues: [
                { key: "lix_sync", value: "false", skip_change_control: true },
            ],
        });
    }
    catch {
        return new Response(null, {
            status: 400,
        });
    }
    const lixId = await lix.db
        .selectFrom("key_value")
        .where("key", "=", "lix_id")
        .selectAll()
        .executeTakeFirstOrThrow();
    const exists = await context.environment.hasLix({ id: lixId.value });
    if (exists) {
        return new Response(null, {
            status: 409,
        });
    }
    await context.environment.setLix({ id: lixId.value, blob });
    return new Response(JSON.stringify({ id: lixId.value }), {
        status: 201,
        headers: {
            "Content-Type": "application/json",
        },
    });
};
//# sourceMappingURL=new-v1.js.map