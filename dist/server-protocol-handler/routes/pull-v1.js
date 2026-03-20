import { getDiffingRows } from "../../sync/get-diffing-rows.js";
export const route = async (context) => {
    const body = (await context.request.json());
    const exists = await context.environment.hasLix({ id: body.lix_id });
    if (!exists) {
        return new Response(null, { status: 404 });
    }
    const open = await context.environment.openLix({ id: body.lix_id });
    try {
        // console.log("----------- PROCESSING PULL FROM CLIENT  -------------");
        const { upsertedRows: tableRowsToReturn, state: sessionStatesServer } = await getDiffingRows({
            lix: open.lix,
            targetVectorClock: body.vector_clock,
        });
        // console.log("----------- DONE PROCESSING PULL FROM CLIENT  -------------");
        return new Response(JSON.stringify({
            vector_clock: sessionStatesServer,
            data: tableRowsToReturn,
        }), {
            status: 200,
            headers: {
                "Content-Type": "application/json",
            },
        });
    }
    catch (error) {
        return new Response(JSON.stringify({
            code: "FAILED_TO_FETCH_DATA",
            message: error?.message,
        }), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
            },
        });
    }
    finally {
        await context.environment.closeLix(open);
    }
};
//# sourceMappingURL=pull-v1.js.map