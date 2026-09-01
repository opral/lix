import { expect, test } from "vitest";
import { openLix } from "../index.js";

test("adding an observation reconnects an established browser multiplex stream", async () => {
	const observeRequests: Request[] = [];
	let executeRequests = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
					return Response.json({
						protocolVersion: 6,
						activeBranchId: "main-id",
						activeAccountId: "00000000-0000-7000-8000-000000000002",
						sessionId: "session-1",
					});
				}
				if (request.method === "DELETE") return new Response(null, { status: 204 });
				if (pathname.endsWith("/execute")) {
					executeRequests += 1;
					const body = (await request.json()) as { sql: string };
					const value = body.sql.includes("second") ? "second" : "first";
					return Response.json({
						columns: [{ name: "value", type: "text" }],
						rows: [[{ kind: "text", value }]],
						rowsAffected: 0,
						notices: [],
					});
				}

				observeRequests.push(request.clone());
				const body = (await request.clone().json()) as {
					subscriptions: Array<{ id: string; sql: string }>;
				};
				return heldSseResponse(
					body.subscriptions
						.map(({ id, sql }) =>
							frame({
								subscriptionId: id,
								sequence: 0,
								mutationSequence: 0,
								result: {
									columns: [{ name: "value", type: "text" }],
									rows: [
										[
											{
												kind: "text",
												value: sql.includes("second") ? "second" : "first",
											},
										],
									],
									rowsAffected: 0,
									notices: [],
								},
							}),
						)
						.join(""),
					request.signal,
				);
			},
		},
	});

	const first = lix.observe("SELECT 'first' AS value");
	expect((await first.next())?.result.rows[0]?.value).toBe("first");
	const second = lix.observe("SELECT 'second' AS value");
	expect((await second.next())?.result.rows[0]?.value).toBe("second");
	expect(executeRequests).toBe(0);

	const activeRequests = observeRequests.filter(
		(request) => !request.signal.aborted,
	);
	expect(activeRequests).toHaveLength(1);
	const activeBody = (await activeRequests[0]?.json()) as {
		subscriptions: Array<{ id: string }>;
	};
	expect(activeBody.subscriptions.map(({ id }) => id)).toEqual([
		"observe-1",
		"observe-2",
	]);

	first.close();
	second.close();
	await lix.close();
});

function frame(value: unknown): string {
	return `event: next\ndata: ${JSON.stringify(value)}\n\n`;
}

function heldSseResponse(body: string, signal: AbortSignal): Response {
	const encoded = new TextEncoder().encode(body);
	return new Response(
		new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(encoded);
				const abort = () => {
					try {
						controller.error(new DOMException("Aborted", "AbortError"));
					} catch {
						// The consumer already released the stream.
					}
				};
				if (signal.aborted) abort();
				else signal.addEventListener("abort", abort, { once: true });
			},
		}),
		{ headers: { "content-type": "text/event-stream" } },
	);
}
