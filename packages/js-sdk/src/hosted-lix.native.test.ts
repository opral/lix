import { createServer } from "node:http";
import { once } from "node:events";
import { expect, test } from "vitest";
import { createRequire } from "node:module";
// Node's loader exercises the packaged worker entry URLs and conditional imports.
const { createLix, deleteLix, openLix } = createRequire(import.meta.url)(
	"../dist/index.js",
) as typeof import("./index.js");

// Exercises the Rust HTTP lifecycle through the real Node binding. The test host
// captures the protocol payload; it does not implement a second Lix server.
test("native hosted lifecycle sends complete snapshots and preserves source history", async () => {
	const requests: {
		method?: string;
		path?: string;
		authorization?: string;
		idempotencyKey?: string;
		contentType?: string;
		body: Buffer;
	}[] = [];
	const id = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
	let origin = "";
	const server = createServer(async (request, response) => {
		const chunks: Buffer[] = [];
		for await (const chunk of request) chunks.push(Buffer.from(chunk));
		requests.push({
			method: request.method,
			path: request.url,
			authorization: request.headers.authorization,
			idempotencyKey: request.headers["idempotency-key"] as string | undefined,
			contentType: request.headers["content-type"],
			body: Buffer.concat(chunks),
		});
		if (request.method === "DELETE") {
			response.writeHead(204).end();
			return;
		}
		response
			.writeHead(201, { "content-type": "application/json" })
			.end(JSON.stringify({ id, url: `${origin}/lix/${id}` }));
	});
	server.listen(0, "127.0.0.1");
	await once(server, "listening");
	const address = server.address();
	if (!address || typeof address === "string")
		throw new Error("missing test listener");
	origin = `http://127.0.0.1:${address.port}`;
	const target = { url: origin, headers: { Authorization: "Bearer fixture" } };
	const local = await openLix();
	try {
		await local.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('hosted-fixture', 'preserved')",
		);
		await local.execute(
			"INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('untracked-fixture', 'retained', true)",
		);
		await local.execute("SELECT commit_id FROM lix_create_checkpoint()");
		const expected = Buffer.from(
			await new Response(local.exportSnapshot()).arrayBuffer(),
		);
		const repository = await createLix({
			server: target,
			from: local,
			idempotencyKey: "native-fixture",
		});
		expect(repository).toEqual({ id, url: `${origin}/lix/${id}` });
		expect(requests[0]).toMatchObject({
			method: "POST",
			path: "/lix/v1",
			authorization: "Bearer fixture",
			contentType: "application/vnd.lix.snapshot",
			idempotencyKey: "native-fixture",
		});
		expect(requests[0]?.body).toEqual(expected);
		const restored = await openLix.fromSnapshot(requests[0]!.body);
		try {
			expect(
				(
					await restored.execute(
						"SELECT value FROM lix_key_value WHERE key = 'untracked-fixture' AND lixcol_untracked = true",
					)
				).rows,
			).toHaveLength(1);
		} finally {
			await restored.close();
		}

		expect(
			(
				await local.execute(
					"SELECT value FROM lix_key_value WHERE key = 'hosted-fixture'",
				)
			).rows,
		).toHaveLength(1);
		await createLix({ server: target });
		expect(requests[1]?.body.byteLength).toBe(0);
		await deleteLix({ server: { ...target, url: repository.url } });
		expect(requests[2]).toMatchObject({
			method: "DELETE",
			path: `/lix/v1/${id}`,
			authorization: "Bearer fixture",
		});
		expect(
			(
				await local.execute(
					"SELECT value FROM lix_key_value WHERE key = 'hosted-fixture'",
				)
			).rows,
		).toHaveLength(1);
	} finally {
		await local.close();
		server.closeAllConnections();
		await new Promise<void>((resolve, reject) =>
			server.close((error) => (error ? reject(error) : resolve())),
		);
	}
}, 30_000);

test("early HTTP rejection cancels a large upload without stranding its local source", async () => {
	const { randomBytes } = await import("node:crypto");
	let rejectedRequests = 0;
	const server = createServer((_request, response) => {
		rejectedRequests += 1;
		// Deliberately do not read the upload. The repository producer must be
		// canceled even while blocked behind the bounded upload pipe.
		response
			.writeHead(403, {
				"content-type": "application/json",
				connection: "close",
			})
			.end(
				JSON.stringify({
					error: { code: "LIX_FORBIDDEN", message: "fixture denied" },
				}),
			);
	});
	server.listen(0, "127.0.0.1");
	await once(server, "listening");
	const address = server.address();
	if (!address || typeof address === "string")
		throw new Error("missing test listener");
	const local = await openLix();
	try {
		await local.execute(
			"INSERT INTO lix_file (path, content) VALUES ($1, $2)",
			["/large.bin", randomBytes(1024 * 1024)],
		);
		const started = Date.now();
		await expect(
			createLix({
				server: { url: `http://127.0.0.1:${address.port}` },
				from: local,
			}),
		).rejects.toMatchObject({
			code: expect.stringMatching(/^LIX_(FORBIDDEN|ERROR_SYNC_TRANSPORT)$/),
		});
		expect(rejectedRequests).toBe(1);
		expect(Date.now() - started).toBeLessThan(5_000);
		expect(
			(
				await local.execute(
					"SELECT path FROM lix_file WHERE path = '/large.bin'",
				)
			).rows,
		).toHaveLength(1);
	} finally {
		server.closeAllConnections();
		await local.close();
		await new Promise<void>((resolve, reject) =>
			server.close((error) => (error ? reject(error) : resolve())),
		);
	}
}, 30_000);
