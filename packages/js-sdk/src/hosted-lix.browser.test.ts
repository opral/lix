import { afterEach, beforeAll, expect, test, vi } from "vitest";
import { openLixBinding } from "./binding.browser.js";
import { createLix } from "./hosted-lix.js";
import { Lix } from "./lix.js";
import { initializeWasm } from "./wasm-init.js";
beforeAll(() => initializeWasm(), 60_000);

// Direct WASM binding keeps the test fetch in the same realm. Production browser
// usage runs this same Rust transport in the Lix worker.
afterEach(() => vi.unstubAllGlobals());
const host = { url: "https://example.com" };
const descriptor = {
	id: "01936f4e-7b6c-7c3d-8f9a-123456789abc",
	url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
};

test("browser hosted upload streams the snapshot and forwards idempotency", async () => {
	let bytes: Uint8Array | undefined;
	const fetch = vi.fn(async (_url: unknown, init?: RequestInit) => {
		expect(init?.body).toBeInstanceOf(ReadableStream);
		expect(new Headers(init?.headers).get("idempotency-key")).toBe(
			"browser-fixture",
		);
		bytes = new Uint8Array(await new Response(init?.body).arrayBuffer());
		return new Response(JSON.stringify(descriptor), { status: 201 });
	});
	vi.stubGlobal("fetch", fetch);
	const local = new Lix(await openLixBinding({ kind: "memory" }));
	try {
		await local.execute(
			"INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('browser-fixture', 'retained', true)",
		);
		const expected = new Uint8Array(
			await new Response(local.exportSnapshot()).arrayBuffer(),
		);
		expect(
			await createLix({
				server: host,
				from: local,
				idempotencyKey: "browser-fixture",
			}),
		).toEqual(descriptor);
		expect(bytes).toEqual(expected);
	} finally {
		await local.close();
	}
});

test("oversized creation responses are canceled before being buffered", async () => {
	let pulls = 0;
	const canceled = vi.fn();
	vi.stubGlobal(
		"fetch",
		vi.fn(async (_url: unknown, init?: RequestInit) => {
			await new Response(init?.body).arrayBuffer();
			const response = new Response(
				new ReadableStream(
					{
						pull(controller) {
							pulls += 1;
							controller.enqueue(new Uint8Array(32 * 1024));
						},
						cancel: canceled,
					},
					{ highWaterMark: 0 },
				),
				{ status: 201 },
			);
			response.arrayBuffer = () => {
				throw new Error("must not buffer the response");
			};
			return response;
		}),
	);
	const local = new Lix(await openLixBinding({ kind: "memory" }));
	try {
		await expect(
			createLix({ server: host, from: local }),
		).rejects.toMatchObject({ code: "LIX_SERVER_PROTOCOL_ERROR" });
		expect(pulls).toBe(3);
		await vi.waitFor(() => expect(canceled).toHaveBeenCalledOnce());
		expect((await local.execute("SELECT 1 AS alive")).rows).toHaveLength(1);
	} finally {
		await local.close();
	}
});

test("unsupported browser request streaming fails explicitly without uploading", async () => {
	vi.stubGlobal("Request", class UnsupportedRequest {});
	const fetch = vi.fn();
	vi.stubGlobal("fetch", fetch);
	const local = new Lix(await openLixBinding({ kind: "memory" }));
	try {
		await expect(
			createLix({ server: host, from: local }),
		).rejects.toMatchObject({ code: "LIX_UNSUPPORTED_OPERATION" });
		expect(fetch).not.toHaveBeenCalled();
	} finally {
		await local.close();
	}
});

test("canceling an active upload pull does not strand the snapshot producer", async () => {
	vi.stubGlobal(
		"fetch",
		vi.fn(async (_url: unknown, init?: RequestInit) => {
			const reader = (init?.body as ReadableStream<Uint8Array>).getReader();
			const pending = reader.read().catch(() => undefined);
			await Promise.resolve(); // Let the Rust pull enter its pending next() call.
			await reader.cancel();
			await pending;
			reader.releaseLock();
			return new Response(
				JSON.stringify({
					error: { code: "LIX_FORBIDDEN", message: "fixture denied" },
				}),
				{ status: 403 },
			);
		}),
	);
	const local = new Lix(await openLixBinding({ kind: "memory" }));
	try {
		const bytes = new Uint8Array(1024 * 1024);
		for (let offset = 0; offset < bytes.length; offset += 65536)
			crypto.getRandomValues(bytes.subarray(offset, offset + 65536));
		await local.execute(
			"INSERT INTO lix_file (path, content) VALUES ($1, $2)",
			["/large.bin", bytes],
		);
		await expect(
			createLix({ server: host, from: local }),
		).rejects.toMatchObject({
			code: expect.stringMatching(/^LIX_(FORBIDDEN|SNAPSHOT_IO)$/),
		});
		expect(globalThis.fetch).toHaveBeenCalledOnce();
		expect(
			(
				await local.execute(
					"SELECT path FROM lix_file WHERE path = '/large.bin'",
				)
			).rows,
		).toHaveLength(1);
	} finally {
		await local.close();
	}
}, 30_000);
