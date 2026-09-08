import type { CreateLixOptions, DeleteLixOptions, HostedLix } from "./types.js";
import { createHostedFromLix } from "./lix.js";
import { hostedLixWorkerOperation } from "./worker/client.js";

async function resolveServer(server: CreateLixOptions["server"]) {
	if (!server || typeof server !== "object")
		throw new TypeError("A server is required");
	if ("fetch" in server && server.fetch !== undefined)
		throw new TypeError("hosted lifecycle does not accept a custom fetch");
	if ("mode" in server) throw new TypeError("server.mode was removed");
	const url = new URL(server.url).toString();
	const headers = new Headers(
		typeof server.headers === "function"
			? await server.headers()
			: server.headers,
	);
	const entries: [string, string][] = [];
	headers.forEach((value, key) => entries.push([key, value]));
	return { url, headers: entries };
}

/** Creates a hosted repository, optionally from a consistent snapshot of a local Lix. */
export async function createLix(options: CreateLixOptions): Promise<HostedLix> {
	if (options.from !== undefined) {
		return createHostedFromLix(options.from, async () => ({
			...(await resolveServer(options.server)),
			idempotencyKey: options.idempotencyKey,
		}));
	}
	const server = await resolveServer(options.server);
	return hostedLixWorkerOperation<HostedLix>({
		kind: "hosted.create",
		server: { ...server, idempotencyKey: options.idempotencyKey },
	});
}

/** Deletes the hosted repository. Local replicas are not deleted. */
export async function deleteLix(options: DeleteLixOptions): Promise<void> {
	const server = await resolveServer(options.server);
	await hostedLixWorkerOperation({ kind: "hosted.delete", server });
}
