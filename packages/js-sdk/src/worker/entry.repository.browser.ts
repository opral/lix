/// <reference lib="webworker" />
import { createRepositoryHost } from "./repository-host.js";
import {
	CLOSE_TIMEOUT_MS,
	type RepositoryMessage,
} from "./repository-protocol.js";

let initialized = false;
onmessage = (event) => {
	if (initialized || event.data?.kind !== "start") return;
	initialized = true;
	const buildId = location.href;
	postMessage({ kind: "build", buildId });
	const { key, channelName } = event.data as {
		key: string;
		channelName: string;
	};
	const channel = new BroadcastChannel(channelName);
	const generation = crypto.randomUUID();
	const clients = new Map<string, MessagePort>();
	const departed = new Set<string>();
	const host = createRepositoryHost();
	let active = false;
	let retained = true;
	let retiring = false;
	const retire = () => {
		if (retained || clients.size || retiring) return;
		retiring = true;
		active = false;
		const timer = setTimeout(() => close(), CLOSE_TIMEOUT_MS);
		void host
			.close()
			.finally(() => {
				clearTimeout(timer);
				postMessage({ kind: "retired" });
				close();
			})
			.catch(() => undefined);
	};
	onmessage = (event) => {
		if (event.data?.kind === "retain") retained = true;
		if (event.data?.kind === "release") {
			retained = false;
			retire();
		}
	};
	const send = (message: RepositoryMessage) => channel.postMessage(message);
	channel.onmessage = (event: MessageEvent<RepositoryMessage>) => {
		const message = event.data;
		if (!active) return;
		if (message.kind === "discover") {
			send({
				kind: "owner",
				client: message.client,
				nonce: message.nonce,
				generation,
				buildId,
			});
			return;
		}
		if (
			!("generation" in message) ||
			message.generation !== generation ||
			!("client" in message)
		)
			return;
		if (message.kind === "connect") {
			if (departed.has(message.client)) return;
			if (!clients.has(message.client)) {
				const { port1, port2 } = new MessageChannel();
				clients.set(message.client, port1);
				port1.onmessage = (event) => {
					if (event.data?.kind === "repository.disconnected") {
						clients.delete(message.client);
						departed.delete(message.client);
						port1.close();
						send({
							kind: "disconnected",
							client: message.client,
							generation,
							error: event.data.error,
						});
						retire();
					} else
						send({
							kind: "output",
							client: message.client,
							generation,
							message: event.data,
						});
				};
				port1.start();
				host.connect(port2);
				// Death detection closes the host outside its finite-operation queue.
				void navigator.locks.request(message.lease, async () => {
					if (!clients.has(message.client)) return;
					departed.add(message.client);
					const timer = setTimeout(() => {
						send({ kind: "gone", generation });
						close();
					}, CLOSE_TIMEOUT_MS);
					port1.addEventListener("message", (event) => {
						if (event.data?.kind === "repository.disconnected")
							clearTimeout(timer);
					});
					port1.postMessage({ kind: "repository.disconnect" });
				});
			}
			send({ kind: "connected", client: message.client, generation });
		} else if (message.kind === "input" && !departed.has(message.client)) {
			clients.get(message.client)?.postMessage(message.message);
		} else if (message.kind === "disconnect") {
			departed.add(message.client);
			clients
				.get(message.client)
				?.postMessage({ kind: "repository.disconnect" });
		}
	};
	// This unversioned owner lock belongs to the same realm as engine and OPFS.
	// Backend physical locks remain the fence against an incompatible old build.
	void navigator.locks
		.request(`lix:repository-owner:${key}`, async () => {
			active = true;
			send({ kind: "available" });
			await new Promise<void>(() => {});
		})
		.catch((error) => {
			postMessage({ kind: "failure", message: String(error) });
			close();
		});
};
