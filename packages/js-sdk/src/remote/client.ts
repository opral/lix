import type { LixBinding } from "../binding-types.js";
import type { RemoteLixServerOptions } from "../types.js";

const SERVER_PROTOCOL_PATH = "/lix/v1/";

// Generated WASM glue lives in dist/wasm; package imports keep src tests and
// the compiled SDK on the same module.
import initWasm, { openRemote } from "#remote-wasm";

type RemoteLixClientOptions = {
	initialActiveBranchId?: string;
};

let wasmInitialized: Promise<unknown> | undefined;

async function initializeRemoteWasm(): Promise<void> {
	if (wasmInitialized === undefined) {
		wasmInitialized = (async () => {
			try {
				await initWasm();
			} catch (error) {
				const wasmUrl = remoteWasmUrl();
				if (wasmUrl.protocol !== "file:") {
					throw error;
				}
				const { readFile } = await import("node:fs/promises");
				const { fileURLToPath } = await import("node:url");
				await initWasm(await readFile(fileURLToPath(wasmUrl)));
			}
		})();
	}
	await wasmInitialized;
}

function remoteWasmUrl(): URL {
	const moduleUrl =
		typeof import.meta.resolve === "function"
			? import.meta.resolve("#remote-wasm")
			: undefined;
	return new URL(
		"./lix_js_sdk_bg.wasm",
		moduleUrl ?? new URL("../wasm/lix_js_sdk.js", import.meta.url),
	);
}

export async function openRemoteLixBinding(
	options: RemoteLixServerOptions,
	clientOptions: RemoteLixClientOptions = {},
): Promise<LixBinding> {
	if (!options || typeof options !== "object") {
		throw new TypeError("openLix() remote server must be an object");
	}
	if (options.mode !== "remote") {
		throw new TypeError("openLix() remote server mode must be 'remote'");
	}
	const baseUrl = protocolBaseUrl(options.url);
	const remoteFetch = options.fetch ?? globalThis.fetch?.bind(globalThis);
	if (typeof remoteFetch !== "function") {
		throw new TypeError("openLix() remote mode requires fetch");
	}
	if (
		options.headers !== undefined &&
		typeof options.headers !== "function" &&
		!isHeadersInit(options.headers)
	) {
		throw new TypeError(
			"openLix() remote server headers must be HeadersInit or a function",
		);
	}
	if (
		clientOptions.initialActiveBranchId !== undefined &&
		clientOptions.initialActiveBranchId.length === 0
	) {
		throw new TypeError("initialActiveBranchId must be a non-empty string");
	}
	const headers =
		options.headers === undefined
			? undefined
			: typeof options.headers === "function"
				? options.headers
				: () => options.headers as HeadersInit;

	await initializeRemoteWasm();
	return (await openRemote(
		baseUrl.href,
		remoteFetch,
		headers,
		clientOptions.initialActiveBranchId,
	)) as LixBinding;
}

function protocolBaseUrl(value: string | URL): URL {
	let repositoryUrl: URL;
	try {
		repositoryUrl = new URL(value);
	} catch {
		throw new TypeError("openLix() remote server url must be an absolute URL");
	}
	if (
		repositoryUrl.protocol !== "http:" &&
		repositoryUrl.protocol !== "https:"
	) {
		throw new TypeError("openLix() remote server url must use http or https");
	}
	if (repositoryUrl.search || repositoryUrl.hash) {
		throw new TypeError(
			"openLix() remote server url must not contain a query or fragment",
		);
	}
	repositoryUrl.pathname = `${repositoryUrl.pathname.replace(/\/$/, "")}${SERVER_PROTOCOL_PATH}`;
	return repositoryUrl;
}

function isHeadersInit(value: unknown): value is HeadersInit {
	try {
		new Headers(value as HeadersInit);
		return true;
	} catch {
		return false;
	}
}
