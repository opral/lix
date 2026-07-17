import type {
	BindingExecuteResult,
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	RemoteLixServerOptions,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "../types.js";
import type { NativeLixValue } from "../value.js";
import {
	decodeExecuteResult,
	decodeHandshake,
	encodeWireValue,
	errorFromResponseBody,
	protocolError,
	record,
	remoteError,
} from "./protocol.js";

export async function openRemoteLixBinding(
	options: RemoteLixServerOptions,
): Promise<LixBinding> {
	const client = new RemoteLixBinding(options);
	await client.open();
	return client;
}

class RemoteLixBinding implements LixBinding {
	readonly #baseUrl: URL;
	readonly #fetch: NonNullable<RemoteLixServerOptions["fetch"]>;
	readonly #headers: RemoteLixServerOptions["headers"];
	readonly #initialBranchId: string | undefined;
	#activeBranchId = "";
	#acceptingOperations = true;
	#operationQueue: Promise<void> = Promise.resolve();

	constructor(options: RemoteLixServerOptions) {
		if (!options || typeof options !== "object") {
			throw new TypeError("openLix() remote server must be an object");
		}
		if (options.mode !== "remote") {
			throw new TypeError("openLix() remote server mode must be 'remote'");
		}
		this.#baseUrl = protocolBaseUrl(options.url);
		const remoteFetch = options.fetch ?? globalThis.fetch?.bind(globalThis);
		if (typeof remoteFetch !== "function") {
			throw new TypeError("openLix() remote mode requires fetch");
		}
		this.#fetch = remoteFetch;
		if (
			options.branchId !== undefined &&
			(typeof options.branchId !== "string" || options.branchId.length === 0)
		) {
			throw new TypeError(
				"openLix() remote server branchId must be a non-empty string",
			);
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
		this.#headers = options.headers;
		this.#initialBranchId = options.branchId;
	}

	async open(): Promise<void> {
		const handshake = decodeHandshake(await this.#requestJson("", { method: "GET" }));
		this.#activeBranchId = handshake.activeBranchId;
		if (this.#initialBranchId !== undefined) {
			await this.switchBranch({ branchId: this.#initialBranchId });
		}
	}

	async execute(
		sql: string,
		params: NativeLixValue[],
		options?: ExecuteOptions,
	): Promise<BindingExecuteResult> {
		this.#assertOpen();
		return this.#enqueue(async () =>
			decodeExecuteResult(
				await this.#requestJson("execute", {
					method: "POST",
					body: JSON.stringify({
						branchId: this.#activeBranchId,
						sql,
						params: params.map(encodeWireValue),
						...(options === undefined ? {} : { options }),
					}),
				}),
			),
		);
	}

	async observe(
		_sql: string,
		_params: NativeLixValue[],
	): Promise<ObserveEventsBinding> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("observe");
	}

	async beginTransaction(): Promise<LixTransactionBinding> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("beginTransaction");
	}

	async activeBranchId(): Promise<string> {
		this.#assertOpen();
		return this.#enqueue(async () => this.#activeBranchId);
	}

	async createBranch(
		options: CreateBranchOptions,
	): Promise<CreateBranchReceipt> {
		this.#assertOpen();
		return this.#enqueue(async () => {
			const value = record(
				await this.#requestJson("branch/create", {
					method: "POST",
					body: JSON.stringify({
						branchId: this.#activeBranchId,
						...options,
					}),
				}),
				"create branch response",
			);
			if (
				typeof value.id !== "string" ||
				typeof value.name !== "string" ||
				typeof value.hidden !== "boolean" ||
				typeof value.commitId !== "string"
			) {
				throw protocolError("create branch response is invalid");
			}
			return {
				id: value.id,
				name: value.name,
				hidden: value.hidden,
				commitId: value.commitId,
			};
		});
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		this.#assertOpen();
		return this.#enqueue(async () => {
			const value = record(
				await this.#requestJson("branch/switch", {
					method: "POST",
					body: JSON.stringify(options),
				}),
				"switch branch response",
			);
			if (value.branchId !== options.branchId) {
				throw protocolError("switch branch response is invalid");
			}
			this.#activeBranchId = options.branchId;
			return { branchId: options.branchId };
		});
	}

	async importFilesystemPaths(_paths: string[]): Promise<void> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("importFilesystemPaths");
	}

	async mergeBranchPreview(
		_options: MergeBranchOptions,
	): Promise<MergeBranchPreview> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("mergeBranchPreview");
	}

	async mergeBranch(
		_options: MergeBranchOptions,
	): Promise<MergeBranchReceipt> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("mergeBranch");
	}

	async syncDiskToLix(): Promise<void> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("syncDiskToLix");
	}

	async close(): Promise<void> {
		if (!this.#acceptingOperations) return this.#operationQueue;
		this.#acceptingOperations = false;
		return this.#enqueue(async () => undefined);
	}

	async #requestJson(path: string, init: RequestInit): Promise<unknown> {
		const headers = new Headers(await resolveHeaders(this.#headers));
		headers.set("accept", "application/json");
		if (init.body !== undefined) headers.set("content-type", "application/json");
		let response: Response;
		try {
			response = await this.#fetch(new URL(path, this.#baseUrl), {
				...init,
				headers,
			});
		} catch (cause) {
			throw remoteError(
				"LIX_REMOTE_UNAVAILABLE",
				"The remote Lix server is unavailable",
				{ details: { cause: errorMessage(cause) } },
			);
		}
		const text = await response.text();
		if (!response.ok) {
			let body: unknown;
			try {
				body = JSON.parse(text);
			} catch {
				throw remoteError(
					"LIX_REMOTE_REQUEST_FAILED",
					`Remote Lix request failed with status ${response.status}`,
					{
						status: response.status,
						details: text.length === 0 ? undefined : { body: text.slice(0, 1000) },
					},
				);
			}
			throw errorFromResponseBody(body, response.status);
		}
		try {
			return JSON.parse(text);
		} catch {
			throw protocolError(
				`remote response ${response.status} did not contain valid JSON`,
			);
		}
	}

	#assertOpen(): void {
		if (!this.#acceptingOperations) {
			throw remoteError("LIX_ERROR_CLOSED", "Lix is closed");
		}
	}

	#enqueue<T>(operation: () => Promise<T>): Promise<T> {
		const result = this.#operationQueue.then(operation, operation);
		this.#operationQueue = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}
}

async function resolveHeaders(
	headers: RemoteLixServerOptions["headers"],
): Promise<HeadersInit | undefined> {
	return typeof headers === "function" ? await headers() : headers;
}

function protocolBaseUrl(value: string | URL): URL {
	let workspaceUrl: URL;
	try {
		workspaceUrl = new URL(value);
	} catch {
		throw new TypeError("openLix() remote server url must be an absolute URL");
	}
	if (workspaceUrl.protocol !== "http:" && workspaceUrl.protocol !== "https:") {
		throw new TypeError("openLix() remote server url must use http or https");
	}
	if (workspaceUrl.search || workspaceUrl.hash) {
		throw new TypeError(
			"openLix() remote server url must not contain a query or fragment",
		);
	}
	workspaceUrl.pathname = `${workspaceUrl.pathname.replace(/\/$/, "")}/.lix/v1/`;
	return workspaceUrl;
}

function unsupportedRemoteOperation(operation: string): Error & { code: string } {
	return remoteError(
		"LIX_UNSUPPORTED_REMOTE_OPERATION",
		`${operation} is not supported in remote mode`,
		{ details: { operation } },
	);
}

function isHeadersInit(value: unknown): value is HeadersInit {
	try {
		new Headers(value as HeadersInit);
		return true;
	} catch {
		return false;
	}
}

function errorMessage(value: unknown): string {
	return value instanceof Error ? value.message : String(value);
}
