import { Worker } from "node:worker_threads";
import { openLixBinding } from "../binding.node.js";
import type {
	LixBinding,
	LixStorageConfig,
	SyncServerBindingOptions,
	TelemetryDispatch,
	TelemetryParentContext,
	OpenProgressDispatch,
} from "../binding-types.js";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";

export function createWorkerConnection(): WorkerConnection {
	const worker = new Worker(new URL("./entry.node.js", import.meta.url), {
		name: "lix",
		execArgv: workerExecArgv(process.execArgv),
	});
	let terminating = false;
	return {
		postMessage(message: WorkerInput) {
			worker.postMessage(message);
		},
		onMessage(listener) {
			worker.on("message", (message: WorkerResponse) => listener(message));
		},
		onFatal(listener) {
			worker.on("error", (error) => {
				if (!terminating) listener(error);
			});
			worker.on("exit", (code) => {
				if (!terminating) listener(new Error(`Lix worker exited with code ${code}`));
			});
		},
		ref() {
			worker.ref();
		},
		unref() {
			worker.unref();
		},
		async terminate() {
			terminating = true;
			await worker.terminate();
		},
	};
}

export function workerExecArgv(execArgv: readonly string[]): string[] {
	// Workers share process-wide V8/TLS/heap configuration. Node's test runner
	// expands those defaults into execArgv, but Workers reject them when supplied
	// explicitly. Forward only module loading and permission options instead.
	const valueOptions = new Set([
		"--conditions", "-C", "--require", "-r", "--import",
		"--loader", "--experimental-loader",
		"--allow-fs-read", "--allow-fs-write",
	]);
	const flagOptions = new Set([
		"--permission", "--experimental-permission",
		"--experimental-strip-types", "--no-experimental-strip-types",
		"--experimental-transform-types", "--no-experimental-transform-types",
		"--enable-source-maps", "--no-enable-source-maps",
		"--preserve-symlinks", "--preserve-symlinks-main",
	]);
	const filtered: string[] = [];
	for (let index = 0; index < execArgv.length; index++) {
		const arg = execArgv[index];
		const option = arg.split("=", 1)[0];
		if (valueOptions.has(option)) {
			filtered.push(arg);
			if (arg === option && index + 1 < execArgv.length) {
				filtered.push(execArgv[++index]);
			}
		} else if (flagOptions.has(option) || option.startsWith("--allow-")) {
			filtered.push(arg);
		}
	}
	return filtered;
}

/// Native filesystem Lix already owns a dedicated serialized engine actor. Routing it
/// through a second JavaScript worker adds two message-port hops per query
/// without adding isolation or concurrency.
export const openDirectLixBinding = async (
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
	telemetryParent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
	openProgress?: OpenProgressDispatch,
	snapshot?: ReadableStream<Uint8Array>,
): Promise<LixBinding | undefined> => {
	if (storage.kind !== "filesystem") return undefined;
	return openLixBinding(
		storage,
		telemetry,
		telemetryParent,
		server,
		openProgress,
		snapshot,
	);
};

export function createRepositoryConnection(
	_key: string,
): WorkerConnection | undefined {
	return undefined;
}
