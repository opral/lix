import type {
	WorkerInput,
	WorkerResponse,
	SerializedWorkerError,
} from "./protocol.js";

export type RepositoryMessage =
	| { kind: "discover"; client: string; nonce: string }
	| { kind: "owner"; client: string; nonce: string; generation: string; buildId?: string }
	| { kind: "available" }
	| { kind: "gone"; generation: string }
	| { kind: "connect"; client: string; generation: string; lease: string }
	| { kind: "connected"; client: string; generation: string }
	| { kind: "input"; client: string; generation: string; message: WorkerInput }
	| { kind: "disconnect"; client: string; generation: string }
	| {
			kind: "disconnected";
			client: string;
			generation: string;
			error?: SerializedWorkerError;
	  }
	| {
			kind: "output";
			client: string;
			generation: string;
			message: WorkerResponse;
	  };

export const OPEN_TIMEOUT_MS = 30_000;
export const CLOSE_TIMEOUT_MS = 5_000;
export const CALLBACK_TIMEOUT_MS = 15_000;
export function repositoryError(
	code: string,
	message: string,
): Error & { code: string } {
	return Object.assign(new Error(message), { code });
}
