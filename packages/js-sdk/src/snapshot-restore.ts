import type { SnapshotRestoreBinding } from "./binding-types.js";

export const SNAPSHOT_RESTORE_CHUNK_BYTES = 64 * 1024;

/** Copies arbitrary caller-owned input into bounded transport-owned chunks. */
export function* ownedSnapshotRestoreChunks(
	chunk: Uint8Array,
): Generator<Uint8Array> {
	for (let offset = 0; offset < chunk.byteLength; ) {
		const end = Math.min(
			offset + SNAPSHOT_RESTORE_CHUNK_BYTES,
			chunk.byteLength,
		);
		const owned = new Uint8Array(end - offset);
		owned.set(chunk.subarray(offset, end));
		yield owned;
		offset = end;
	}
}

function waitForSnapshotRestoreCompletion<T>(
	restore: SnapshotRestoreBinding<T>,
): Promise<void> {
	return new Promise((resolve, reject) => {
		const poll = () => {
			try {
				if (restore.isComplete()) {
					resolve();
					return;
				}
			} catch (error) {
				reject(error);
				return;
			}
			// Keep this lightweight for long-running imports while still surfacing a
			// decoder failure promptly when the producer has stopped yielding data.
			setTimeout(poll, 10);
		};
		poll();
	});
}

/** Pumps a snapshot into a bounded native restore without buffering the artifact. */
export async function restoreSnapshot<T>(
	source: ReadableStream<Uint8Array>,
	restore: SnapshotRestoreBinding<T>,
): Promise<T> {
	let reader: ReadableStreamDefaultReader<Uint8Array>;
	try {
		reader = source.getReader();
	} catch (error) {
		// Some internal callers create the restore before entering this helper.
		// Completion-aware cancellation prevents a locked source from stranding it.
		await Promise.resolve()
			.then(() => restore.cancel())
			.catch(() => undefined);
		throw error;
	}
	const backendCompletion = waitForSnapshotRestoreCompletion(restore).then(
		() => ({ kind: "backend-complete" }) as const,
		(error: unknown) => ({ kind: "completion-error", error }) as const,
	);
	let inputOpen = true;
	try {
		while (true) {
			const outcome = await Promise.race([
				reader.read().then(
					(result) => ({ kind: "source", result }) as const,
					(error: unknown) => ({ kind: "source-error", error }) as const,
				),
				backendCompletion,
			]);
			if (outcome.kind === "completion-error") throw outcome.error;
			if (outcome.kind === "backend-complete") {
				// The decoder can reject after accepting a malformed chunk while the
				// producer stalls forever. Stop that pending read and drain finish() so
				// the backend's structured semantic error remains authoritative.
				await reader.cancel().catch(() => undefined);
				inputOpen = false;
				return await restore.finish();
			}
			if (outcome.kind === "source-error") throw outcome.error;
			const { value, done } = outcome.result;
			if (done) break;
			if (!(value instanceof Uint8Array)) {
				throw new TypeError("snapshot stream chunks must be Uint8Array values");
			}
			for (const chunk of ownedSnapshotRestoreChunks(value)) {
				try {
					await restore.write(chunk);
				} catch (writeError) {
					// The decoder may have rejected before accepting this chunk. Stop the
					// producer, then drain finish() so its semantic Lix error wins over the
					// transport-level write rejection.
					await reader.cancel(writeError).catch(() => undefined);
					inputOpen = false;
					return await restore.finish();
				}
			}
		}
		inputOpen = false;
		return await restore.finish();
	} catch (error) {
		if (inputOpen) {
			inputOpen = false;
			await Promise.resolve()
				.then(() => restore.cancel())
				.catch(() => undefined);
		}
		throw error;
	} finally {
		reader.releaseLock();
	}
}
