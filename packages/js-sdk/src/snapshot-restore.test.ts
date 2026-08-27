import { expect, test, vi } from "vitest";
import type { SnapshotRestoreBinding } from "./binding-types.js";
import {
	restoreSnapshot,
	SNAPSHOT_RESTORE_CHUNK_BYTES,
} from "./snapshot-restore.js";

test("restore transport copies oversized caller input into bounded owned chunks", async () => {
	const backing = new Uint8Array(SNAPSHOT_RESTORE_CHUNK_BYTES * 2 + 39);
	const callerChunk = backing.subarray(16, backing.byteLength - 16);
	for (let index = 0; index < callerChunk.byteLength; index++) {
		callerChunk[index] = index % 251;
	}
	const writes: Uint8Array[] = [];
	let complete = false;
	const restore: SnapshotRestoreBinding<string> = {
		async write(chunk) {
			writes.push(chunk);
		},
		async finish() {
			complete = true;
			return "restored";
		},
		isComplete: () => complete,
		cancel: vi.fn(),
	};
	const source = new ReadableStream<Uint8Array>(
		{
			start(controller) {
				controller.enqueue(callerChunk);
				controller.close();
			},
		},
		{ highWaterMark: 0 },
	);

	await expect(restoreSnapshot(source, restore)).resolves.toBe("restored");
	expect(writes.map((chunk) => chunk.byteLength)).toEqual([
		SNAPSHOT_RESTORE_CHUNK_BYTES,
		SNAPSHOT_RESTORE_CHUNK_BYTES,
		7,
	]);
	expect(writes.every((chunk) => chunk.buffer !== backing.buffer)).toBe(true);
	const restored = new Uint8Array(callerChunk.byteLength);
	let offset = 0;
	for (const chunk of writes) {
		restored.set(chunk, offset);
		offset += chunk.byteLength;
	}
	expect(restored).toEqual(callerChunk);
});

test("a locked source cancels and awaits an already-created restore", async () => {
	const source = new ReadableStream<Uint8Array>();
	const lock = source.getReader();
	let releaseCancellation!: () => void;
	const cancellation = new Promise<void>((resolve) => {
		releaseCancellation = resolve;
	});
	const cancel = vi.fn(async () => await cancellation);
	const restore: SnapshotRestoreBinding<never> = {
		write: vi.fn(),
		finish: vi.fn(),
		isComplete: () => false,
		cancel,
	};

	const restoring = restoreSnapshot(source, restore);
	await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
	let settled = false;
	void restoring.catch(() => undefined).then(() => {
		settled = true;
	});
	await Promise.resolve();
	expect(settled).toBe(false);
	releaseCancellation();
	await expect(restoring).rejects.toBeInstanceOf(TypeError);
	lock.releaseLock();
});

test("backend rejection interrupts a stalled source tail and keeps its semantic error", async () => {
	const semanticError = Object.assign(new Error("invalid snapshot header"), {
		name: "LixError",
		code: "LIX_INVALID_SNAPSHOT",
	});
	let complete = false;
	const restore: SnapshotRestoreBinding<never> = {
		async write() {
			queueMicrotask(() => {
				complete = true;
			});
		},
		isComplete: () => complete,
		async finish() {
			throw semanticError;
		},
		cancel: vi.fn(),
	};
	let sent = false;
	let canceled = false;
	const source = new ReadableStream<Uint8Array>(
		{
			pull(controller) {
				if (sent) return;
				sent = true;
				controller.enqueue(new TextEncoder().encode("not a snapshot"));
			},
			cancel() {
				canceled = true;
			},
		},
		{ highWaterMark: 0 },
	);

	await expect(restoreSnapshot(source, restore)).rejects.toBe(semanticError);
	expect(canceled).toBe(true);
	expect(restore.cancel).not.toHaveBeenCalled();
});
