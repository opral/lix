import type { LixStorageChangeWatch } from "@lix-js/sdk";

/**
 * Coalesces storage invalidations while preserving the next unseen change for
 * every watcher. The sequence is package-private; consumers only receive a
 * wake-up signal through `changed()`.
 */
export class StorageChangeNotifier {
	#sequence = 0;
	#closedError: Error | undefined;
	readonly #watches = new Set<StorageChangeWatch>();

	watch(): LixStorageChangeWatch {
		if (this.#closedError) throw this.#closedError;
		const watch = new StorageChangeWatch(this, this.#sequence);
		this.#watches.add(watch);
		return watch;
	}

	notify(): void {
		if (this.#closedError) return;
		this.#sequence += 1;
		for (const watch of this.#watches) watch.notify(this.#sequence);
	}

	close(error: Error): void {
		if (this.#closedError) return;
		this.#closedError = error;
		for (const watch of this.#watches) watch.fail(error);
		this.#watches.clear();
	}

	remove(watch: StorageChangeWatch): void {
		this.#watches.delete(watch);
	}

	currentSequence(): number {
		return this.#sequence;
	}
}

class StorageChangeWatch implements LixStorageChangeWatch {
	#seenSequence: number;
	#pending:
		| { resolve: () => void; reject: (error: Error) => void }
		| undefined;
	#closed = false;

	constructor(
		private readonly notifier: StorageChangeNotifier,
		sequence: number,
	) {
		this.#seenSequence = sequence;
	}

	changed(): Promise<void> {
		if (this.#closed) return Promise.reject(closedWatchError());
		if (this.#pending) {
			return Promise.reject(
				new Error("changed() is already pending on this storage change watch"),
			);
		}
		const currentSequence = this.notifier.currentSequence();
		if (currentSequence !== this.#seenSequence) {
			this.#seenSequence = currentSequence;
			return Promise.resolve();
		}
		return new Promise<void>((resolve, reject) => {
			this.#pending = { resolve, reject };
		});
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.notifier.remove(this);
		this.#pending?.reject(closedWatchError());
		this.#pending = undefined;
	}

	notify(sequence: number): void {
		if (this.#closed || sequence === this.#seenSequence) return;
		const pending = this.#pending;
		if (!pending) return;
		this.#seenSequence = sequence;
		this.#pending = undefined;
		pending.resolve();
	}

	fail(error: Error): void {
		if (this.#closed) return;
		this.#closed = true;
		const pending = this.#pending;
		this.#pending = undefined;
		pending?.reject(error);
	}
}

function closedWatchError(): Error {
	const error = new Error("storage change watch is closed") as Error & {
		code: string;
	};
	error.name = "LixStorageError";
	error.code = "LIX_STORAGE_CLOSED";
	return error;
}
