export type SseEvent = {
	event: string;
	data: string;
	retry?: number;
};

/**
 * Parses a fetch response body as a stream of server-sent events.
 *
 * The caller remains responsible for validating the response status and
 * content type before passing its body here.
 */
export async function* readSseEvents(
	stream: ReadableStream<Uint8Array>,
): AsyncGenerator<SseEvent, void, void> {
	const decoder = new TextDecoder();
	const reader = stream.getReader();
	let bufferedText = "";
	let eventName = "";
	let retry: number | undefined;
	let dataLines: string[] = [];

	function processLine(line: string): SseEvent | undefined {
		if (line.length === 0) {
			return dispatchEvent();
		}
		if (line.startsWith(":")) {
			return undefined;
		}

		const colonIndex = line.indexOf(":");
		const field = colonIndex === -1 ? line : line.slice(0, colonIndex);
		let value = colonIndex === -1 ? "" : line.slice(colonIndex + 1);
		if (value.startsWith(" ")) value = value.slice(1);

		switch (field) {
			case "event":
				eventName = value;
				break;
			case "data":
				dataLines.push(value);
				break;
			case "retry": {
				if (/^\d+$/.test(value)) {
					const parsed = Number(value);
					if (Number.isSafeInteger(parsed)) retry = parsed;
				}
				break;
			}
			// Event IDs and extension fields are intentionally ignored. Remote Lix
			// observations use their payload sequence for resumption instead.
			default:
				break;
		}
		return undefined;
	}

	function dispatchEvent(): SseEvent | undefined {
		const hasData = dataLines.length !== 0;
		const event: SseEvent | undefined = hasData
			? {
					event: eventName.length === 0 ? "message" : eventName,
					data: dataLines.join("\n"),
					...(retry === undefined ? {} : { retry }),
				}
			: undefined;
		eventName = "";
		retry = undefined;
		dataLines = [];
		return event;
	}

	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			bufferedText += decoder.decode(value, { stream: true });

			let newlineIndex = bufferedText.indexOf("\n");
			while (newlineIndex !== -1) {
				let line = bufferedText.slice(0, newlineIndex);
				bufferedText = bufferedText.slice(newlineIndex + 1);
				if (line.endsWith("\r")) line = line.slice(0, -1);
				const event = processLine(line);
				if (event !== undefined) yield event;
				newlineIndex = bufferedText.indexOf("\n");
			}
		}

		decoder.decode();
		// SSE dispatches only at a blank line. An incomplete final frame indicates
		// a truncated connection and is deliberately left for the caller to retry.
	} finally {
		reader.releaseLock();
	}
}
