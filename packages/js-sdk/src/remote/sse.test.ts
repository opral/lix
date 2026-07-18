import { expect, test } from "vitest";
import { readSseEvents } from "./sse.js";

test("parses SSE fields and ignores comments, IDs, and unknown fields", async () => {
	const events = await collect(
		streamFromText(
			": connected\n" +
				"id: invisible\n" +
				"event: next\n" +
				"retry: 1500\n" +
				"data: first line\n" +
				"data:second line\n" +
				"extension: ignored\n" +
				"\n" +
				": keepalive\n\n" +
				"data\n\n" +
				"event: custom\r\n" +
				"retry: nope\r\n" +
				"retry: -1\r\n" +
				"data: last\r\n\r\n",
		),
	);

	expect(events).toEqual([
		{
			event: "next",
			data: "first line\nsecond line",
			retry: 1500,
		},
		{ event: "message", data: "" },
		{ event: "custom", data: "last" },
	]);
});

test("handles CRLF, UTF-8, and every possible byte chunk boundary", async () => {
	const bytes = new TextEncoder().encode(
		"event: unicode\r\ndata: café 😀\r\ndata: tail\r\n\r\n",
	);
	const events = await collect(
		streamFromBytes(Array.from(bytes, (byte) => new Uint8Array([byte]))),
	);

	expect(events).toEqual([
		{ event: "unicode", data: "café 😀\ntail" },
	]);
});

test("discards an unterminated final event at EOF", async () => {
	const events = await collect(
		streamFromText(
			": comment\nid: ignored\nevent: final\ndata: one\ndata: two",
		),
	);

	expect(events).toEqual([]);
});

test("does not dispatch comments, keepalives, or field-only blocks", async () => {
	const events = await collect(
		streamFromText(
			": one\n\n: two\r\n\r\nid: ignored\n\nevent: unused\n\nretry: 20\n\n",
		),
	);

	expect(events).toEqual([]);
});

async function collect(
	stream: ReadableStream<Uint8Array>,
): Promise<Array<{ event: string; data: string; retry?: number }>> {
	const events = [];
	for await (const event of readSseEvents(stream)) events.push(event);
	return events;
}

function streamFromText(text: string): ReadableStream<Uint8Array> {
	return streamFromBytes([new TextEncoder().encode(text)]);
}

function streamFromBytes(
	chunks: Uint8Array[],
): ReadableStream<Uint8Array> {
	return new ReadableStream({
		start(controller) {
			for (const chunk of chunks) controller.enqueue(chunk);
			controller.close();
		},
	});
}
