import { afterEach, expect, test, vi } from "vitest";
import { RepositorySession } from "./repository-session.js";
import type {
	WorkerInput,
	WorkerOperation,
	WorkerRequest,
	WorkerResponse,
} from "./protocol.js";
const instances: RepositorySession[] = [];
afterEach(() => {
	for (const s of instances.splice(0)) s.close();
	vi.useRealTimers();
});
const tick = async () => {
	for (let i = 0; i < 20; i++) await Promise.resolve();
};
function fixture() {
	const sent: WorkerInput[] = [],
		output: WorkerResponse[] = [];
	const fatal = vi.fn();
	let auto = true,
		resource = 10,
		id = 1;
	const context = { branchId: "branch-acknowledged", accountId: "account" };
	const session = new RepositorySession(
		(message) => {
			sent.push(message);
			if (
				!auto ||
				!("id" in message) ||
				message.operation.kind === "observe.next"
			)
				return;
			queueMicrotask(() =>
				session.receive({
					id: message.id,
					ok: true,
					value: [
						"openAnotherSession",
						"observe",
						"beginTransaction",
						"exportSnapshot",
					].includes(message.operation.kind)
						? resource++
						: undefined,
					context: ["open", "openAnotherSession", "switchBranch"].includes(
						message.operation.kind,
					)
						? context
						: undefined,
				}),
			);
		},
		(message) => output.push(message),
		fatal,
	);
	instances.push(session);
	return {
		session,
		sent,
		output,
		fatal,
		context,
		pause: () => (auto = false),
		resume: () => (auto = true),
		request(operation: WorkerOperation, sessionId = 0) {
			const request = { id: id++, sessionId, operation };
			session.post(request);
			return request.id;
		},
		result(id: number) {
			return output.find((m) => "id" in m && m.id === id) as Extract<
				WorkerResponse,
				{ ok: true }
			>;
		},
		async open() {
			session.connected();
			await tick();
			this.request({
				kind: "open",
				storage: { kind: "memory" },
				telemetryEnabled: false,
				progressEnabled: false,
			});
			await tick();
		},
	};
}
test("recovers acknowledged contexts and observations without replaying writes or transactions", async () => {
	const f = fixture();
	await f.open();
	const child = f.request({ kind: "openAnotherSession", options: {} });
	await tick();
	const sessionId = f.result(child).value as number;
	const watch = f.request(
		{ kind: "observe", sql: "SELECT 1", params: [] },
		sessionId,
	);
	const tx = f.request({ kind: "beginTransaction" });
	await tick();
	const observeId = f.result(watch).value as number,
		transactionId = f.result(tx).value as number;
	f.request({ kind: "close" });
	await tick(); // A child outlives its original parent.
	const next = f.request({ kind: "observe.next", observeId }, sessionId);
	f.pause();
	const write = f.request(
		{ kind: "execute", sql: "INSERT INTO x VALUES (1)", params: [] },
		sessionId,
	);
	const commit = f.request(
		{ kind: "transaction.commit", transactionId },
		sessionId,
	);
	f.session.lost();
	expect(f.result(write)).toMatchObject({
		ok: false,
		error: { code: "LIX_WRITE_OUTCOME_UNKNOWN" },
	});
	expect(f.result(commit)).toMatchObject({
		ok: false,
		error: { code: "LIX_WRITE_OUTCOME_UNKNOWN" },
	});
	expect(f.result(next)).toBeUndefined();
	const previous = f.sent.length;
	f.resume();
	f.session.connected();
	await tick();
	const restored = f.sent
		.slice(previous)
		.filter((m): m is WorkerRequest => "id" in m);
	expect(restored.map((m) => m.operation.kind)).toEqual([
		"open",
		"openAnotherSession",
		"close",
		"observe",
		"observe.next",
	]);
	expect(restored[1].operation).toEqual({
		kind: "openAnotherSession",
		options: f.context,
	});
	f.session.receive({
		id: restored.at(-1)!.id,
		ok: true,
		value: { done: false, value: "fresh snapshot" },
	});
	expect(f.result(next)).toMatchObject({
		ok: true,
		value: { value: "fresh snapshot" },
	});
	const staleTx = f.request(
		{ kind: "transaction.execute", transactionId, sql: "SELECT 1", params: [] },
		sessionId,
	);
	expect(f.result(staleTx)).toMatchObject({
		ok: false,
		error: { code: "LIX_TRANSACTION_LOST" },
	});
	expect(f.fatal).not.toHaveBeenCalled();
});
test("fences stale credential callbacks across owner generations", async () => {
	const f = fixture();
	await f.open();
	f.session.receive({ kind: "sync.headers", requestId: 1 });
	const old = f.output.at(-1) as { requestId: number };
	f.session.lost();
	f.session.connected();
	await tick();
	f.session.receive({ kind: "sync.headers", requestId: 1 });
	const fresh = f.output.at(-1) as { requestId: number };
	expect(fresh.requestId).not.toBe(old.requestId);
	const count = f.sent.length;
	f.session.post({
		kind: "sync.headers.result",
		requestId: old.requestId,
		result: { ok: true, headers: [] },
	});
	expect(f.sent).toHaveLength(count);
	f.session.post({
		kind: "sync.headers.result",
		requestId: fresh.requestId,
		result: { ok: true, headers: [] },
	});
	expect(f.sent.at(-1)).toMatchObject({
		kind: "sync.headers.result",
		requestId: 1,
	});
});
test("bounds recovery even with only an idle observation pending", async () => {
	vi.useFakeTimers();
	const f = fixture();
	await f.open();
	f.pause();
	f.session.lost();
	f.session.connected();
	await vi.advanceTimersByTimeAsync(30000);
	expect(f.fatal).toHaveBeenCalledWith(
		expect.objectContaining({ code: "LIX_RECOVERY_TIMEOUT" }),
	);
});
test("standalone replica conversion does not require an open session", async () => {
	const f = fixture();
	f.session.connected();
	await tick();
	const id = f.request({
		kind: "replica.convert",
		storage: { kind: "memory" },
		server: { url: "https://example.test", dynamicHeaders: false },
	});
	await tick();
	expect(f.result(id)).toMatchObject({ ok: true });
	expect(f.sent).toEqual([
		expect.objectContaining({
			operation: expect.objectContaining({ kind: "replica.convert" }),
		}),
	]);
});
