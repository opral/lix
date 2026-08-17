import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

const syncTestState = {
	branchId: "",
	headCommitId: "",
	longPollStarts: 0,
	activeLongPolls: 0,
	abortedLongPolls: 0,
	scopedPullResponsesRemaining: 0,
};

type SyncTestRequest = {
	url?: string;
	method?: string;
	[Symbol.asyncIterator](): AsyncIterator<unknown>;
};

type SyncTestResponse = {
	statusCode: number;
	writableEnded: boolean;
	setHeader(name: string, value: string): void;
	end(body?: string): void;
	on(event: "close", listener: () => void): void;
};

function respondJson(response: SyncTestResponse, value: unknown): void {
	response.statusCode = 200;
	response.setHeader("content-type", "application/json");
	response.end(JSON.stringify(value));
}

async function readJson(
	request: SyncTestRequest,
): Promise<Record<string, string>> {
	let body = "";
	for await (const chunk of request) body += String(chunk);
	return JSON.parse(body) as Record<string, string>;
}

export default defineConfig({
	plugins: [
		{
			name: "opfs-sync-long-poll-test-server",
			configureServer(server) {
				server.middlewares.use(async (request, response, next) => {
					const testRequest = request as unknown as SyncTestRequest;
					const testResponse = response as unknown as SyncTestResponse;
					const url = new URL(testRequest.url ?? "/", "http://localhost");
					if (url.pathname === "/__lix_sync_test/config") {
						const config = await readJson(testRequest);
						Object.assign(syncTestState, {
							branchId: config.branchId,
							headCommitId: config.headCommitId,
							longPollStarts: 0,
							activeLongPolls: 0,
							abortedLongPolls: 0,
							scopedPullResponsesRemaining: 1,
						});
						respondJson(testResponse, { ok: true });
						return;
					}
					if (url.pathname === "/__lix_sync_test/state") {
						respondJson(testResponse, syncTestState);
						return;
					}
					if (url.pathname === "/__lix_sync_test/repository/lix/v1") {
						respondJson(testResponse, {
							activeBranchId: syncTestState.branchId,
							sessionId: "browser-long-poll-test",
						});
						return;
					}
					if (
						url.pathname ===
						"/__lix_sync_test/repository/lix/v1/sync/branches"
					) {
						respondJson(testResponse, []);
						return;
					}
					if (
						url.pathname ===
						"/__lix_sync_test/repository/lix/v1/sync/pull"
					) {
						const pull = {
							branchId: syncTestState.branchId,
							events: [],
							nextCursor: 0,
							headCursor: 0,
							headCommitId: syncTestState.headCommitId,
						};
						const finite = url.searchParams.get("limit") === "0";
						const schemas = url.searchParams.get("schemas") ?? "";
						const requestedColdScope = schemas.includes("lix_key_value");
						if (
							finite ||
							(requestedColdScope && syncTestState.scopedPullResponsesRemaining > 0)
						) {
							if (requestedColdScope) syncTestState.scopedPullResponsesRemaining -= 1;
							respondJson(testResponse, pull);
							return;
						}
						syncTestState.longPollStarts += 1;
						syncTestState.activeLongPolls += 1;
						testResponse.on("close", () => {
							syncTestState.activeLongPolls -= 1;
							if (!testResponse.writableEnded) syncTestState.abortedLongPolls += 1;
						});
						return;
					}
					if (
						url.pathname === "/__lix_sync_test/repository/lix/v1/session" &&
						testRequest.method === "DELETE"
					) {
						testResponse.statusCode = 204;
						testResponse.end();
						return;
					}
					next();
				});
			},
		},
	],
	server: {
		fs: {
			// The provider's peer SDK is a sibling package during workspace tests.
			allow: [new URL("..", import.meta.url).pathname],
		},
	},
	test: {
		include: ["tests/**/*.browser.test.ts"],
		browser: {
			enabled: true,
			headless: true,
			provider: playwright(),
			instances: [{ browser: "chromium" }],
		},
	},
});
