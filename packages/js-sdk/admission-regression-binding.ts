import type { SyncServerBindingOptions } from "./src/binding-types.js";
const values = new Map<string, string>();
const session = (server: SyncServerBindingOptions) => ({
	activeBranchId: async () => "00000000-0000-7000-8000-000000000004",
	activeAccountId: async () => "00000000-0000-7000-8000-000000000003",
	openAnotherSession: async () => session(server),
	execute: async (sql: string) => {
		if (sql === "cancel") {
			const controller = new AbortController();
			const pending = server.transport!({
				url: server.url + "/probe",
				init: { signal: controller.signal },
				response: { mode: "buffered", maxBytes: 4096 },
			});
			controller.abort();
			await pending;
		}
		if (sql === "remote") {
			const response = await server.transport!({
				url: server.url + "/probe",
				init: {},
				response: { mode: "buffered", maxBytes: 4096 },
			});
			return {columns: [], rows: [[await response.text()]], rowsAffected: 0, notices: []};
		}
		if (sql.startsWith("write:")) values.set("value", sql.slice(6));
		return {
			columns: [],
			rows: [[values.get("value") ?? null]],
			rowsAffected: 0,
			notices: [],
		};
	},
	setTelemetryParent() {},
	close: async () => {},
});
export const openLixBinding = async (_storage: unknown, _telemetry: unknown, _parent: unknown, server: SyncServerBindingOptions) => session(server);
export const convertReplicaBinding = async () => {};
export const retryReplicaMigrationCleanupBinding = async () => {};
export const createHostedBinding = async () => {};
export const deleteHostedBinding = async () => {};
