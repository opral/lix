import { expect, test } from "vitest";
import type { ExecuteOptions, Lix, LixBatchOptions } from "./index.js";

interface CustomRow {
	value: string;
}

test("execute overloads accept interface rows and forwarded option unions", () => {
	const typecheck = (
		lix: Lix,
		executeOptions: ExecuteOptions,
		batchOptions: LixBatchOptions,
	) => {
		void lix.execute<CustomRow>("SELECT 'ok' AS value");
		void lix.execute("SELECT 1", [], executeOptions);
		void lix.executeBatch([{ sql: "SELECT 1" }], batchOptions);
		const forwardExecute = (options?: ExecuteOptions) =>
			lix.execute("SELECT 1", [], options);
		const forwardBatch = (options?: LixBatchOptions) =>
			lix.executeBatch([{ sql: "SELECT 1" }], options);
		void lix.transaction(async (transaction) => {
			void transaction.execute("SELECT 1", [], executeOptions);
		});
		void forwardExecute;
		void forwardBatch;
	};

	expect(typecheck).toBeTypeOf("function");
});
