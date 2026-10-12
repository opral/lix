import { expect, test } from "vitest";
import type {
	ExecuteManyOptions,
	ExecuteOptions,
	Lix,
	QueryOptions,
	Reader,
	RowStream,
} from "./index.js";

interface CustomRow {
	value: string;
}

test("query, stream and execute accept interface rows and forwarded option unions", () => {
	const typecheck = (
		lix: Lix,
		executeOptions: ExecuteOptions,
		manyOptions: ExecuteManyOptions,
		queryOptions: QueryOptions,
	) => {
		const rows: Promise<CustomRow[]> = lix.query<CustomRow>("SELECT 'ok' AS value");
		const one: Promise<CustomRow | undefined> = lix.queryOne<CustomRow>("SELECT 'ok' AS value");
		const stream: Promise<RowStream<CustomRow>> = lix.stream<CustomRow>("SELECT 'ok' AS value");
		void rows;
		void one;
		void stream;
		void lix.query("SELECT 1", [], queryOptions);
		void lix.execute<CustomRow>("SELECT 'ok' AS value");
		void lix.execute("SELECT 1", [], executeOptions);
		void lix.executeMany("INSERT INTO t VALUES ($1)", [[1]], manyOptions);
		const forwardExecute = (options?: ExecuteOptions) =>
			lix.execute("SELECT 1", [], options);
		void lix.transaction(async (transaction) => {
			const reader: Reader = transaction;
			void reader.query("SELECT 1");
			void transaction.execute("SELECT 1", [], executeOptions);
		});
		void forwardExecute;
	};

	expect(typecheck).toBeTypeOf("function");
});
