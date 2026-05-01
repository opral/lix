declare module "better-sqlite3" {
	export type DatabaseOptions = {
		readonly?: boolean;
		fileMustExist?: boolean;
		timeout?: number;
		verbose?: (message?: unknown, ...additional: unknown[]) => void;
	};

	export type Statement = {
		get(...params: unknown[]): unknown;
		all(...params: unknown[]): unknown[];
		run(...params: unknown[]): unknown;
	};

	export type Database = {
		readonly inTransaction: boolean;
		exec(sql: string): Database;
		prepare(sql: string): Statement;
		pragma(source: string, options?: unknown): unknown;
		close(): void;
	};

	type DatabaseConstructor = {
		new (filename: string, options?: DatabaseOptions): Database;
		(filename: string, options?: DatabaseOptions): Database;
	};

	const Database: DatabaseConstructor;
	export default Database;
}
