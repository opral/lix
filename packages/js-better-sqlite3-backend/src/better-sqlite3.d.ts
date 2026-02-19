declare module "better-sqlite3" {
  interface BetterSqlite3Statement {
    reader: boolean;
    run(...params: readonly unknown[]): this;
    raw(rawMode: true): { all(...params: readonly unknown[]): readonly unknown[][] };
    raw(rawMode: false): {
      run(...params: readonly unknown[]): this;
      all(...params: readonly unknown[]): readonly unknown[];
    };
    raw(rawMode: boolean): {
      all(...params: readonly unknown[]): readonly unknown[][] | readonly unknown[];
    };
  }

  interface BetterSqlite3Database {
    prepare(sql: string): BetterSqlite3Statement;
    exec(sql: string): this;
    close(): void;
    serialize(): Uint8Array;
  }

  class Database implements BetterSqlite3Database {
    constructor(filename?: string, options?: Record<string, unknown>);
    prepare(sql: string): BetterSqlite3Statement;
    exec(sql: string): this;
    close(): void;
    serialize(): Uint8Array;
  }

  export = Database;
}
