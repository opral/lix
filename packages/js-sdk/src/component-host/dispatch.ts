import { compileComponent } from "./index.js";
import type { ComponentInstance, ComponentLimits } from "./index.js";

export type ComponentRequest = {
  operation: string;
  data: string;
  bytes?: Uint8Array;
  host?: { call(method: string, argumentsJson: string): string; free?(): void };
};
export type ComponentDispatch = (request: ComponentRequest) => Promise<string>;

type Scope = { host: NonNullable<ComponentRequest["host"]>; active: boolean };
type Factory = Awaited<ReturnType<typeof compileComponent>>;
type Guest = {
  instance: ComponentInstance;
  resources: ReturnType<typeof resourceTypes>;
  busy: boolean;
};

/** One isolated handle registry per engine runtime, shared by Node and browsers. */
export function createComponentDispatch(): (
  request: ComponentRequest,
) => Promise<string> {
  const factories = new Map<number, Factory>();
  const guests = new Map<number, Guest>();
  const pending = new Map<
    string,
    { canceled: boolean; completed: boolean; dispose?: () => void }
  >();
  const begin = (requestId: unknown) => {
    if (requestId == null) return undefined;
    const key = String(requestId);
    let entry = pending.get(key);
    if (!entry) {
      entry = { canceled: false, completed: false };
      pending.set(key, entry);
    }
    return { key, entry };
  };
  const complete = (token: ReturnType<typeof begin>, dispose: () => void) => {
    if (!token) return;
    token.entry.completed = true;
    token.entry.dispose = dispose;
    if (token.entry.canceled) {
      dispose();
      pending.delete(token.key);
    }
  };
  let sequence = 0;
  const allocate = () => {
    if (++sequence > Number.MAX_SAFE_INTEGER)
      throw new Error("Component handle space exhausted");
    return sequence;
  };
  return async (request) => {
    const data = JSON.parse(request.data);
    switch (request.operation) {
      case "compile": {
        const token = begin(data.requestId);
        try {
          if (!(request.bytes instanceof Uint8Array))
            throw new Error("Missing component bytes");
          const factory = await compileComponent(
            request.bytes,
            data.limits as ComponentLimits,
          );
          const id = allocate();
          factories.set(id, factory);
          complete(token, () => {
            factories.delete(id);
          });
          return JSON.stringify({ id });
        } catch (error) {
          complete(token, () => {});
          throw error;
        }
      }
      case "instantiate": {
        const token = begin(data.requestId);
        try {
          const factory = factories.get(data.id);
          if (!factory) throw new Error("Unknown component factory");
          const resources = resourceTypes();
          const instance = await factory.instantiate({
            ...wasiImports(),
            "lix:plugin-v2/host": resources,
            // Previously published v2 components retain their original identity.
            "lix:plugin/host": resources,
          });
          const id = allocate();
          guests.set(id, { instance, resources, busy: false });
          complete(token, () => {
            guests.delete(id);
          });
          return JSON.stringify({ id });
        } catch (error) {
          complete(token, () => {});
          throw error;
        }
      }
      case "invoke": {
        const guest = guests.get(data.id);
        if (!guest || guest.busy)
          throw new Error("Unknown or busy component guest");
        if (!request.host) throw new Error("Missing component host");
        guest.busy = true;
        const scope: Scope = { host: request.host, active: true };
        try {
          guest.instance.setDeadline(Number(data.limits.timeoutMs ?? 5000));
          const { input, output } = data.input;
          const r = guest.resources;
          const wrap = (kind: keyof typeof r, id: number) =>
            new r[kind](scope, id);
          let argument: any;
          let sink: any;
          let iface: any;
          if (data.operation === "merge") {
            argument = wrap("ColumnMergeSource", input);
            sink = wrap("ColumnMergeSink", output);
            iface = guest.instance.exports.columnMerger;
          } else {
            argument = decode(input);
            sink = wrap("Transition", output);
            iface = guest.instance.exports.fileProjection;
            switch (data.operation) {
              case "parse":
                argument.file = wrap("Snapshot", input.file);
                break;
              case "parseChanges":
                argument.before = wrap("Snapshot", input.before);
                argument.rows =
                  input.rows == null
                    ? undefined
                    : wrap("RowSource", input.rows);
                break;
              case "serialize":
                argument.rows = wrap("RowSource", input.rows);
                argument.before =
                  input.before == null
                    ? undefined
                    : wrap("Snapshot", input.before);
                break;
              case "serializeChanges":
                argument.before = wrap("Snapshot", input.before);
                argument.rowChanges = wrap("RowSource", input.rowChanges);
                break;
              default:
                throw new Error(
                  `Unknown component operation ${data.operation}`,
                );
            }
          }
          const fn = iface?.[data.operation];
          if (typeof fn !== "function")
            throw new Error(`Component does not export ${data.operation}`);
          await fn(argument, sink);
          const memory = JSON.parse(
            scope.host.call(
              "runtime.memoryHighWater",
              JSON.stringify({ bytes: String(guest.instance.memoryBytes()) }),
            ),
          );
          if (memory.error) throw memory.error;
          return "null";
        } catch (error) {
          const payload = (
            error as { payload?: { tag?: string; val?: unknown } }
          )?.payload;
          if (
            payload?.tag &&
            ["invalid-input", "limit-exceeded", "internal"].includes(
              payload.tag,
            )
          )
            return JSON.stringify({ pluginError: payload });
          throw error;
        } finally {
          scope.active = false;
          request.host.free?.();
          guest.busy = false;
        }
      }
      case "cancelRequest": {
        const token = begin(data.requestId ?? data.id);
        if (token) {
          token.entry.canceled = true;
          token.entry.dispose?.();
          if (token.entry.completed) pending.delete(token.key);
        }
        return "null";
      }
      case "finishRequest":
        pending.delete(String(data.requestId ?? data.id));
        return "null";
      case "disposeFactory":
        factories.delete(data.id);
        return "null";
      case "disposeGuest":
        guests.delete(data.id);
        return "null";
      default:
        throw new Error(
          `Unknown component dispatch operation ${request.operation}`,
        );
    }
  };
}

const u64Fields = new Set([
  "high",
  "offset",
  "deleteLen",
  "totalLen",
  "totalLength",
  "baseLen",
  "aLen",
  "bLen",
  "baseRowLen",
  "aRowLen",
  "bRowLen",
]);
const byteFields = new Set(["bytes", "payload", "schemaFingerprint", "insert"]);
function decode(value: any, key = ""): any {
  if (value == null) return undefined;
  if (u64Fields.has(key)) return BigInt(value);
  if (byteFields.has(key)) return Uint8Array.from(value);
  if (key === "primaryKey" || key === "attachments")
    return value.map((bytes: number[]) => Uint8Array.from(bytes));
  if (Array.isArray(value)) return value.map((item) => decode(item));
  if (typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([k, v]) => [k, decode(v, k)]),
    );
  return value;
}
function encode(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Uint8Array) return Array.from(value);
  return value;
}
function resourceTypes() {
  class Resource {
    constructor(
      readonly scope: Scope,
      readonly id: number,
    ) {}
    invoke(
      type: string,
      method: string,
      names: string[],
      values: unknown[],
    ): any {
      if (!this.scope.active)
        throw new Error("Component resource invocation has ended");
      const args = {
        resource: this.id,
        ...Object.fromEntries(names.map((name, i) => [name, values[i]])),
      };
      const result = JSON.parse(
        this.scope.host.call(`${type}.${method}`, JSON.stringify(args, encode)),
      );
      if (result.error) throw result.error;
      if (method === "fileLen") return BigInt(result.ok);
      if (
        method === "readFile" ||
        method === "readRow" ||
        method === "readValue"
      )
        return result.ok == null ? undefined : Uint8Array.from(result.ok);
      return decode(result.ok);
    }
  }
  const define = (type: string, methods: Record<string, string[]>) => {
    class Handle extends Resource {}
    for (const [method, names] of Object.entries(methods))
      Object.defineProperty(Handle.prototype, method, {
        value: function (this: Resource, ...values: unknown[]) {
          return this.invoke(type, method, names, values);
        },
      });
    Object.defineProperty(
      Handle.prototype,
      Symbol.dispose || Symbol.for("dispose"),
      {
        value: function (this: Resource) {
          if (this.scope.active) this.invoke(type, "drop", [], []);
        },
      },
    );
    return Handle;
  };
  return {
    Snapshot: define("snapshot", {
      fileLen: [],
      readFile: ["offset", "length"],
      readState: ["key", "offset", "maxBytes"],
    }),
    RowSource: define("rowSource", { nextPage: ["maxBytes"] }),
    Transition: define("transition", {
      maxBatchBytes: [],
      putState: ["key", "value"],
      deleteState: ["key"],
      deleteStatePrefix: ["prefix"],
      emitRows: ["page"],
      replaceAllRows: [],
      emitFileEdit: ["edit"],
      beginFileReplacement: ["totalLength"],
      writeFileReplacement: ["chunk"],
      finishFileReplacement: [],
    }),
    ColumnMergeSource: define("columnMergeSource", {
      len: [],
      get: ["index"],
      readValue: ["index", "side", "offset", "length"],
      readRow: ["index", "side", "offset", "length"],
    }),
    ColumnMergeSink: define("columnMergeSink", {
      maxBatchBytes: [],
      useLww: ["ordinal"],
      beginReplace: ["ordinal", "totalLength"],
      writeReplacement: ["chunk"],
      finishReplace: [],
    }),
  };
}

/** Closed input, discarded output, and no ambient filesystem/network grants. */
function wasiImports(): Record<string, unknown> {
  class Pollable {
    ready() {
      return true;
    }
    block() {}
  }
  class InputStream {
    read() {
      throw { tag: "closed" };
    }
    blockingRead() {
      return this.read();
    }
    subscribe() {
      return new Pollable();
    }
  }
  class OutputStream {
    checkWrite() {
      return 65536n;
    }
    write() {}
    flush() {}
    blockingFlush() {}
    blockingWriteAndFlush() {}
    subscribe() {
      return new Pollable();
    }
  }
  class TerminalInput {}
  class TerminalOutput {}
  class IoError {
    toDebugString() {
      return "Component stream error";
    }
  }
  return {
    "wasi:cli/environment": {
      getEnvironment: () => [],
      getArguments: () => [],
      initialCwd: () => undefined,
    },
    "wasi:cli/exit": {
      exit() {
        throw new Error("Component requested process exit");
      },
    },
    "wasi:cli/stdin": { getStdin: () => new InputStream() },
    "wasi:cli/stdout": { getStdout: () => new OutputStream() },
    "wasi:cli/stderr": { getStderr: () => new OutputStream() },
    "wasi:cli/terminal-input": { TerminalInput },
    "wasi:cli/terminal-output": { TerminalOutput },
    "wasi:cli/terminal-stdin": { getTerminalStdin: () => undefined },
    "wasi:cli/terminal-stdout": { getTerminalStdout: () => undefined },
    "wasi:cli/terminal-stderr": { getTerminalStderr: () => undefined },
    "wasi:clocks/monotonic-clock": {
      now: () => BigInt(Math.floor(performance.now() * 1e6)),
      resolution: () => 1_000_000n,
      subscribeDuration: () => new Pollable(),
      subscribeInstant: () => new Pollable(),
    },
    "wasi:io/error": { Error: IoError },
    "wasi:io/poll": {
      Pollable,
      poll: (items: unknown[]) => Uint32Array.from(items.map((_, i) => i)),
    },
    "wasi:io/streams": { InputStream, OutputStream },
    "wasi:random/insecure-seed": { insecureSeed: () => [0n, 0n] },
  };
}
