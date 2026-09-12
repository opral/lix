import { $init, generate } from "@bytecodealliance/jco-transpile/component";
import {
  coreMemoryCount,
  coreTableCount,
  instrumentCore,
  TICK_MODULE,
} from "./instrument.js";

export type ComponentImports = Record<string, unknown>;
export type ComponentLimits = {
  maxMemoryBytes: string;
  maxFuel?: string | null;
  timeoutMs?: string | null;
};
export type ComponentInstance = {
  exports: Record<string, any>;
  memoryBytes(): number;
  setDeadline(milliseconds: number): void;
};

/** Compile once, instantiate separately for every engine actor. */
export async function compileComponent(
  bytes: Uint8Array,
  limits: ComponentLimits,
): Promise<{
  instantiate(imports: ComponentImports): Promise<ComponentInstance>;
}> {
  if (limits.maxFuel != null)
    throw new Error(
      "Instruction fuel limits are unsupported by the JavaScript Component host",
    );
  await $init;
  const output = generate(bytes, {
    name: "plugin",
    instantiation: { tag: "async" },
    noTypescript: true,
    noNodejsCompat: true,
    base64Cutoff: 0,
  });

  const cores = output.files.filter(([name]) => name.endsWith(".wasm"));
  const memoryCount = cores.reduce(
    (total, [, core]) => total + coreMemoryCount(core),
    0,
  );
  if (memoryCount > 1)
    throw new Error("Multiple component memories are unsupported");
  if (cores.reduce((total, [, core]) => total + coreTableCount(core), 0) > 8)
    throw new Error("Component table count exceeds limit");
  const modules = new Map<string, WebAssembly.Module>();
  const tableCounts = new Map<WebAssembly.Module, number>();
  for (const [name, core] of cores) {
    const compiled = await WebAssembly.compile(
      instrumentCore(
        core,
        Math.floor(Number(limits.maxMemoryBytes) / Math.max(1, memoryCount)),
      ) as Uint8Array<ArrayBuffer>,
    );
    if (
      WebAssembly.Module.imports(compiled).some(
        (item) => item.kind === "memory",
      )
    )
      throw new Error("Imported component memories are unsupported");
    modules.set(name, compiled);
    tableCounts.set(compiled, coreTableCount(core));
  }
  const js = output.files.find(([name]) => name === "plugin.js")?.[1];
  if (!js)
    throw new Error("Component transpiler did not emit JavaScript bindings");
  const url = `data:text/javascript;charset=utf-8,${encodeURIComponent(new TextDecoder().decode(js))}`;
  const binding = await import(/* @vite-ignore */ url);
  return {
    async instantiate(imports) {
      const memories: WebAssembly.Memory[] = [];
      let tableCount = 0;
      let deadline = performance.now() + Number(limits.timeoutMs ?? 5000);
      const exports = await binding.instantiate(
        (name: string) => {
          const module = modules.get(name);
          if (!module) throw new Error(`Unknown component core module ${name}`);
          return module;
        },
        imports,
        (module: WebAssembly.Module, coreImports: WebAssembly.Imports) => {
          if (
            memories.length &&
            WebAssembly.Module.exports(module).some(
              (item) => item.name === "__lix_runtime_memory",
            )
          )
            throw new Error(
              "Multiple component memory instances are unsupported",
            );
          tableCount += tableCounts.get(module) ?? 0;
          if (tableCount > 8)
            throw new Error("Component table instance count exceeds limit");
          const instance = new WebAssembly.Instance(module, {
            ...coreImports,
            [TICK_MODULE]: {
              tick() {
                if (performance.now() >= deadline)
                  throw new Error("Component execution deadline exceeded");
              },
            },
          });
          const memory = instance.exports.__lix_runtime_memory;
          if (memory instanceof WebAssembly.Memory) memories.push(memory);
          return instance;
        },
      );
      return {
        exports,
        memoryBytes: () =>
          memories.reduce(
            (total, memory) => total + memory.buffer.byteLength,
            0,
          ),
        setDeadline(milliseconds) {
          if (!Number.isFinite(milliseconds) || milliseconds <= 0)
            throw new Error("Invalid component deadline");
          deadline =
            performance.now() +
            Math.min(milliseconds, Number(limits.timeoutMs ?? milliseconds));
        },
      };
    },
  };
}
