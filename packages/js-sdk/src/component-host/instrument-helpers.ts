/** Shared, dependency-free metadata used by both host instrumentation paths. */
export const TICK_MODULE = "lix:runtime/deadline";

/** Count defined memories to apportion the component's aggregate ceiling. */
export function coreMemoryCount(bytes: Uint8Array): number {
  return coreSectionCount(bytes, 5);
}

export function coreTableCount(bytes: Uint8Array): number {
  return coreSectionCount(bytes, 4);
}

function coreSectionCount(bytes: Uint8Array, section: number): number {
  let offset = 8;
  const read = () => {
    let value = 0;
    for (let shift = 0; shift < 35; shift += 7) {
      const byte = bytes[offset++];
      if (byte === undefined) throw new Error("Truncated Wasm integer");
      value += (byte & 127) * 2 ** shift;
      if (!(byte & 128)) return value;
    }
    throw new Error("Invalid Wasm integer");
  };
  while (offset < bytes.length) {
    const id = bytes[offset++];
    const size = read();
    if (id === section) return read();
    offset += size;
  }
  return 0;
}
