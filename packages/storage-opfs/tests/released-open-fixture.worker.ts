// Stage a trusted released-engine snapshot as physical OPFS entries, without
// invoking either the current engine or a migration API before normal opening.
// @ts-expect-error bundled direct entry
import { OpfsBackend } from "../dist/direct.js";
import type { OpfsBackend as Backend } from "../js/provider.js";
import type { LixStorageSpace } from "@lix-js/sdk";
self.onmessage = async ({ data }) => {
  let backend: Backend | undefined;
  try {
    const db: Backend = await OpfsBackend.open(data.name);
    backend = db;
    const bytes = new Uint8Array(data.snapshot);
    const view = new DataView(bytes.buffer);
    if (new TextDecoder().decode(bytes.subarray(0, 7)) !== "LIXSNAP") throw new Error("Invalid fixture");
    const sessionToken = await db.acquireSession();
    const write = await db.beginWrite({ sessionToken, awaitDurable: true, preconditions: [], batchCapacityHintBytes: bytes.length });
    let offset = 16;
    while (bytes[offset] === 1) {
      const id = view.getUint32(offset + 1);
      const keyLength = view.getUint32(offset + 5);
      const valueLength = view.getUint32(offset + 9);
      offset += 13;
      const key = bytes.slice(offset, offset + keyLength);
      offset += keyLength;
      const value = bytes.slice(offset, offset + valueLength);
      offset += valueLength;
      const space: LixStorageSpace = { id, name: `fixture.${id}`, valueSemantics: "mutable", valueIntegrity: "backendVerified" };
      await write.putMany(space, [{ key, value }]);
    }
    if (bytes[offset] !== 255) throw new Error("Truncated fixture");
    await write.commit();
    await db.close();
    backend = undefined;
    self.postMessage({ ok: true });
  } catch (error) { self.postMessage({ error: String(error) }); }
  finally { await backend?.close(); self.close(); }
};
