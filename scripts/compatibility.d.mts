/** Canonical compatibility versions of this Lix source checkout. */
export function getCompatibility(): Readonly<{
  serverProtocolVersion: number;
  syncProtocolVersion: number;
  storageFormatVersion: number;
}>;
