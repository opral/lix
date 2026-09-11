/** Package-private identity shared by the physical owner fence and SDK broker. */
export function physicalName(name: string): string {
	return Array.from(new TextEncoder().encode(name), (byte) =>
		byte.toString(16).padStart(2, "0"),
	).join("");
}
