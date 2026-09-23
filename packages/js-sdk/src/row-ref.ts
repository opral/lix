/** The address encoded by a canonical Lix row reference. */
export type RowRefParts = {
	relation: string;
	fileId: string | null;
	primaryKey: Array<{
		type: "uuid" | "integer" | "string" | "bytes";
		value: string;
	}>;
};

const prefix = "lix_row_ref:v2:";
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/**
 * Reads a row reference without querying Lix. The returned key values are
 * strings so signed 64-bit integer keys remain exact in JavaScript.
 *
 * The encoded representation remains an opaque identity: construct references
 * with `lix_row_ref(...)`, and use this helper only to inspect them.
 */
export function decodeRowRef(ref: string): RowRefParts {
	if (typeof ref !== "string" || !ref.startsWith(prefix)) {
		throw invalidRowRef("expected a canonical v2 row reference");
	}
	const payload = ref.slice(prefix.length);
	if (!/^[A-Za-z0-9_-]+$/.test(payload) || payload.length % 4 === 1) {
		throw invalidRowRef("invalid base64url payload");
	}
	let binary: string;
	try {
		binary = atob(payload.replace(/-/g, "+").replace(/_/g, "/"));
	} catch {
		throw invalidRowRef("invalid base64url payload");
	}
	// Reject alternate base64 spellings, including non-zero unused tail bits.
	if (btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "") !== payload) {
		throw invalidRowRef("noncanonical base64url payload");
	}
	const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
	const cursor = new Cursor(bytes);
	const relation = cursor.text(cursor.u32());
	if (!relation || relation.includes("\0")) {
		throw invalidRowRef("invalid relation");
	}
	if (relation === "lix_file_descriptor" || relation === "lix_directory_descriptor") {
		throw invalidRowRef("private filesystem relation");
	}
	const fileTag = cursor.u8();
	let fileId: string | null;
	if (fileTag === 0) {
		fileId = null;
	} else if (fileTag === 1) {
		fileId = cursor.text(cursor.u32());
		if (!fileId || fileId.includes("\0")) {
			throw invalidRowRef("invalid file ID");
		}
		if (relation === "lix_file" || relation === "lix_directory") {
			throw invalidRowRef("filesystem references cannot have file scope");
		}
	} else {
		throw invalidRowRef("unknown file scope tag");
	}
	const count = cursor.u16();
	if (count === 0) throw invalidRowRef("empty primary key");
	const primaryKey: RowRefParts["primaryKey"] = [];
	for (let index = 0; index < count; index++) {
		switch (cursor.u8()) {
			case 1: {
				const hex = Array.from(cursor.read(16), (byte) => byte.toString(16).padStart(2, "0")).join("");
				primaryKey.push({
					type: "uuid",
					value: `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`,
				});
				break;
			}
			case 2:
				primaryKey.push({ type: "integer", value: cursor.i64().toString() });
				break;
			case 3: {
				const value = cursor.text(cursor.u32());
				if (value.includes("\0")) throw invalidRowRef("NUL in text key");
				primaryKey.push({ type: "string", value });
				break;
			}
			case 4:
				primaryKey.push({ type: "bytes", value: btoa(cursor.binary(cursor.u32())) });
				break;
			default:
				throw invalidRowRef("unknown key component type");
		}
	}
	if (!cursor.done()) throw invalidRowRef("trailing bytes");
	return { relation, fileId, primaryKey };
}

class Cursor {
	private offset = 0;
	private readonly view: DataView;

	constructor(private readonly bytes: Uint8Array) {
		this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}

	read(length: number): Uint8Array {
		if (length > this.bytes.length - this.offset) throw invalidRowRef("truncated payload");
		const value = this.bytes.subarray(this.offset, this.offset + length);
		this.offset += length;
		return value;
	}

	u8(): number { return this.read(1)[0]; }
	u16(): number { const start = this.offset; this.read(2); return this.view.getUint16(start); }
	u32(): number { const start = this.offset; this.read(4); return this.view.getUint32(start); }
	i64(): bigint { const start = this.offset; this.read(8); return this.view.getBigInt64(start); }
	text(length: number): string {
		try { return utf8.decode(this.read(length)); }
		catch { throw invalidRowRef("invalid UTF-8"); }
	}
	binary(length: number): string {
		return Array.from(this.read(length), (byte) => String.fromCharCode(byte)).join("");
	}
	done(): boolean { return this.offset === this.bytes.length; }
}

function invalidRowRef(reason: string): TypeError {
	return new TypeError(`Invalid lix_row_ref: ${reason}`);
}
