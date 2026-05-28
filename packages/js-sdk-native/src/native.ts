import { fileURLToPath } from "node:url";

const native = { exports: {} as Record<string, any> };
process.dlopen(
	native,
	fileURLToPath(new URL("../lix_js_sdk_native.node", import.meta.url)),
);

export const addon = native.exports;
