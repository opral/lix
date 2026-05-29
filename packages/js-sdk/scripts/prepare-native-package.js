#!/usr/bin/env node
import { cp, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	nativePackageName,
	nativePlatformForCurrentProcess,
	nativePlatformForSuffix,
} from "./native-platforms.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageDir = join(__dirname, "..");
const binaryPath = join(packageDir, "lix_js_sdk.node");
const args = process.argv.slice(2);
const suffixArg = args.find((arg) => arg.startsWith("--suffix="))?.slice("--suffix=".length);
const outArg = args.find((arg) => arg.startsWith("--out="))?.slice("--out=".length);
const platform = suffixArg ? nativePlatformForSuffix(suffixArg) : nativePlatformForCurrentProcess();
const currentPlatform = nativePlatformForCurrentProcess();

if (!platform) {
	throw new Error(`Unsupported native package platform: ${suffixArg ?? `${process.platform}-${process.arch}`}`);
}
if (suffixArg && platform !== currentPlatform && process.env.LIX_NATIVE_PACKAGE_ALLOW_CROSS !== "1") {
	throw new Error(
		`Refusing to package ${process.platform}-${process.arch} binary as ${suffixArg}. ` +
			"Set LIX_NATIVE_PACKAGE_ALLOW_CROSS=1 only for intentional cross-compilation.",
	);
}

const sdkPackage = JSON.parse(await readFile(join(packageDir, "package.json"), "utf8"));
const outDir = outArg ? resolve(process.cwd(), outArg) : join(packageDir, "native-packages", platform.suffix);

await mkdir(outDir, { recursive: true });
await cp(binaryPath, join(outDir, "lix_js_sdk.node"));
await writeFile(
	join(outDir, "package.json"),
	`${JSON.stringify(
		{
			name: nativePackageName(platform.suffix),
			version: sdkPackage.version,
			description: `Native binary for @lix-js/sdk on ${platform.os}-${platform.cpu}.`,
			main: "./lix_js_sdk.node",
			os: [platform.os],
			cpu: [platform.cpu],
			files: ["lix_js_sdk.node"],
			publishConfig: {
				access: "public",
			},
		},
		null,
		"\t",
	)}\n`,
);

console.log(outDir);
