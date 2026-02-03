#!/usr/bin/env node
import { promises as fs } from "node:fs";
import { dirname } from "node:path";

function chunk(values, size) {
  const result = [];
  for (let i = 0; i < values.length; i += size) {
    result.push(values.slice(i, i + size));
  }
  return result;
}

export async function embedWasm({ input, outJs, outDts, header }) {
  const wasmBytes = await fs.readFile(input);
  const numbers = Array.from(wasmBytes);
  const chunks = chunk(numbers, 20);
  const literal = chunks
    .map((group, index) => {
      const suffix = index === chunks.length - 1 ? "" : ",";
      return `\t${group.map((value) => value.toString()).join(", ")}${suffix}`;
    })
    .join("\n");

  const jsBody = `${header}export const wasmBinary = new Uint8Array([\n${literal}\n]).buffer;\n\nexport default wasmBinary;\n`;
  const dtsBody = `export declare const wasmBinary: ArrayBuffer;\nexport default wasmBinary;\n`;

  await fs.mkdir(dirname(outJs), { recursive: true });
  await fs.writeFile(outJs, jsBody);
  await fs.writeFile(outDts, dtsBody);
}
