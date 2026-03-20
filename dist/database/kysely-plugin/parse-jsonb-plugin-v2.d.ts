/**
 * THIS WAS A PROTOYPE FOR DETECTING JSONB COLUMNS
 * INSTEAD OF RELYING ON HEURISTICS BUT WAS TOO COMPLEX
 * FOR v1 of lix.
 *
 * Code still exists for future reference.
 *
 * https://github.com/opral/lix-sdk/issues/145
 */
import { type KyselyPlugin } from "kysely";
export declare function ParseJsonBPluginV2(jsonbColumns: Record<string, string[]>): KyselyPlugin;
//# sourceMappingURL=parse-jsonb-plugin-v2.d.ts.map