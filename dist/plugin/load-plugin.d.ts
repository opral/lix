import type { LixDatabaseSchema } from "../database/schema.js";
import type { LixPlugin } from "./lix-plugin.js";
import { Kysely } from "kysely";
export declare function loadPlugins(db: Kysely<LixDatabaseSchema>): Promise<LixPlugin[]>;
//# sourceMappingURL=load-plugin.d.ts.map