import { expect,test,vi } from "vitest";
const worker=vi.hoisted(()=>vi.fn(async()=>{}));
vi.mock("./worker/client.js",()=>({convertReplicaWorkerOperation:worker}));
import { convertReplicaToPartial } from "./open-lix.js";

test("conversion forwards provider configuration without opening full storage",async()=>{
 const options={name:"closed-provider"};
 const storage={lixStorage:{version:3 as const,moduleUrl:"https://example.test/provider.js",options}};
 const fetcher=vi.fn();
 const server={url:"https://example.test/lix/id",headers:async()=>({authorization:"Bearer test"}),fetch:fetcher};
 await convertReplicaToPartial({storage,server,branchId:"selected"});
 expect(worker).toHaveBeenLastCalledWith({kind:"jsStorage",moduleUrl:storage.lixStorage.moduleUrl,options},server,"selected");
});
test("failed conversion releases same-object reservation for exact retry",async()=>{
 const storage={lixStorage:{version:3 as const,moduleUrl:"https://example.test/provider.js",options:{}}};
 const options={storage,server:{url:"https://example.test/lix/id"}};
 worker.mockRejectedValueOnce(Object.assign(new Error("pending edits"),{code:"LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED"}));
 await expect(convertReplicaToPartial(options)).rejects.toMatchObject({code:"LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED"});
 await expect(convertReplicaToPartial(options)).resolves.toBeUndefined();
});
