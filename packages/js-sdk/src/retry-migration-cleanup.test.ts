import {expect,test,vi} from "vitest";
const cleanup=vi.hoisted(()=>vi.fn<(...args:unknown[])=>Promise<number>>());
vi.mock("./worker/client.js",()=>({retryReplicaMigrationCleanupWorkerOperation:cleanup}));
import {retryReplicaMigrationCleanup,convertReplicaToPartial} from "./open-lix.js";
function fixture(){return {storage:{lixStorage:{version:3 as const,moduleUrl:"https://example.test/provider.js",options:{name:"retained"}}},server:{url:"https://example.test/lix/id",headers:async()=>({authorization:"Bearer fresh"}),fetch:vi.fn()}};}
test("cleanup forwards dynamic authentication and returns completed count",async()=>{
 const options=fixture();cleanup.mockResolvedValueOnce(2);
 await expect(retryReplicaMigrationCleanup(options)).resolves.toBe(2);
 expect(cleanup).toHaveBeenLastCalledWith({kind:"jsStorage",moduleUrl:options.storage.lixStorage.moduleUrl,options:options.storage.lixStorage.options},options.server);
});
test("lost cleanup response releases closed-storage reservation for retry",async()=>{
 const options=fixture();cleanup.mockRejectedValueOnce(new Error("lost response")).mockResolvedValueOnce(1);
 await expect(retryReplicaMigrationCleanup(options)).rejects.toThrow("lost response");
 await expect(retryReplicaMigrationCleanup(options)).resolves.toBe(1);
});
test("in-flight cleanup excludes cleanup and conversion on the same storage",async()=>{
 const options=fixture();let finish!:(value:number)=>void;
 cleanup.mockImplementationOnce(()=>new Promise<number>(resolve=>{finish=resolve;}));
 const pending=retryReplicaMigrationCleanup(options);
 await vi.waitFor(()=>expect(finish).toBeTypeOf("function"));
 await expect(retryReplicaMigrationCleanup(options)).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"});
 await expect(convertReplicaToPartial(options)).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"});
 finish(0);await expect(pending).resolves.toBe(0);
 cleanup.mockResolvedValueOnce(0);await expect(retryReplicaMigrationCleanup(options)).resolves.toBe(0);
});
