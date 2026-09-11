import {expect,test,vi} from "vitest";
import {withConversionProvider} from "./conversion-provider.js";
test("conversion closes a non-idempotent provider exactly once",async()=>{
 let closes=0;
 const provider={close:vi.fn(async()=>{if(++closes>1)throw new Error("already closed");})};
 await withConversionProvider(provider,async()=>{});
 expect(provider.close).toHaveBeenCalledTimes(1);
});
test("conversion preserves its primary error when provider cleanup also fails",async()=>{
 const primary=Object.assign(new Error("pending edits preserved"),{code:"LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED"});
 const close=vi.fn(async()=>{throw new Error("cleanup failed");});
 await expect(withConversionProvider({close},async()=>{throw primary;})).rejects.toBe(primary);
 expect(close).toHaveBeenCalledTimes(1);
});
test("successful conversion reports provider cleanup failure",async()=>{
 const cleanup=new Error("cleanup failed");
 await expect(withConversionProvider({close:async()=>{throw cleanup;}},async()=>{})).rejects.toBe(cleanup);
});
test("closed storage operation returns result after one provider close",async()=>{
 const close=vi.fn(async()=>{});
 await expect(withConversionProvider({close},async()=>3)).resolves.toBe(3);
 expect(close).toHaveBeenCalledTimes(1);
});
