import {afterEach, expect, test, vi} from 'vitest';
import {createSharedWorkerConnection} from './factory.browser.js';

afterEach(() => {vi.unstubAllGlobals();vi.useRealTimers();});
function connection() {
 const port = {postMessage:vi.fn(),start:vi.fn(),close:vi.fn(),onmessage:undefined as undefined|((event:{data:unknown})=>void)};
 vi.stubGlobal('SharedWorker',class {port=port;onerror=undefined;});
 vi.stubGlobal('navigator',{locks:{request:async(_name:string,callback:()=>Promise<void>)=>callback()}});
 const result=createSharedWorkerConnection('test');
 result.onMessage(()=>{});
 return {result,port};
}
test('shared termination waits for physical close acknowledgement',async()=>{
 const {result,port}=connection();
 const closing=result.terminate();
 await Promise.resolve();
 expect(port.close).not.toHaveBeenCalled();
 port.onmessage!({data:{kind:'shared.disconnected'}});
 await closing;
 expect(port.close).toHaveBeenCalledTimes(1);
 await result.terminate();
 expect(port.close).toHaveBeenCalledTimes(1);
});
test('unconfirmed shared close stays bounded on repeated termination',async()=>{
 vi.useFakeTimers();
 const {result,port}=connection();
 const first=result.terminate();
 const failure=expect(first).rejects.toMatchObject({code:'LIX_SHARED_ENGINE_CLOSE_UNCONFIRMED'});
 await vi.advanceTimersByTimeAsync(10000);
 await failure;
 await expect(result.terminate()).rejects.toMatchObject({code:'LIX_SHARED_ENGINE_CLOSE_UNCONFIRMED'});
 expect(port.close).toHaveBeenCalledTimes(1);
});
