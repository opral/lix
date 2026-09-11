import config from "./vitest.partial-sync.config.js";
export default {...config,test:{...config.test,include:["tests/partial-provider-telemetry.bench.browser.test.ts"]}};
