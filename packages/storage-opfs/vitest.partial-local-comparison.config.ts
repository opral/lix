import config from "./vitest.partial-sync.config.js";
export default {...config,test:{...config.test,include:["tests/partial-local-comparison.bench.browser.test.ts"]}};
