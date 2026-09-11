import config from "./vitest.partial-sync.config.js";
// Reuses artifact middleware; run separately from the key/value profile.
export default {...config,test:{...config.test,include:["tests/partial-file-sync.bench.browser.test.ts"]}};
