import profileConfig from "./vitest.partial-sync.config.js";
export default {
 ...profileConfig,
 test: {...profileConfig.test,include:["tests/partial-owner-sync.bench.browser.test.ts"]},
};
