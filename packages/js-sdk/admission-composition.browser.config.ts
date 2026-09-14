import { playwright } from '@vitest/browser-playwright';
import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
const authority = process.env.LIX_TEST_AUTHORITY_URL;
if (!authority) throw new Error('Run cargo test -p lix-server composed_browser_shared_admission_and_opfs -- --ignored --nocapture');
const account = '00000000-0000-7000-8000-000000000003';
export default defineConfig({
  define: {"import.meta.env.LIX_AUTHORITY_COMPOSITION": "true"},
  server: {
    fs: {allow: [fileURLToPath(new URL('..', import.meta.url))]},
    proxy: {'/lix/': {
      target: authority,
      configure(proxy) {
        proxy.on('proxyReq', (outgoing, incoming) => {
          const token = incoming.headers.authorization;
          outgoing.removeHeader('x-lix-account-id');
          outgoing.removeHeader('x-lix-idempotency-scope');
          outgoing.setHeader('authorization', 'Bearer browser-integration-only');
          outgoing.setHeader('x-lix-account-id', token === 'Bearer other-account' ? '00000000-0000-7000-8000-000000000005' : account);
        });
      },
    }},
  },
  test: {include:['integration/admission-composition.browser.test.ts','integration/migration-composition.browser.test.ts'],browser:{enabled:true,headless:true,provider:playwright(),instances:[{browser:'chromium'}]}},
});
