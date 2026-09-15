import { playwright } from '@vitest/browser-playwright';
import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
const binding = fileURLToPath(new URL('./admission-regression-binding.ts', import.meta.url));
export default defineConfig({
  define: {"import.meta.env.LIX_ADMISSION_REGRESSION": "true"},
  resolve: {alias: {'#binding': binding, '#worker-factory': fileURLToPath(new URL('./src/worker/factory.browser.ts', import.meta.url))}},
  worker: {plugins: () => [{name: 'admission-binding', enforce: 'pre' as const, resolveId(id: string) {if(id === '#binding') return binding;}}]},
  plugins: [{ name: 'admission-authority', configureServer(server) {
    server.middlewares.use((req,res,next) => {
      const match = req.url?.match(/^\/lix\/(?:v1\/)?([0-9a-f-]{36})\/(admission|probe)$/);
      if (!match) return next();
      const token = req.headers.authorization ?? '';
      if (token.includes('offline') || req.headers['x-test-drop']) {req.socket.destroy(); return;}
      if (token.includes('denied')) {res.statusCode=401; res.end(); return;}
      if (match[2] === 'probe') {res.end(token);return;}
      if (req.headers['lix-sync-protocol-version'] !== '15') {res.statusCode=426;res.end();return;}
      res.setHeader('content-type','application/json');
      res.end(JSON.stringify({repositoryId:match[1],principalId: token.includes('other') ? '00000000-0000-7000-8000-000000000005' : '00000000-0000-7000-8000-000000000003',protocolEpoch:16,storageEpoch:81}));
    });
  }}],
  test: {include: ['src/worker/shared-admission.browser.test.ts'], browser: {enabled:true,headless:true,provider:playwright(),instances:[{browser:'chromium'}]}},
});
