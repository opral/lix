import type { LspEnvironment } from "./environment/environment.js";
export type LixServerProtocolHandler = (request: Request) => Promise<Response>;
export type LixServerProtocolHandlerContext = {
    request: Request;
    environment: LspEnvironment;
    params?: Record<string, string | undefined>;
};
export type LixServerProtocolHandlerRoute = (context: LixServerProtocolHandlerContext) => Promise<Response>;
/**
 * The handler for the lix server protocol.
 *
 * @example
 *   Usage with a server framework.
 *
 *   ```ts
 * 	 // any server framework goes
 *   // here, like express, polka, etc.
 *   // frameworks that do not use
 *   // web standard Request and Response
 *   // objects will need to be mapped.
 *   const app = new Hono();
 *
 *   const lspHandler = createServerProtocolHandler({ storage });
 *
 *   app.use('/lsp/*', async (req) => {
 *      await lspHandler(req);
 *   });
 *   ```
 *
 * @example
 *   Testing the handler.
 *
 *   ```ts
 *   const lspHandler = createServerProtocolHandler({ storage });
 *   const request = new Request('/lsp/new', {
 *     method: 'POST',
 *     body: new Blob(['...']),
 *   });
 *
 *   const response = await lspHandler(request);
 *
 *   expect(response).to(...);
 *   ```
 */
export declare function createServerProtocolHandler(args: {
    environment: LspEnvironment;
}): Promise<LixServerProtocolHandler>;
export declare const createServerApiHandler: typeof createServerProtocolHandler;
//# sourceMappingURL=create-server-protocol-handler.d.ts.map