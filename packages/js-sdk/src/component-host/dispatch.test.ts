import { expect, test, vi } from "vitest";
import { createComponentDispatch } from "./dispatch.js";

const loader = vi.hoisted(() => ({ loads: 0, compile: vi.fn() }));
vi.mock("./index.js", () => {
  loader.loads += 1;
  return { compileComponent: loader.compile };
});
const limits = { maxMemoryBytes: "1048576", timeoutMs: "5000" };
const instance = () => ({
  exports: {},
  setDeadline() {},
  memoryBytes: () => 0,
});
const factory = () => ({ instantiate: async () => instance() });
const compile = (requestId: string) => ({
  operation: "compile",
  data: JSON.stringify({ requestId, limits }),
  bytes: new Uint8Array([0]),
});
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("loads the component compiler only when a plugin is compiled by default", async () => {
  expect(loader.loads).toBe(0);
  const dispatch = createComponentDispatch();
  await dispatch({ operation: "disposeGuest", data: '{"id":1}' });
  expect(loader.loads).toBe(0);
  loader.compile.mockResolvedValueOnce(factory());
  await dispatch(compile("first-plugin"));
  expect(loader.loads).toBe(1);
  expect(loader.compile).toHaveBeenCalledOnce();
});

test("a supplied browser compiler is used without a dispatch-time import", async () => {
  const loads = loader.loads;
  const suppliedCompiler = vi.fn().mockResolvedValue(factory());
  const dispatch = createComponentDispatch(suppliedCompiler);
  await dispatch({ operation: "disposeGuest", data: '{"id":1}' });
  expect(suppliedCompiler).not.toHaveBeenCalled();
  await dispatch(compile("browser-plugin"));
  expect(suppliedCompiler).toHaveBeenCalledOnce();
  expect(loader.loads).toBe(loads);
});

test("cancellation while compiling disposes the eventual factory", async () => {
  const pending = deferred<ReturnType<typeof factory>>();
  loader.compile.mockReturnValueOnce(pending.promise);
  const dispatch = createComponentDispatch();
  const response = dispatch(compile("pending-compile"));
  await dispatch({
    operation: "cancelRequest",
    data: '{"requestId":"pending-compile"}',
  });
  pending.resolve(factory());
  const handle = JSON.parse(await response);
  await expect(
    dispatch({ operation: "instantiate", data: JSON.stringify(handle) }),
  ).rejects.toThrow("Unknown component factory");
});

test("cancellation after a result is delivered disposes the unclaimed handle", async () => {
  loader.compile.mockResolvedValueOnce(factory());
  const dispatch = createComponentDispatch();
  const handle = JSON.parse(await dispatch(compile("unclaimed")));
  await dispatch({
    operation: "cancelRequest",
    data: '{"id":"unclaimed"}',
  });
  await expect(
    dispatch({ operation: "instantiate", data: JSON.stringify(handle) }),
  ).rejects.toThrow("Unknown component factory");
});

test("finished factory ownership survives request cleanup, while a canceled instance is discarded", async () => {
  const pending = deferred<ReturnType<typeof instance>>();
  loader.compile.mockResolvedValueOnce({ instantiate: () => pending.promise });
  const dispatch = createComponentDispatch();
  const handle = JSON.parse(await dispatch(compile("owned")));
  await dispatch({ operation: "finishRequest", data: '{"id":"owned"}' });
  const response = dispatch({
    operation: "instantiate",
    data: JSON.stringify({ ...handle, requestId: "pending-instance" }),
  });
  await dispatch({
    operation: "cancelRequest",
    data: '{"requestId":"pending-instance"}',
  });
  pending.resolve(instance());
  const guest = JSON.parse(await response);
  await expect(
    dispatch({ operation: "invoke", data: JSON.stringify(guest) }),
  ).rejects.toThrow("Unknown or busy component guest");
});
