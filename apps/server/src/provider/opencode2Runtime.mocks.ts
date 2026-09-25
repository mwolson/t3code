import { vi } from "vite-plus/test";

export const forbiddenLifecycle = vi.fn(() => {
  throw new Error("Forbidden lifecycle helper");
});
export const forbiddenProcess = vi.fn(() => {
  throw new Error("Forbidden child process");
});

const service = {
  Info: forbiddenLifecycle,
  discover: forbiddenLifecycle,
  ensure: forbiddenLifecycle,
  headers: forbiddenLifecycle,
  incumbent: forbiddenLifecycle,
  stop: forbiddenLifecycle,
};
export const serviceModule = { ...service, Service: service };

export const childProcessMethods = {
  exec: forbiddenProcess,
  execFile: forbiddenProcess,
  execFileSync: forbiddenProcess,
  execSync: forbiddenProcess,
  fork: forbiddenProcess,
  spawn: forbiddenProcess,
  spawnSync: forbiddenProcess,
};
