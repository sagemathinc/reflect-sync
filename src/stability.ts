// src/stability.ts
import { lstat } from "node:fs/promises";

export type StabilityOptions = {
  windowMs?: number;
  pollMs?: number;
  maxWaitMs?: number;
};

export type StabilityResult = {
  stable: boolean;
  reason?: string;
};

const defaultWindow = Number(
  process.env.REFLECT_STABILITY_WINDOW_MS ?? 250,
);
const defaultPoll = Number(process.env.REFLECT_STABILITY_POLL_MS ?? 50);
const defaultMaxWait = Number(
  process.env.REFLECT_STABILITY_MAX_WAIT_MS ??
    Math.max(defaultWindow * 4, 1000),
);

export const DEFAULT_STABILITY_OPTIONS: Required<StabilityOptions> = {
  windowMs: Number.isFinite(defaultWindow) ? defaultWindow : 250,
  pollMs: Number.isFinite(defaultPoll) ? defaultPoll : 50,
  maxWaitMs: Number.isFinite(defaultMaxWait) ? defaultMaxWait : 2000,
};

type Snapshot = {
  size: number;
  mtimeMs: number;
  ctimeMs: number;
  isFile: boolean;
  exists: boolean;
};

const sleep = (ms: number) =>
  ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve();

async function sample(abs: string): Promise<Snapshot> {
  try {
    const st = await lstat(abs);
    const mtimeMs = (st as any).mtimeMs ?? st.mtime.getTime();
    const ctimeMs = (st as any).ctimeMs ?? st.ctime.getTime();
    return {
      size: st.size,
      mtimeMs,
      ctimeMs,
      isFile: st.isFile(),
      exists: true,
    };
  } catch (err: any) {
    if (err?.code === "ENOENT") {
      return {
        size: 0,
        mtimeMs: 0,
        ctimeMs: 0,
        isFile: false,
        exists: false,
      };
    }
    throw err;
  }
}

function snapshotChanged(a: Snapshot, b: Snapshot) {
  return a.size !== b.size || a.mtimeMs !== b.mtimeMs || a.ctimeMs !== b.ctimeMs;
}

export async function waitForStableFile(
  abs: string,
  opts: StabilityOptions = {},
): Promise<StabilityResult> {
  const windowMs =
    opts.windowMs ?? DEFAULT_STABILITY_OPTIONS.windowMs;
  const pollMs = opts.pollMs ?? DEFAULT_STABILITY_OPTIONS.pollMs;
  const maxWaitMs =
    opts.maxWaitMs ?? DEFAULT_STABILITY_OPTIONS.maxWaitMs;

  if (!Number.isFinite(windowMs) || windowMs <= 0) {
    return { stable: true, reason: "disabled" };
  }

  let snap = await sample(abs);
  if (!snap.exists) {
    return { stable: true, reason: "missing" };
  }
  if (!snap.isFile) {
    return { stable: true, reason: "not-file" };
  }

  const freshnessMs = Date.now() - Math.max(snap.mtimeMs, snap.ctimeMs);
  if (freshnessMs >= windowMs) {
    return { stable: true, reason: "already-stable" };
  }

  let remaining = windowMs - freshnessMs;
  const deadline = Date.now() + Math.max(remaining, maxWaitMs);

  while (remaining > 0 && Date.now() < deadline) {
    await sleep(Math.min(pollMs, remaining));
    const next = await sample(abs);
    if (!next.exists) {
      return { stable: true, reason: "missing" };
    }
    if (!next.isFile) {
      return { stable: true, reason: "not-file" };
    }
    if (snapshotChanged(snap, next)) {
      snap = next;
      remaining = windowMs;
      continue;
    }
    const age = Date.now() - Math.max(next.mtimeMs, next.ctimeMs);
    remaining = Math.max(0, windowMs - age);
  }

  if (remaining <= 0) {
    return { stable: true, reason: "window-met" };
  }

  return { stable: false, reason: "timeout" };
}
