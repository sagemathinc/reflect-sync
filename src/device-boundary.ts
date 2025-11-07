import path from "node:path";
import { lstat } from "node:fs/promises";
import type { Stats } from "node:fs";

type DeviceId = number | bigint;

export type DeviceCheckOptions = {
  isDir?: boolean;
  stats?: Stats;
};

/**
 * Tracks the device id of a sync root and memoizes device ids for
 * directories beneath it so we can cheaply decide whether a path
 * crosses filesystem boundaries.
 */
export class DeviceBoundary {
  private cache = new Map<string, DeviceId>();

  private constructor(
    private readonly rootAbs: string,
    private readonly rootDev: DeviceId,
  ) {
    this.cache.set(rootAbs, rootDev);
  }

  static async create(root: string): Promise<DeviceBoundary> {
    const abs = path.resolve(root);
    const st = await lstat(abs);
    return new DeviceBoundary(abs, st.dev);
  }

  get root(): string {
    return this.rootAbs;
  }

  get deviceId(): DeviceId {
    return this.rootDev;
  }

  private normalize(absPath: string): string {
    return path.resolve(absPath);
  }

  private dirKey(absPath: string, isDir: boolean): string {
    const resolved = this.normalize(absPath);
    if (isDir) {
      return resolved;
    }
    const dir = path.dirname(resolved);
    return dir === resolved ? resolved : dir;
  }

  private lookup(dirKey: string): DeviceId | undefined {
    let current = dirKey;
    while (true) {
      const cached = this.cache.get(current);
      if (cached !== undefined) {
        return cached;
      }
      const parent = path.dirname(current);
      if (!parent || parent === current) {
        break;
      }
      current = parent;
    }
    return undefined;
  }

  async isOnRootDevice(
    absPath: string,
    opts: DeviceCheckOptions = {},
  ): Promise<boolean> {
    const isDir = !!opts.isDir;
    const key = this.dirKey(absPath, isDir);
    if (!opts.stats) {
      const cached = this.lookup(key);
      if (cached !== undefined) {
        return cached === this.rootDev;
      }
    }

    let stats = opts.stats;
    if (!stats) {
      try {
        stats = await lstat(this.normalize(absPath));
      } catch (err: any) {
        if (err?.code === "ENOENT") {
          // If the entry vanished, fall back to the parent directory's device.
          const parent = path.dirname(key);
          const parentDev =
            parent && parent !== key ? this.lookup(parent) : undefined;
          return (parentDev ?? this.rootDev) === this.rootDev;
        }
        throw err;
      }
    }

    const dev = stats.dev;
    const storeKey = isDir ? this.normalize(absPath) : key;
    this.cache.set(storeKey, dev);
    return dev === this.rootDev;
  }
}
