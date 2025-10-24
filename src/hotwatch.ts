// src/hotwatch.ts
import chokidar, { FSWatcher } from "chokidar";
import path from "node:path";
import { MAX_WATCHERS } from "./defaults.js";

export type HotWatchEvent =
  | "add"
  | "change"
  | "unlink"
  | "addDir"
  | "unlinkDir";

export const HOT_EVENTS: HotWatchEvent[] = [
  "add",
  "change",
  "unlink",
  "addDir",
  "unlinkDir",
];

export interface HotWatchOptions {
  maxWatchers?: number;
  ttlMs?: number; // default 30 min
  hotDepth?: number; // default 2
  awaitWriteFinish?: { stabilityThreshold: number; pollInterval: number }; // default {200, 50}
}

// POSIX-normalize path (also makes Windows separators into '/')
export const norm = (p: string) =>
  path.sep === "/" ? p : p.split(path.sep).join("/");

// dirname with safe handling of "" -> "." and posix semantics
export const parentDir = (r: string) => norm(path.posix.dirname(r || "."));

// relative depth of absPath under anchorAbs
export function relDepth(anchorAbs: string, absPath: string): number {
  const r = norm(path.relative(anchorAbs, absPath));
  if (!r || r === ".") return 0;
  return r.split("/").length - 1;
}

// collapse many sibling dirs under a parent when possible
export function minimalCover(dirs: string[]): string[] {
  const sorted = Array.from(new Set(dirs)).sort((a, b) => a.length - b.length);
  const out: string[] = [];
  for (const d of sorted) {
    if (
      !out.some((p) => d === p || d.startsWith(p.endsWith("/") ? p : p + "/"))
    ) {
      out.push(d);
    }
  }
  return out;
}

export class HotWatchManager {
  private map = new Map<string, { watcher: FSWatcher; expiresAt: number }>();
  private lru: string[] = []; // oldest first
  private opts: Required<HotWatchOptions>;

  constructor(
    private root: string,
    private onHotEvent: (abs: string, ev: HotWatchEvent) => void,
    opts: HotWatchOptions = {},
  ) {
    this.opts = {
      maxWatchers: opts.maxWatchers ?? MAX_WATCHERS,
      ttlMs: opts.ttlMs ?? 30 * 60_000,
      hotDepth: opts.hotDepth ?? 2,
      awaitWriteFinish: opts.awaitWriteFinish ?? {
        stabilityThreshold: 200,
        pollInterval: 50,
      },
    };
  }

  size() {
    return this.map.size;
  }

  async add(rdir: string) {
    rdir = rdir === "" ? "." : rdir;
    const anchorAbs = norm(path.join(this.root, rdir));
    const now = Date.now();

    if (this.map.has(anchorAbs)) {
      this.bump(anchorAbs);
      this.map.get(anchorAbs)!.expiresAt = now + this.opts.ttlMs;
      return;
    }

    const watcher = chokidar.watch(anchorAbs, {
      persistent: true,
      ignoreInitial: true,
      depth: this.opts.hotDepth,
      awaitWriteFinish: this.opts.awaitWriteFinish,
    });

    const handler = async (ev: HotWatchEvent, abs: string) => {
      const absN = norm(abs);
      this.onHotEvent(absN, ev);

      // escalate deeper when event is at frontier depth
      const d = relDepth(anchorAbs, absN);
      if (d >= this.opts.hotDepth && this.map.size < this.opts.maxWatchers) {
        const deeperDir = norm(path.dirname(absN));
        const r = norm(path.relative(this.root, deeperDir));
        if (r && r !== ".") {
          await this.add(r);
        }
      }

      // refresh TTL & LRU
      const st = this.map.get(anchorAbs);
      if (st) st.expiresAt = Date.now() + this.opts.ttlMs;
      this.bump(anchorAbs);
    };

    HOT_EVENTS.forEach((evt) => {
      watcher.on(evt as any, (p: string) => handler(evt as HotWatchEvent, p));
    });

    this.map.set(anchorAbs, { watcher, expiresAt: now + this.opts.ttlMs });
    this.lru.push(anchorAbs);
    await this.evictIfNeeded();
  }

  private bump(abs: string) {
    const i = this.lru.indexOf(abs);
    if (i >= 0) {
      this.lru.splice(i, 1);
      this.lru.push(abs);
    }
  }

  private async evictIfNeeded() {
    const now = Date.now();
    for (const [abs, st] of Array.from(this.map)) {
      if (st.expiresAt <= now) {
        await this.drop(abs);
      }
    }
    while (this.map.size > this.opts.maxWatchers) {
      const victim = this.lru.shift();
      if (!victim) {
        break;
      }
      if (this.map.has(victim)) {
        await this.drop(victim);
      }
    }
  }

  private async drop(abs: string) {
    const st = this.map.get(abs);
    if (!st) {
      return;
    }
    await st.watcher.close().catch(() => {});
    this.map.delete(abs);
    const i = this.lru.indexOf(abs);
    if (i >= 0) this.lru.splice(i, 1);
  }

  async closeAll() {
    await Promise.all(
      Array.from(this.map.values()).map((s) =>
        s.watcher.close().catch(() => {}),
      ),
    );
    this.map.clear();
    this.lru = [];
  }
}
