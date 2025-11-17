// rsync-compression.ts
import { spawn as rawSpawn } from "node:child_process";

export type RsyncCompressSpec = string | undefined;

// The string is a spec of the form "[auto|zstd|lz4|zlib|zlibx|none][:level],
// where the level options are:
// zstd: -131072..22 (3 default), zlib/zlibx: 1..9 (6 default), lz4: 0
// E.g., "auto"  or "zstd:8" or "zlib:2"

// Translate a compression spec into rsync CLI args.
export function rsyncCompressionArgs(spec?: RsyncCompressSpec): string[] {
  if (!spec || spec.startsWith("none")) return [];

  const v = spec.split(":");
  const algo = v[0] ? v[0] : "zstd";
  const level = v[1] ? String(Number(v[1])) : "";
  const args: string[] = [];

  if (spec == "auto") {
    // at this point, let rsync decide
    args.push("--compress");
  } else {
    args.push(`--compress-choice=${algo}`);
  }
  if (level) args.push(`--compress-level=${level}`);

  return args;
}

export function isCompressing(spec: RsyncCompressSpec | undefined) {
  if (!spec) return false;
  return !spec.startsWith("none");
}

function capture(
  cmd: string,
  args: string[],
  timeoutMs = 2000,
): Promise<{ code: number | null; stdout: string }> {
  return new Promise((resolve) => {
    const p = rawSpawn(cmd, args, { stdio: ["ignore", "pipe", "ignore"] });
    let buf = "";
    let done = false;
    const to = setTimeout(() => {
      if (!done) {
        done = true;
        p.kill("SIGKILL");
        resolve({ code: null, stdout: buf });
      }
    }, timeoutMs).unref();
    p.stdout.setEncoding("utf8").on("data", (s) => (buf += s));
    p.on("exit", (code) => {
      if (!done) {
        done = true;
        clearTimeout(to);
        resolve({ code, stdout: buf });
      }
    });
    p.on("error", () => {
      if (!done) {
        done = true;
        clearTimeout(to);
        resolve({ code: 1, stdout: buf });
      }
    });
  });
}

export async function estimateLinkClass(
  host?: string,
  port?: number,
): Promise<"local" | "fast" | "medium" | "slow"> {
  if (!host) return "local";
  // [ ] TODO: this link speed test seems bad, since it's just
  // testing time to make the ssh connection, which might have
  // little to do with the actual bandwidth (?).
  const t0 = Date.now();
  const TIMEOUT = 2000;
  const args = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=2"];
  if (port != null) {
    args.push("-p", String(port));
  }
  args.push(host, "true");
  const { code } = await capture("ssh", args, TIMEOUT);
  if (code) {
    throw Error(`cannot connect to ${host}`);
  }
  const dt = Date.now() - t0;
  if (dt < 750) return "fast";
  if (dt < TIMEOUT - 100) return "medium";
  return "slow";
}

async function chooseCompressionAuto(
  host: string | undefined,
  port?: number,
): Promise<RsyncCompressSpec> {
  if (!host) {
    // no compression locally
    return "none";
  }
  const link = await estimateLinkClass(host, port);
  if (link == "local") return "none";
  if (link === "fast") return "lz4";
  if (link == "medium") return "zstd";
  if (link == "slow") return "zstd:10";
  return "zstd";
}

export async function resolveCompression(
  // server we're connecting to (if any)
  host: string | undefined,
  // user config, if any
  compress: RsyncCompressSpec | "auto" | undefined,
  port?: number,
): Promise<RsyncCompressSpec> {
  return !compress || compress === "auto"
    ? await chooseCompressionAuto(host, port)
    : compress;
}
