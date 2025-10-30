// src/hash.ts
import { createReadStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import fs from "node:fs/promises";
import { createHash, getHashes } from "node:crypto";

export const HASH_STREAM_CUTOFF = 10_000_000; // ~10MB; small files do one-shot hashing
export const STREAM_HWM = 8 * 1024 * 1024; // 8MB read chunks

const ENCODING = "base64";

// Curated set we’re willing to expose
export const CURATED_HASH_ALGOS = [
  "sha1",
  "sha256",
  "sha512",
  "blake2b512",
  "blake2s256",
  "sha3-256",
  "sha3-512",
] as const;

export type HashAlg = (typeof CURATED_HASH_ALGOS)[number];

export function defaultHashAlg(): HashAlg {
  return "sha256";
}

let supportedHashes: HashAlg[] | null = null;
export function listSupportedHashes(): HashAlg[] {
  if (supportedHashes == null) {
    const avail = new Set(getHashes().map((s) => s.toLowerCase()));
    supportedHashes = CURATED_HASH_ALGOS.filter((a) =>
      avail.has(a),
    ) as HashAlg[];
  }
  return supportedHashes!;
}

/**
 * Normalize/validate requested algorithm against runtime support.
 * Accepts short shorthands "blake2b" -> blake2b512, "blake2s" -> blake2s256.
 */
export function normalizeHashAlg(requested?: string): HashAlg {
  const list = listSupportedHashes();
  if (!requested) return defaultHashAlg();
  const low = requested.toLowerCase();
  const exact = list.find((h) => h.toLowerCase() === low);
  if (exact) return exact;

  if (low === "blake2b") {
    const b = list.find((h) => h.toLowerCase() === "blake2b512");
    if (b) return b;
  }
  if (low === "blake2s") {
    const s = list.find((h) => h.toLowerCase() === "blake2s256");
    if (s) return s;
  }

  throw new Error(
    `Unknown/unsupported hash algorithm "${requested}". Try one of:\n  ${list.join(", ")}`,
  );
}

// Hash a string (e.g., symlink targets).
export function stringDigest(alg: string, s: string): string {
  return createHash(alg).update(s).digest(ENCODING);
}

/**
 * Hash a file. Uses a fast path for small files and a backpressured streaming
 * pipeline for large files. If 'size' is not provided, we'll stat the file.
 */
export async function fileDigest(
  alg: string,
  path: string,
  size?: number,
): Promise<string> {
  let n = size;
  if (n == null) {
    try {
      const st = await fs.stat(path);
      n = st.size;
    } catch (e) {
      // fall back to streaming if stat fails for some reason
      n = HASH_STREAM_CUTOFF + 1;
    }
  }

  // Fast path: read whole file at once
  if (n <= HASH_STREAM_CUTOFF) {
    const buf = await fs.readFile(path);
    return createHash(alg).update(buf).digest(ENCODING);
  }

  // Streaming path: strong backpressure + error propagation via pipeline
  const h = createHash(alg);
  const rs = createReadStream(path, { highWaterMark: STREAM_HWM });

  await pipeline(rs, async function* (src) {
    for await (const chunk of src) {
      h.update(chunk);
      // yield once to satisfy transform signature (no downstream consumer)
      yield;
    }
  });

  return h.digest(ENCODING);
}

// not a full hash, just mode bits in hex.
export function modeHash(mode: number): string {
  return (mode & 0o7777).toString(16);
}
