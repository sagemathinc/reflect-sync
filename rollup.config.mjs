// rollup.config.mjs
import resolve from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";
import terser from '@rollup/plugin-terser';
import fs from "node:fs";

let workerSrc = "";
try {
  workerSrc = fs.readFileSync("dist/hash-worker.bundle.cjs", "utf8");
} catch {/* ok for dev */}

export default {
  input: "dist/cli.js",
  output: {
    file: "dist/bundle.cjs",
    format: "commonjs",
    inlineDynamicImports: true,
    sourcemap: false,
    banner: `
      // reflect-sync SEA banner
      process.env.RFSYNC_BUNDLED = '1';
      globalThis.__RFSYNC_HASH_WORKER__ = ${JSON.stringify(workerSrc)};
    `,
  },
  external: (id) => id.startsWith("node:"),
  plugins: [resolve({ preferBuiltins: true }), commonjs(), json(), terser({ compress: true, mangle: true })],
  treeshake: true,
};
