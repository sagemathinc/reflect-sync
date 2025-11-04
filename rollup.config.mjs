// rollup.config.mjs
import resolve from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";
import terser from '@rollup/plugin-terser';
import fs from "node:fs";

let workerSrc = "";
try {
  workerSrc = fs.readFileSync("dist/hash-worker.bundle.cjs", "utf8");
} catch {
  /* ok for dev */
}

const banner = `
  // reflect-sync SEA banner
  process.env.REFLECT_BUNDLED = '1';
  globalThis.__REFLECT_HASH_WORKER__ = ${JSON.stringify(workerSrc)};
`;

const external = (id) => id.startsWith("node:");

const plugins = () => [
  resolve({ preferBuiltins: true }),
  commonjs(),
  json(),
  terser({ compress: true, mangle: true }),
];

const makeConfig = (format, file) => ({
  input: "dist/cli.js",
  output: {
    file,
    format,
    inlineDynamicImports: true,
    sourcemap: false,
    banner,
  },
  external,
  plugins: plugins(),
  treeshake: true,
});

export default [
  makeConfig("commonjs", "dist/bundle.cjs"),
  makeConfig("esm", "dist/bundle.mjs"),
];
