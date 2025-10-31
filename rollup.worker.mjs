// rollup.worker.mjs
import resolve from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";
import terser from '@rollup/plugin-terser';

export default {
  input: "dist/hash-worker.js",           // tsc output
  output: {
    file: "dist/hash-worker.bundle.cjs",  // single CJS file
    format: "cjs",
    inlineDynamicImports: true,
    sourcemap: false,
  },
  external: (id) => id.startsWith("node:"), // keep Node built-ins external
  plugins: [resolve({ preferBuiltins: true }), commonjs(), json(), terser({ compress: true, mangle: true })],
  treeshake: true,
};
