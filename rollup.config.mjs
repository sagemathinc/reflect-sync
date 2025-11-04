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
  if (!process.__REFLECT_WARNING_FILTER__) {
    process.removeAllListeners('warning');
    process.__REFLECT_WARNING_FILTER__ = true;
    process.on('warning', (warning) => {
      if (
        warning?.name === 'ExperimentalWarning' &&
        typeof warning?.message === 'string' &&
        warning.message.toLowerCase().includes('sqlite')
      ) {
        return;
      }
      const output =
        warning?.stack ??
        warning?.message ??
        (typeof warning === 'string' ? warning : String(warning));
      console.warn(output);
    });
  }
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
