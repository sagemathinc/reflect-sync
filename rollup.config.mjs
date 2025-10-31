import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';

export default {
  input: 'dist/cli.js',
  output: {
    file: 'dist/bundle.cjs',
    format: 'commonjs',
    inlineDynamicImports: true,   // <-- critical: a single file
    sourcemap: false,
    banner: "process.env.RFSYNC_BUNDLED = '1';"
  },
  external: (id) => id.startsWith('node:'),
  plugins: [
    resolve({ preferBuiltins: true }),
    commonjs(),
    json()
  ],
  treeshake: true
};
