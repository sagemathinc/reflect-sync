import path from 'node:path';
import { fileURLToPath } from 'node:url';
import tseslint from '@typescript-eslint/eslint-plugin';
import tsParser from '@typescript-eslint/parser';
import importPlugin from 'eslint-plugin-import';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default [
  {
    ignores: ['node_modules/**', 'dist/**', 'bundle/**', 'coverage/**', 'bin/reflect-sync.mjs'],
  },
  {
    files: ['src/**/*.{ts,tsx}'],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        sourceType: 'module',
        project: './tsconfig.eslint.json',
        tsconfigRootDir: __dirname,
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
      import: importPlugin,
    },
    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          ignoreRestSiblings: false,
        },
      ],
      'import/no-unused-modules': [
        'warn',
        {
          unusedExports: true,
          missingExports: true,
        },
      ],
    },
  },
  {
    files: [
      'src/tests/**/*.{ts,tsx}',
      'src/**/cli*.ts',
      'src/forward-cli.ts',
      'src/hash-worker.ts',
      'src/install-cli.ts',
    ],
    rules: {
      'import/no-unused-modules': 'off',
    },
  },
];
