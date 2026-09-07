import { defineConfig } from "vitest/config";

const sshTests = [
  "src/tests/ssh.*.test.ts",
  "src/tests/integration/basic-ssh.session.test.ts",
  "src/tests/integration/ssh-control-master.test.ts",
];

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    // The filesystem-heavy sync suites spawn additional Node/rsync processes.
    // Bounding file-level parallelism prevents load-induced timeouts on CI.
    maxWorkers: 2,
    setupFiles: ["./test-setup.js"],
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["src/tests/**/*.test.ts"],
          exclude: ["src/tests/integration/**/*.test.ts", ...sshTests],
        },
      },
      {
        extends: true,
        test: {
          name: "integration",
          include: ["src/tests/integration/**/*.test.ts"],
          exclude: sshTests,
        },
      },
      {
        extends: true,
        test: {
          name: "ssh",
          include: sshTests,
          sequence: { concurrent: false },
        },
      },
    ],
  },
});
