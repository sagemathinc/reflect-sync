#!/usr/bin/env node

import { promises as fs } from "fs";

const COUNT = 1000;

/**
 * Rewrites foo.txt as fast as possible (with a tiny delay so the event loop can breathe).
 */
export async function constantlyRewriteFile(path = "foo.txt", delay = 1) {
  let i = 0;

  while (true) {
    for (let c = 0; c < COUNT; c++) {
      const content = `write #${i} at ${new Date().toISOString()}\n`;
      await fs.writeFile(path + c, content);
    }
    i++;

    await new Promise((resolve) => setTimeout(resolve, delay));
    for (let c = 0; c < COUNT; c++) {
      await fs.unlink(path + c);
    }
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
}

// Example usage (uncomment if you want it to start automatically):
// constantlyRewriteFile().catch(console.error);

async function main() {
  console.log(process.argv[2]);
  constantlyRewriteFile(process.argv[2], Number(process.argv[3] ?? "1"));
}

main();
