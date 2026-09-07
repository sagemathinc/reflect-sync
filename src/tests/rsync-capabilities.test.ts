import { probeRsync } from "../rsync-capabilities.js";
import type { CommandRunner } from "../process-capture.js";
import {
  rsyncArgsBase,
  rsyncArgsDirs,
  rsyncArgsFixMeta,
  rsyncArgsFixMetaDirs,
} from "../rsync.js";

function runner(version: string, help: string): CommandRunner {
  return async (_command, args) => ({
    command: "rsync",
    args,
    code: 0,
    signal: null,
    stdout: args.includes("--version") ? version : help,
    stderr: "",
    timedOut: false,
  });
}

describe("rsync capability probe", () => {
  it("accepts upstream rsync with every required feature", async () => {
    const result = await probeRsync(
      "/managed/rsync",
      runner(
        "rsync  version 3.4.1  protocol version 32\nCompress list: zstd lz4 zlibx zlib none",
        "--from0 --relative --outbuf=MODE --info=FLAGS progress2 --log-file=FILE --log-file-format=FMT --compress-choice=STR",
      ),
    );
    expect(result.compatible).toBe(true);
    expect(result.implementation).toBe("upstream");
    expect(result.protocol).toBe(32);
    expect(result.features.zstd).toBe(true);
  });

  it("rejects Apple openrsync and lists missing features", async () => {
    const result = await probeRsync(
      "/usr/bin/rsync",
      runner(
        "openrsync: protocol version 29",
        "--relative --log-file=FILE --log-file-format=FMT",
      ),
    );
    expect(result.compatible).toBe(false);
    expect(result.implementation).toBe("openrsync");
    expect(result.missing).toContain("from0");
    expect(result.missing).toContain("outbuf");
  });
});

describe("rsync file-type contract", () => {
  it("disables device and special-file copying in archive argument sets", () => {
    for (const args of [
      rsyncArgsBase({}, "/from", "/to"),
      rsyncArgsFixMeta({}),
      rsyncArgsFixMetaDirs({}),
    ]) {
      expect(args).toContain("--no-devices");
      expect(args).toContain("--no-specials");
    }
  });

  it("does not request device nodes in the directory-only argument set", () => {
    const args = rsyncArgsDirs({});
    expect(args).not.toContain("--devices");
    expect(args).not.toContain("--specials");
  });
});
