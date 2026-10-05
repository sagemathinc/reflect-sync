import { describe, it, expect } from "vitest";
import {
  CHANNEL_PORTS,
  sshArgs,
  validateConnection,
  validateTargetName,
} from "../jupyter.js";

const valid = {
  transport: "tcp",
  ip: "127.0.0.1",
  key: "test-key",
  signature_scheme: "hmac-sha256",
  shell_port: 31001,
  iopub_port: 31002,
  stdin_port: 31003,
  control_port: 31004,
  hb_port: 31005,
};

describe("standard remote kernel connection contract", () => {
  it("preserves client ports and signing configuration", () => {
    expect(validateConnection(valid)).toEqual(valid);
    expect(CHANNEL_PORTS).toHaveLength(5);
  });
  it.each([
    { ip: "0.0.0.0" },
    { transport: "ipc" },
    { key: "" },
    { shell_port: 31002 },
    { hb_port: 0 },
  ])("rejects unsafe or unsupported configuration %j", (patch) => {
    expect(() => validateConnection({ ...valid, ...patch })).toThrow();
  });
  it("requires batch SSH with host verification and forward failure detection", () => {
    const args = sshArgs("jupyter");
    expect(args).toContain("StrictHostKeyChecking=yes");
    expect(args).toContain("BatchMode=yes");
    expect(args).toContain("ExitOnForwardFailure=yes");
    expect(args).toContain("ForwardAgent=no");
    const enrollment = sshArgs("jupyter", true);
    expect(enrollment).toContain("StrictHostKeyChecking=accept-new");
    expect(enrollment).not.toContain("StrictHostKeyChecking=no");
    expect(enrollment).toContain("BatchMode=yes");
    expect(enrollment).toContain("ForwardAgent=no");
  });
  it.each(["-oProxyCommand=bad", "host\ncommand", ""])(
    "rejects invalid SSH destination %j",
    (host) => {
      expect(() => sshArgs(host)).toThrow();
    },
  );
  it.each(["../escape", "-bad/path", ""])(
    "rejects invalid identifier %j",
    (name) => {
      expect(() => validateTargetName(name)).toThrow();
    },
  );
});
