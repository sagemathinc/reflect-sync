import { buildSshArgs } from "../forward-runner.js";
import type { ForwardRow } from "../session-db.js";

function makeForwardRow(patch: Partial<ForwardRow>): ForwardRow {
  return {
    id: 1,
    created_at: Date.now(),
    updated_at: Date.now(),
    name: null,
    direction: "local_to_remote",
    ssh_host: "user@example.com",
    ssh_port: null,
    ssh_compress: 0,
    ssh_args: null,
    local_host: "127.0.0.1",
    local_port: 7100,
    remote_host: "localhost",
    remote_port: 7100,
    desired_state: "running",
    actual_state: "running",
    monitor_pid: null,
    last_error: null,
    ...patch,
  };
}

describe("forward runner args", () => {
  it("maps legacy local_to_remote target 127.0.0.1 to localhost", () => {
    const row = makeForwardRow({
      remote_host: "127.0.0.1",
    });
    expect(buildSshArgs(row)).toEqual([
      "-N",
      "-L",
      "127.0.0.1:7100:localhost:7100",
      "user@example.com",
    ]);
  });

  it("preserves explicit remote target host names", () => {
    const row = makeForwardRow({
      remote_host: "my-service.internal",
    });
    expect(buildSshArgs(row)).toEqual([
      "-N",
      "-L",
      "127.0.0.1:7100:my-service.internal:7100",
      "user@example.com",
    ]);
  });
});
