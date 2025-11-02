import { promises as fs } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

jest.mock("../remote.js", () => ({
  ensureRemoteParentDir: jest.fn(async () => {}),
  sshDeleteDirectory: jest.fn(async () => {}),
}));

import { defaultHashAlg } from "../hash.js";
import { newSession } from "../session-manage.js";
import { getSessionDbPath, loadSessionById } from "../session-db.js";
import { ensureRemoteParentDir } from "../remote.js";

describe("newSession remote ports", () => {
  let previousHome: string | undefined;
  let tempHome: string;
  let sessionDb: string;

  beforeEach(async () => {
    previousHome = process.env.REFLECT_HOME;
    tempHome = await fs.mkdtemp(join(tmpdir(), "reflect-session-test-"));
    process.env.REFLECT_HOME = tempHome;
    sessionDb = getSessionDbPath();
    await fs.mkdir(join(tempHome, "beta-root"), { recursive: true });
  });

  afterEach(async () => {
    if (previousHome === undefined) delete process.env.REFLECT_HOME;
    else process.env.REFLECT_HOME = previousHome;
    jest.clearAllMocks();
    await fs.rm(tempHome, { recursive: true, force: true });
  });

  it("persists parsed remote port", async () => {
    const alphaSpec = "example.com:2222:/srv/alpha";
    const betaSpec = join(tempHome, "beta-root");

    const id = await newSession({
      alphaSpec,
      betaSpec,
      sessionDb,
      compress: "auto",
      compressLevel: "",
      prefer: "alpha",
      hash: defaultHashAlg(),
      label: [],
      name: undefined,
      logger: undefined,
      ignore: undefined,
    } as any);

    const row = loadSessionById(sessionDb, id)!;
    expect(row.alpha_host).toBe("example.com");
    expect(row.alpha_port).toBe(2222);
    expect(row.beta_host).toBeNull();
    expect(row.beta_port).toBeNull();

    expect(ensureRemoteParentDir).toHaveBeenCalledWith(
      expect.objectContaining({ host: "example.com", port: 2222 }),
    );
  });
});
