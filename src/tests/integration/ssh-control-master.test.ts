import { Database } from "../../db.js";
import { createTestSession, SSH_AVAILABLE } from "./env.js";
import type { TestSession } from "./env.js";

jest.setTimeout(25_000);
if (!process.env.REFLECT_LOG_LEVEL) {
  process.env.REFLECT_LOG_LEVEL = "info";
}

const describeIfSsh = SSH_AVAILABLE ? describe : describe.skip;

describeIfSsh("ssh control master resilience", () => {
  let session: TestSession | undefined;

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
  });

  it("recovers when the ssh control master is killed", async () => {
    session = await createTestSession({
      hot: false,
      full: false,
      beta: { remote: true },
    });

    const firstReady = await waitForControlMasterEvent(session.baseDbPath);
    expect(firstReady.pid).toBeGreaterThan(0);
    if (!firstReady.pid) {
      throw new Error("missing control master pid");
    }

    try {
      process.kill(firstReady.pid, "SIGKILL");
    } catch (err) {
      throw new Error(
        `failed to kill control master ${firstReady.pid}: ${String(err)}`,
      );
    }

    await session.beta.writeFile(
      "control-channel.txt",
      "beta after master restart",
      "utf8",
    );
    await session.sync();

    const restarted = await waitForControlMasterEvent(
      session.baseDbPath,
      firstReady.id,
    );
    expect(restarted.id).toBeGreaterThan(firstReady.id);

    await expect(
      session.alpha.readFile("control-channel.txt", "utf8"),
    ).resolves.toBe("beta after master restart");
  });
});

type ControlMasterEvent = { id: number; pid: number | null };

async function waitForControlMasterEvent(
  baseDbPath: string,
  afterId = 0,
  timeoutMs = 15_000,
): Promise<ControlMasterEvent> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const evt = readLatestControlMasterEvent(baseDbPath, afterId);
    if (evt) return evt;
    await delay(150);
  }
  throw new Error("timed out waiting for ssh control master ready event");
}

function readLatestControlMasterEvent(
  baseDbPath: string,
  afterId: number,
): ControlMasterEvent | null {
  let db: Database | null = null;
  try {
    db = new Database(baseDbPath);
    const row = db
      .prepare(
        `SELECT id, details FROM events WHERE source='ssh' AND msg='control master ready' AND id > ? ORDER BY id DESC LIMIT 1`,
      )
      .get(afterId) as { id?: number; details?: string } | undefined;
    if (!row?.id) return null;
    let pid: number | null = null;
    if (row.details) {
      try {
        const parsed = JSON.parse(row.details);
        if (typeof parsed?.pid === "number") {
          pid = parsed.pid;
        }
      } catch {
        pid = null;
      }
    }
    return { id: row.id, pid };
  } catch {
    return null;
  } finally {
    db?.close();
  }
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
