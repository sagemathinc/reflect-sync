import { createTestSession, SSH_AVAILABLE } from "./env.js";
import type { TestSession } from "./env.js";
import { waitFor, hasEventLog } from "../util.js";

jest.setTimeout(25_000);

describe("hot sync integration (local roots)", () => {
  let session: TestSession | undefined;

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
  });

  it("mirrors alpha→beta via local hot watch", async () => {
    session = await createTestSession({
      hot: true,
      full: false,
    });

    await session.alpha.writeFile("hot/alpha.txt", "alpha-hot");

    await waitFor(
      () => session!.beta.exists("hot/alpha.txt"),
      (exists) => exists === true,
      7_000,
      100,
    );

    await expect(
      session.beta.readFile("hot/alpha.txt", "utf8"),
    ).resolves.toBe("alpha-hot");
  });

  it("mirrors beta→alpha via local hot watch", async () => {
    session = await createTestSession({
      hot: true,
      full: false,
    });

    await session.beta.writeFile("hot/beta.txt", "beta-hot");

    await waitFor(
      () => session!.alpha.exists("hot/beta.txt"),
      (exists) => exists === true,
      7_000,
      100,
    );

    await expect(
      session.alpha.readFile("hot/beta.txt", "utf8"),
    ).resolves.toBe("beta-hot");
  });
});

const describeIfSsh = SSH_AVAILABLE ? describe : describe.skip;

describeIfSsh("hot sync integration (remote beta)", () => {
  let session: TestSession | undefined;

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
  });

  it("mirrors beta→alpha via remote watch", async () => {
    session = await createTestSession({
      hot: true,
      full: false,
      beta: { remote: true },
    });

    await waitFor(
      () =>
        hasEventLog(session!.baseDbPath, "watch", "beta remote watch ready"),
      (ready) => ready === true,
      15_000,
      200,
    );

    await session.beta.writeFile("remote-hot.txt", "beta-remote-hot");

    await waitFor(
      () => session!.alpha.exists("remote-hot.txt"),
      (exists) => exists === true,
      10_000,
      100,
    );

    await expect(
      session.alpha.readFile("remote-hot.txt", "utf8"),
    ).resolves.toBe("beta-remote-hot");
  });
});
