import { createTestSession, SSH_AVAILABLE } from "./env.js";
import type { TestSession } from "./env.js";

jest.setTimeout(20_000);
if (!process.env.REFLECT_LOG_LEVEL) {
  process.env.REFLECT_LOG_LEVEL = "info";
}

const describeIfSsh = SSH_AVAILABLE ? describe : describe.skip;

describe("integration test harness", () => {
  let session: TestSession | undefined;

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
  });

  it("syncs basic local file operations", async () => {
    session = await createTestSession({
      hot: false,
      full: false,
    });

    await session.alpha.writeFile("foo/bar.txt", "alpha v1");
    await session.alpha.mkdir("dir/subdir");
    await session.sync();

    await expect(
      session.beta.readFile("foo/bar.txt", "utf8"),
    ).resolves.toBe("alpha v1");
    await expect(session.beta.exists("dir/subdir")).resolves.toBe(true);

    // NOTE: this behavior where it actually ignores this change
    // is correct right now.  However, we should add an extra
    // scan step to the definition of what a full sync does
    // i.e. what session.sync() above does,
    // so that this change is NOT ignored. 
    await session.beta.appendFile("foo/bar.txt", "\nbeta delta");
    await session.sync();

    await expect(
      session.alpha.readFile("foo/bar.txt", "utf8"),
    ).resolves.toBe("alpha v1");

    await session.alpha.rm("foo");
    await session.sync();
    await expect(session.beta.exists("foo")).resolves.toBe(false);
  });
});

describeIfSsh("integration harness over ssh", () => {
  let session: TestSession | undefined;

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
  });

  it("mirrors remote beta changes back to alpha", async () => {
    session = await createTestSession({
      hot: false,
      full: false,
      beta: { remote: true },
    });

    await session.beta.writeFile("remote-only.txt", "beta");
    await session.sync();

    await expect(
      session.alpha.readFile("remote-only.txt", "utf8"),
    ).resolves.toBe("beta");
  });

});
