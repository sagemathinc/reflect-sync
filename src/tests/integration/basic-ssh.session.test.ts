import { createTestSession, SSH_AVAILABLE } from "./env.js";
import type { TestSession } from "./env.js";

jest.setTimeout(10_000);

const describeIfSsh = SSH_AVAILABLE ? describe : describe.skip;

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
