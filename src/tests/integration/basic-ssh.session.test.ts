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

  it.only("LOCAL: create a file with nextjs style filename, sync, delete, sync", async () => {
    session = await createTestSession({
      hot: false,
      full: false,
    });

    // store/[[...page]].tsx
    await session.alpha.mkdir("store");
    await session.alpha.writeFile("store/[[...page]].tsx", "<html/>");
    await session.sync();

    await expect(
      session.beta.readFile("store/[[...page]].tsx", "utf8"),
    ).resolves.toBe("<html/>");

    await session.alpha.rm("store", { recursive: true });
    await session.sync();
    expect(await session.beta.exists("store")).toBe(false);
  });

  it("create a file with simple filename, sync, delete, sync", async () => {
    session = await createTestSession({
      hot: false,
      full: false,
      beta: { remote: true },
    });

    // store/a.tsx
    await session.alpha.mkdir("store");
    await session.alpha.writeFile("store/a.txt", "<html/>");
    await session.sync();

    await expect(session.beta.readFile("store/a.txt", "utf8")).resolves.toBe(
      "<html/>",
    );

    await session.alpha.rm("store", { recursive: true });
    await session.sync();
    expect(await session.beta.exists("store")).toBe(false);
  });

  // TODO: This test fails!
  it.only("REMOTE: create a file with nextjs style filename, sync, delete, sync", async () => {
    session = await createTestSession({
      hot: false,
      full: false,
      beta: { remote: true },
    });

    // store/[[...page]].tsx
    await session.alpha.mkdir("store");
    await session.alpha.writeFile("store/[[...page]].tsx", "<html/>");
    await session.sync();

    await expect(
      session.beta.readFile("store/[[...page]].tsx", "utf8"),
    ).resolves.toBe("<html/>");

    await session.alpha.rm("store", { recursive: true });
    await session.sync();
    expect(await session.beta.exists("store")).toBe(false);
  });
});
