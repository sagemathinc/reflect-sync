import fsp from "node:fs/promises";
import path from "node:path";
import { Database } from "../../db.js";
import { stringDigest, defaultHashAlg, modeHash } from "../../hash.js";
import { createTestSession } from "./env.js";

jest.setTimeout(20000);

describe("sync maintenance options", () => {
  it("vacuum flag triggers vacuum log entry", async () => {
    const session = await createTestSession({ hot: false, full: false });
    try {
      // Populate a little data then vacuum
      await session.alpha.writeFile("a.txt", "hello");
      await session.sync();
      await session.sync({ extraArgs: ["--vacuum"] });

      const db = new Database(session.sessionDb);
      const rows = db
        .prepare(
          `SELECT message FROM session_logs WHERE message = 'vacuum' ORDER BY id DESC LIMIT 1`,
        )
        .all() as { message: string }[];
      db.close();
      expect(rows.length).toBe(1);
    } finally {
      await session.dispose();
    }
  });

  it("rehash flag forces hashing even when metadata is unchanged", async () => {
    const session = await createTestSession({ hot: false, full: false });
    try {
      const rel = "foo.txt";
      const alphaPath = path.join(session.alpha.path(""), rel);

      await session.alpha.writeFile(rel, "AAAA");
      await session.sync();

      const db = new Database(session.alphaDbPath);
      const beforeRow = db
        .prepare(`SELECT hash, mtime, ctime, size FROM nodes WHERE path = ?`)
        .get(rel) as {
        hash: string;
        mtime: number;
        ctime: number;
        size: number;
      };
      const v1Hash = beforeRow.hash;

      // Change content but keep size identical.
      await session.alpha.writeFile(rel, "BBBB");
      const st = await fsp.stat(alphaPath);
      // Pretend metadata didn't change: update DB to match current mtime/ctime/size but keep old hash.
      db.prepare(
        `UPDATE nodes SET hash = ?, hashed_ctime = ?, mtime = ?, ctime = ?, size = ?, hash_pending = 0, deleted = 0 WHERE path = ?`,
      ).run(v1Hash, st.ctimeMs, st.mtimeMs, st.ctimeMs, st.size, rel);
      db.close();

      // Without --rehash the stale hash should remain.
      await session.sync();
      let dbCheck = new Database(session.alphaDbPath);
      const stale = dbCheck
        .prepare(`SELECT hash FROM nodes WHERE path = ?`)
        .get(rel) as { hash: string };
      dbCheck.close();
      expect(stale.hash).toBe(v1Hash);

      // With --rehash restricted to the file, hash should update to new content.
      await session.sync({
        paths: [rel],
        extraArgs: ["--rehash"],
      });
      dbCheck = new Database(session.alphaDbPath);
      const updated = dbCheck
        .prepare(`SELECT hash FROM nodes WHERE path = ?`)
        .get(rel) as { hash: string };
      dbCheck.close();

      const expectedHash = `${stringDigest(defaultHashAlg(), "BBBB")}|${modeHash((await fsp.stat(alphaPath)).mode)}`;
      expect(updated.hash).toBe(expectedHash);
    } finally {
      await session.dispose();
    }
  });
});

