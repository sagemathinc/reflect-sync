import os from "node:os";
import path from "node:path";
import fsp from "node:fs/promises";

import { getDb } from "../db";
import { recordHotEvent, fetchHotEvents } from "../hot-events";

describe("hot-events", () => {
  let tmpDir: string;
  let dbPath: string;

  beforeAll(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "hot-events-"));
  });

  beforeEach(async () => {
    dbPath = path.join(tmpDir, `db-${Date.now()}.sqlite`);
    const db = getDb(dbPath);
    db.close();
  });

  afterAll(async () => {
    await fsp.rm(tmpDir, { recursive: true, force: true });
  });

  test("record and fetch events", () => {
    recordHotEvent(dbPath, "alpha", "foo/bar", "hotwatch");
    recordHotEvent(dbPath, "alpha", "foo/baz", "hotwatch");
    let events = fetchHotEvents(dbPath, 0, { side: "alpha" });
    expect(events.length).toBe(2);
    expect(events[0].path).toBe("foo/bar");
    expect(events[1].path).toBe("foo/baz");

    // Fetch again should be empty since entries are removed
    events = fetchHotEvents(dbPath, 0, { side: "alpha" });
    expect(events.length).toBe(0);
  });
});
