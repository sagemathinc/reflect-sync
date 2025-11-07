import { withUpdatedMetadataHash } from "../hash-meta.js";

const fakeStats = (mode: number, uid = 0, gid = 0) =>
  ({
    mode,
    uid,
    gid,
  } as any);

describe("withUpdatedMetadataHash", () => {
  test("updates mode hash while preserving digest", () => {
    const original = "digest|1ed|0:0";
    const updated = withUpdatedMetadataHash(
      original,
      fakeStats(0o2755, 0, 0),
      true,
    );
    expect(updated).toBe("digest|5ed|0:0");
  });

  test("adds uid/gid when numericIds enabled", () => {
    const original = "digest|1ed";
    const updated = withUpdatedMetadataHash(
      original,
      fakeStats(0o777, 123, 456),
      true,
    );
    expect(updated).toBe("digest|1ff|123:456");
  });

  test("keeps existing third segment when numericIds disabled", () => {
    const original = "digest|1ed|7:8";
    const updated = withUpdatedMetadataHash(
      original,
      fakeStats(0o755, 123, 456),
      false,
    );
    expect(updated).toBe("digest|1ed|7:8");
  });

  test("returns null when hash malformed", () => {
    expect(
      withUpdatedMetadataHash("not-split", fakeStats(0o755), true),
    ).toBeNull();
  });
});
