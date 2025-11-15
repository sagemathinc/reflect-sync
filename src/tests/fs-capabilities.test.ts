import fs from "node:fs";
import path from "node:path";
import {
  detectFilesystemCapabilities,
  canonicalizePath,
} from "../fs-capabilities.js";

const CASEFOLD_ROOT = path.join(process.cwd(), "casefold");
const NOT_CASEFOLD_ROOT = path.join(process.cwd(), "not-casefold");

const casefoldExists = fs.existsSync(CASEFOLD_ROOT);
const notCasefoldExists = fs.existsSync(NOT_CASEFOLD_ROOT);

const describeIfCasefold = casefoldExists ? describe : describe.skip;
const describeIfNotCasefold = notCasefoldExists ? describe : describe.skip;

describeIfCasefold("fs capabilities on casefold mount", () => {
  it("detects case insensitivity and unicode normalization", async () => {
    const caps = await detectFilesystemCapabilities(CASEFOLD_ROOT);
    expect(caps.caseInsensitive).toBe(true);
    expect(caps.normalizesUnicode).toBe(true);
  });

  it("canonicalizes casing and unicode", () => {
    const caps = { caseInsensitive: true, normalizesUnicode: true };
    const composed = canonicalizePath("école/FOO", caps);
    const decomposed = canonicalizePath("e\u0301cole/foo", caps);
    const expected = "école/foo".normalize("NFC").toLowerCase();
    expect(composed).toBe(expected);
    expect(decomposed).toBe(expected);
  });
});

describeIfNotCasefold("fs capabilities on normal mount", () => {
  it("detects case sensitive non-normalizing behavior", async () => {
    const caps = await detectFilesystemCapabilities(NOT_CASEFOLD_ROOT);
    expect(caps.caseInsensitive).toBe(false);
    expect(caps.normalizesUnicode).toBe(false);
  });
});
