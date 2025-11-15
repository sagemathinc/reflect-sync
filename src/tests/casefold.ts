import fs from "node:fs";
import path from "node:path";

export const CASEFOLD_ROOT = path.join(process.cwd(), "casefold");
export const NOT_CASEFOLD_ROOT = path.join(process.cwd(), "not-casefold");

export const hasCasefoldRoot = fs.existsSync(CASEFOLD_ROOT);
export const hasNotCasefoldRoot = fs.existsSync(NOT_CASEFOLD_ROOT);

export const describeIfCasefold = hasCasefoldRoot ? describe : describe.skip;
export const describeIfNotCasefold = hasNotCasefoldRoot
  ? describe
  : describe.skip;
