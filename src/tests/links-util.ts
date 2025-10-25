import fsp from "node:fs/promises";

export async function linkExists(p: string) {
  return !!(await fsp
    .lstat(p)
    .then((st) => st.isSymbolicLink())
    .catch(() => false));
}

export async function isRegularFile(p: string) {
  return !!(await fsp
    .lstat(p)
    .then((st) => st.isFile())
    .catch(() => false));
}

export async function readlinkTarget(p: string) {
  return await fsp.readlink(p);
}
