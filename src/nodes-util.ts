export type NodeKind = "f" | "d" | "l";

export type EntryKind = "file" | "dir" | "link" | "missing";

export function nodeKindToEntry(kind: string): EntryKind {
  switch (kind) {
    case "d":
      return "dir";
    case "l":
      return "link";
    default:
      return "file";
  }
}
