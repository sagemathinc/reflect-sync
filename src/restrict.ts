import path from "node:path";

export function normalizeRestrictedInput(
  value: string | null | undefined,
): string | undefined {
  if (typeof value !== "string") return undefined;
  let normalized = value.trim();
  if (!normalized) return undefined;
  normalized = normalized.replace(/\\/g, "/");
  while (normalized.startsWith("./")) {
    normalized = normalized.slice(2);
  }
  normalized = normalized.replace(/^\/+/, "");
  normalized = normalized.replace(/\/{2,}/g, "/");
  normalized = normalized.replace(/\/+$/, "");
  if (!normalized || normalized === ".") return undefined;
  return normalized;
}

export function dedupeRestrictedList(values?: string[]): string[] {
  if (!Array.isArray(values) || !values.length) return [];
  const seen = new Set<string>();
  const cleaned: string[] = [];
  for (const raw of values) {
    const normalized = normalizeRestrictedInput(raw);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    cleaned.push(normalized);
  }
  return cleaned;
}

export function dirnameRel(rel: string): string {
  if (!rel) return "";
  const dir = path.posix.dirname(rel);
  return dir === "." ? "" : dir;
}

export function includeAncestors(paths: string[]): string[] {
  if (!paths.length) return [];
  const expanded = new Set<string>();
  const addChain = (rel: string) => {
    let current = rel;
    while (current) {
      if (expanded.has(current)) break;
      expanded.add(current);
      const parent = dirnameRel(current);
      if (!parent || parent === current) break;
      current = parent;
    }
  };
  for (const rel of paths) {
    if (!rel) continue;
    expanded.add(rel);
    addChain(dirnameRel(rel));
  }
  return Array.from(expanded);
}

export function collectListOption(
  value: string,
  previous: string[] = [],
): string[] {
  if (!value) return previous;
  const parts = value
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
  if (!parts.length) return previous;
  return previous.concat(parts);
}
