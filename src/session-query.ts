import path from "node:path";
import { getDb } from "./db.js";
import { expandHome } from "./remote.js";
import { toRel, toAbs } from "./path-rel.js";
import {
  deriveSessionPaths,
  type SessionRow,
  type Side,
} from "./session-db.js";

export type SessionSide = Side;

type QueryPathKind = "all" | "dir" | "file";

interface QueryContext {
  side: SessionSide;
  dbPath: string;
  host?: string | null;
  port?: number | null;
  rootPosix: string;
  rootDisplay: string;
  rootLocalAbs?: string;
  displayPrefix: string;
  pathProvided: boolean;
  relPath: string | null;
  absPosix: string;
  absDisplay: string;
}

export interface QuerySizeResult {
  side: SessionSide;
  rootPath: string;
  targetPath: string;
  relativePath: string | null;
  pathProvided: boolean;
  totalBytes: number;
  fileCount: number;
  kind: QueryPathKind;
}

export interface RecentFileRow {
  relativePath: string;
  absolutePath: string;
  size: number;
  mtime: number;
  ctime: number;
}

export interface QueryRecentResult {
  side: SessionSide;
  rootPath: string;
  targetPath: string;
  relativePath: string | null;
  pathProvided: boolean;
  limit: number;
  hasMore: boolean;
  files: RecentFileRow[];
}

export async function querySize(opts: {
  session: SessionRow;
  side: SessionSide;
  path?: string;
}): Promise<QuerySizeResult> {
  const ctx = await buildContext(opts.session, opts.side, opts.path);
  const db = getDb(ctx.dbPath);
  try {
    const { kind, totalBytes, fileCount } = computeSize(db, ctx.relPath);
    return {
      side: ctx.side,
      rootPath: ctx.rootDisplay,
      targetPath: ctx.absDisplay,
      relativePath: ctx.relPath,
      pathProvided: ctx.pathProvided,
      totalBytes,
      fileCount,
      kind,
    };
  } finally {
    db.close();
  }
}

export async function queryRecent(opts: {
  session: SessionRow;
  side: SessionSide;
  path?: string;
  limit: number;
}): Promise<QueryRecentResult> {
  const ctx = await buildContext(opts.session, opts.side, opts.path);
  const db = getDb(ctx.dbPath);
  try {
    const kind = determinePathKind(db, ctx.relPath);
    const limit = Math.max(1, opts.limit | 0);
    const rows = fetchRecentFiles(db, ctx, kind, limit);
    return {
      side: ctx.side,
      rootPath: ctx.rootDisplay,
      targetPath: ctx.absDisplay,
      relativePath: ctx.relPath,
      pathProvided: ctx.pathProvided,
      limit,
      hasMore: rows.hasMore,
      files: rows.files,
    };
  } finally {
    db.close();
  }
}

async function buildContext(
  session: SessionRow,
  side: SessionSide,
  requestedPath?: string,
): Promise<QueryContext> {
  const dbPath = resolveDbPath(session, side);
  const rootRaw = side === "alpha" ? session.alpha_root : session.beta_root;
  if (!rootRaw) {
    throw new Error(`Session ${session.id} is missing ${side} root`);
  }

  const host = side === "alpha" ? session.alpha_host : session.beta_host;
  const port =
    side === "alpha"
      ? (session.alpha_port ?? undefined)
      : (session.beta_port ?? undefined);

  const { rootPosix, rootDisplay, rootLocalAbs, displayPrefix } =
    await resolveRootInfo(rootRaw, host ?? undefined, port);

  const pathProvided = !!(requestedPath && requestedPath.trim());
  const relPath =
    pathProvided || requestedPath === ""
      ? await resolveRelativePath(requestedPath ?? "", {
          rootPosix,
          rootLocalAbs,
          host: host ?? undefined,
          port,
          displayPrefix,
          rootDisplay,
        })
      : null;

  const absPosix = relPath === null ? rootPosix : toAbs(relPath, rootPosix);
  const absDisplay = host
    ? `${displayPrefix}${absPosix}`
    : relPath === null
      ? rootLocalAbs!
      : resolveLocalDisplayPath(rootLocalAbs!, relPath);

  return {
    side,
    dbPath,
    host,
    port,
    rootPosix,
    rootDisplay,
    rootLocalAbs,
    displayPrefix,
    pathProvided,
    relPath,
    absPosix,
    absDisplay,
  };
}

async function resolveRootInfo(
  root: string,
  host?: string,
  port?: number | null,
) {
  if (host) {
    const expanded = await expandHome(root, host, undefined, port ?? undefined);
    const normalized = path.posix.normalize(expanded);
    const prefix = `${host}${port ? `:${port}` : ""}:`;
    return {
      rootPosix: normalized,
      rootDisplay: `${prefix}${normalized}`,
      rootLocalAbs: undefined,
      displayPrefix: prefix,
    };
  }

  const expanded = await expandHome(root);
  const resolved = path.resolve(expanded);
  const posix = normalizeLocalPosix(resolved);
  return {
    rootPosix: posix,
    rootDisplay: resolved,
    rootLocalAbs: resolved,
    displayPrefix: "",
  };
}

async function resolveRelativePath(
  input: string,
  ctx: {
    rootPosix: string;
    rootLocalAbs?: string;
    host?: string;
    port?: number | null;
    displayPrefix: string;
    rootDisplay: string;
  },
): Promise<string> {
  if (!input.trim()) return "";
  if (ctx.host) {
    const abs = await resolveRemoteAbsolute(
      input,
      ctx.rootPosix,
      ctx.host,
      ctx.port ?? undefined,
    );
    ensureWithin(
      ctx.rootPosix,
      abs,
      `${ctx.displayPrefix}${abs}`,
      ctx.rootDisplay,
    );
    return toRel(abs, ctx.rootPosix);
  }

  const rootLocal = ctx.rootLocalAbs!;
  const absLocal = await resolveLocalAbsolute(input, rootLocal);
  const absPosix = normalizeLocalPosix(absLocal);
  ensureWithin(ctx.rootPosix, absPosix, absLocal, ctx.rootDisplay);
  return toRel(absPosix, ctx.rootPosix);
}

async function resolveLocalAbsolute(
  pathInput: string,
  rootAbs: string,
): Promise<string> {
  const trimmed = pathInput.trim();
  if (!trimmed) return rootAbs;
  if (trimmed === "~" || trimmed.startsWith("~/")) {
    const expanded = await expandHome(trimmed);
    return path.resolve(expanded);
  }
  if (path.isAbsolute(trimmed)) {
    return path.resolve(trimmed);
  }
  return path.resolve(rootAbs, trimmed);
}

async function resolveRemoteAbsolute(
  pathInput: string,
  rootPosix: string,
  host: string,
  port?: number,
): Promise<string> {
  const trimmed = pathInput.trim();
  if (!trimmed) return rootPosix;
  if (trimmed === "~" || trimmed.startsWith("~/")) {
    const expanded = await expandHome(trimmed, host, undefined, port);
    return path.posix.normalize(expanded);
  }
  if (trimmed.startsWith("/")) {
    return path.posix.normalize(trimmed);
  }
  return path.posix.resolve(rootPosix, trimmed);
}

function resolveLocalDisplayPath(rootAbs: string, rel: string): string {
  return rel ? path.resolve(rootAbs, rel) : rootAbs;
}

function ensureWithin(
  rootPosix: string,
  targetPosix: string,
  displayPath: string,
  rootDisplay: string,
) {
  if (rootPosix === "/") {
    if (!targetPosix.startsWith("/")) {
      throw new Error(
        `Path '${displayPath}' is outside session root '${rootDisplay}'`,
      );
    }
    return;
  }
  if (
    targetPosix !== rootPosix &&
    !targetPosix.startsWith(
      rootPosix.endsWith("/") ? rootPosix : `${rootPosix}/`,
    )
  ) {
    throw new Error(
      `Path '${displayPath}' is outside session root '${rootDisplay}'`,
    );
  }
}

function normalizeLocalPosix(value: string): string {
  return path.posix.normalize(value.split(path.sep).join("/"));
}

function resolveDbPath(session: SessionRow, side: SessionSide): string {
  if (side === "alpha" && session.alpha_db?.trim()) {
    return session.alpha_db.trim();
  }
  if (side === "beta" && session.beta_db?.trim()) {
    return session.beta_db.trim();
  }
  const derived = deriveSessionPaths(session.id);
  return side === "alpha" ? derived.alpha_db : derived.beta_db;
}

function computeSize(
  db: ReturnType<typeof getDb>,
  rel: string | null,
): { kind: QueryPathKind; totalBytes: number; fileCount: number } {
  if (rel === null || rel === "") {
    const kind: QueryPathKind = rel === null ? "all" : "dir";
    const row = db
      .prepare(
        `SELECT COALESCE(SUM(COALESCE(size, 0)), 0) AS total, COUNT(*) AS count
         FROM files
         WHERE deleted = 0`,
      )
      .get() as { total: number; count: number };
    return {
      kind,
      totalBytes: row?.total ?? 0,
      fileCount: row?.count ?? 0,
    };
  }

  const fileRow = db
    .prepare(
      `SELECT COALESCE(size, 0) AS size
         FROM files
        WHERE deleted = 0 AND path = ?`,
    )
    .get(rel) as { size: number } | undefined;
  if (fileRow) {
    return {
      kind: "file",
      totalBytes: fileRow.size ?? 0,
      fileCount: 1,
    };
  }

  ensureDirectoryExists(db, rel);
  const likeParam = `${rel}/%`;
  const row = db
    .prepare(
      `SELECT COALESCE(SUM(COALESCE(size, 0)), 0) AS total, COUNT(*) AS count
         FROM files
        WHERE deleted = 0 AND path LIKE ?`,
    )
    .get(likeParam) as { total: number; count: number };
  return {
    kind: "dir",
    totalBytes: row?.total ?? 0,
    fileCount: row?.count ?? 0,
  };
}

function determinePathKind(
  db: ReturnType<typeof getDb>,
  rel: string | null,
): QueryPathKind {
  if (rel === null) return "all";
  if (rel === "") return "dir";

  const fileRow = db
    .prepare(
      `SELECT 1
         FROM files
        WHERE deleted = 0 AND path = ?
        LIMIT 1`,
    )
    .get(rel);
  if (fileRow) return "file";

  if (directoryExists(db, rel)) return "dir";

  ensureDirectoryExists(db, rel);
  return "dir";
}

function directoryExists(db: ReturnType<typeof getDb>, rel: string): boolean {
  const dirRow = db
    .prepare(
      `SELECT 1
         FROM dirs
        WHERE deleted = 0 AND path = ?
        LIMIT 1`,
    )
    .get(rel);
  if (dirRow) return true;
  const childRow = db
    .prepare(
      `SELECT 1
         FROM files
        WHERE deleted = 0 AND path LIKE ?
        LIMIT 1`,
    )
    .get(`${rel}/%`);
  if (childRow) return true;
  const subdirRow = db
    .prepare(
      `SELECT 1
         FROM dirs
        WHERE deleted = 0 AND path LIKE ?
        LIMIT 1`,
    )
    .get(`${rel}/%`);
  return !!subdirRow;
}

function ensureDirectoryExists(db: ReturnType<typeof getDb>, rel: string) {
  if (directoryExists(db, rel)) return;
  throw new Error(
    `Path '${rel}' is not tracked by this session (possible ignore or out of scope)`,
  );
}

function fetchRecentFiles(
  db: ReturnType<typeof getDb>,
  ctx: QueryContext,
  kind: QueryPathKind,
  limit: number,
) {
  const params: any[] = [];
  let where = "deleted = 0";
  if (kind === "file" && ctx.relPath) {
    where += " AND path = ?";
    params.push(ctx.relPath);
  } else if (ctx.relPath && ctx.relPath !== "") {
    where += " AND (path = ? OR path LIKE ?)";
    params.push(ctx.relPath, `${ctx.relPath}/%`);
  }

  const stmt = db.prepare(
    `SELECT path, COALESCE(size, 0) AS size, mtime, ctime
       FROM files
      WHERE ${where}
      ORDER BY mtime DESC, path ASC
      LIMIT ?`,
  );
  const rows = stmt.all(...params, limit + 1) as {
    path: string;
    size: number;
    mtime: number;
    ctime: number;
  }[];

  const hasMore = rows.length > limit;
  const slice = hasMore ? rows.slice(0, limit) : rows;
  const files: RecentFileRow[] = slice.map((row) => {
    const absPosix = toAbs(row.path, ctx.rootPosix);
    if (ctx.host) {
      return {
        relativePath: row.path,
        absolutePath: `${ctx.displayPrefix}${absPosix}`,
        size: row.size ?? 0,
        mtime: row.mtime,
        ctime: row.ctime,
      };
    }
    const absLocal = resolveLocalDisplayPath(ctx.rootLocalAbs!, row.path);
    return {
      relativePath: row.path,
      absolutePath: absLocal,
      size: row.size ?? 0,
      mtime: row.mtime,
      ctime: row.ctime,
    };
  });

  return { files, hasMore };
}
