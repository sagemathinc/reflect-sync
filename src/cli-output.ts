import { AsciiTable3, AlignmentEnum } from "ascii-table3";

export function table(
  title: string,
  headings: string[],
  rows: string[][],
): string {
  if (!rows.length) return `No ${title.toLowerCase()}.`;
  const result = new AsciiTable3(title)
    .setHeading(...headings)
    .setStyle("unicode-round");
  headings.forEach((_, index) => result.setAlign(index, AlignmentEnum.LEFT));
  for (const row of rows) result.addRow(...row);
  return result.toString();
}

function fields(value: unknown, prefix = ""): string[][] {
  if (value !== null && typeof value === "object") {
    return Object.entries(value).flatMap(([key, item]) =>
      fields(item, prefix ? `${prefix}.${key}` : key),
    );
  }
  return [[prefix, value == null ? "-" : String(value)]];
}

export function output(
  value: unknown,
  json: boolean | undefined,
  title: string,
): void {
  process.stdout.write(
    (json
      ? JSON.stringify(value, null, 2)
      : table(title, ["Field", "Value"], fields(value))) + "\n",
  );
}

export async function batch(
  refs: string[],
  json: boolean | undefined,
  operation: (ref: string) => Promise<unknown>,
): Promise<void> {
  const results: {
    ref: string;
    ok: boolean;
    result?: unknown;
    error?: string;
  }[] = [];
  for (const ref of new Set(refs)) {
    try {
      results.push({ ref, ok: true, result: await operation(ref) });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      process.stderr.write(`${ref}: ${error}\n`);
      process.exitCode = 1;
      results.push({ ref, ok: false, error });
    }
  }
  if (json) output(results, true, "Results");
  else
    process.stdout.write(
      table(
        "Results",
        ["Reference", "Outcome", "Details"],
        results.map((row) => [
          row.ref,
          row.ok ? "ok" : "failed",
          row.error ??
            fields(row.result)
              .map(([key, value]) => `${key}: ${value}`)
              .join(", "),
        ]),
      ) + "\n",
    );
}
