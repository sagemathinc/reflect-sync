// Render an argv array for logs and copy/paste diagnostics. Execution must
// still use spawn(command, args) rather than evaluating this string in a shell.
export function argsJoin(args: string[]): string {
  return args
    .map((arg) => {
      const hasWs = /\s/u.test(arg);
      const hasSingle = arg.includes("'");
      const hasDouble = arg.includes('"');
      if (!hasWs && !hasSingle && !hasDouble) return arg;
      if (hasSingle && !hasDouble) {
        return `"${arg.replace(/(["\\$`])/gu, "\\$1")}"`;
      }
      if (hasDouble && !hasSingle) return `'${arg}'`;
      return `"${arg.replace(/(["\\$`])/gu, "\\$1")}"`;
    })
    .join(" ");
}
