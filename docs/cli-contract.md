# Reflect CLI Contract

This is the public CLI contract for the coordinated 0.17.0 release. Old
top-level sync commands and experimental Jupyter command aliases are removed.

## Resources

- `reflect sync`: persistent bidirectional file-sync configurations.
- `reflect forward`: persistent SSH port-forward configurations.
- `reflect jupyter`: individual kernel executions, each with independent state.
- `reflect jupyter target`: named, reusable local kernelspec registrations.
- `reflect jupyter environment`: remote managed Python environment preparation.
- `reflect jupyter discover`: read-only remote kernel/hardware discovery.
- `reflect jupyter ssh-targets`: local SSH alias discovery.

`reflect daemon` is shared process supervision, not a sync resource. Installation,
doctor, and filesystem diagnostics remain global utilities. Sync plumbing lives
under `reflect sync`; no top-level sync convenience aliases remain.

## Verbs

- `create`: create a sync/forward configuration; start by default, or use
  `--stopped`. `jupyter target add` prepares or selects a remote kernel and
  registers its local kernelspec. It does not launch a kernel.
- `list [ID-or-name...]`: list resource records without contacting every remote
  machine. Jupyter records not confirmed stopped are explicitly unverified.
- `status ID`: inspect execution state. Unreachable is an error, not stopped.
- `start`, `stop`, `restart`: supported for sync and forwarding. Stop retains
  configuration and waits for the local process to exit. Start is idempotent.
- `jupyter interrupt ID` and `jupyter stop ID`: interrupt computation or end
  a kernel execution. Stopping loses in-memory language state, not its target.
  A Jupyter client launches a new execution through the specialized foreground
  `jupyter launch --target NAME --connection-file PATH` interface.
- `remove ID`: remove management state only after execution is stopped.
  `--stop` explicitly permits stopping first. A failed stop or unreachable
  remote prevents removal. `jupyter target remove NAME --stop` additionally
  disables new launches before stopping executions and removing registration.
  Removal never deletes users' synced files, remote environments, or VMs.
- `sync flush ID`: request convergence now (formerly top-level `sync ID`).
  Domain-specific edit, diff, logs, monitor, reset, and query operations remain.

There is no `terminate` alias. There is no generic Jupyter `start` that promises
to resume a stopped interpreter. Remote environment deletion is not provided.

## Identifiers

Sync, forward, and Jupyter session handles are positive local integers, scoped
to their domain and local state directory/database. Handles are persistent and
not reused after deletion. Jupyter retains UUIDs as remote execution identities
and accepts exact UUIDs as selectors; JSON exposes both `id` and `session`.
Configured names are accepted for sync/forward; Jupyter targets use names.
A target name is not a session selector because many notebooks may use it.
Unknown selectors fail; numeric-only configured sync/forward names are rejected.

## Output And Failure

Core create/list/status/start/stop/restart/remove commands and Jupyter management
commands offer `--json`. Success writes one JSON value to stdout; human mode
uses readable messages or the shared rounded table style. Empty lists are `[]`.
Logs/monitor are streams and use NDJSON with `--json`. Flush progress goes to
stderr so its JSON result remains parseable. Diagnostics go to stderr.

Errors exit nonzero and do not print a success result for a failed operation.
Batch operations may partially succeed; their JSON result identifies each
success/failure and the command still exits nonzero if any item failed.
Options such as `--session-db` and `--log-level` work before or after the domain
name. JSON and human output use the same operational path.

## Examples

```sh
reflect sync create ./course student@vm:/home/student/course --name course
reflect sync list --json
reflect sync stop course
reflect sync remove course
reflect forward create :8888 student@vm:8888 --name notebook
reflect forward remove notebook --stop
reflect jupyter discover --host gpu
reflect jupyter target add gpu --host gpu --environment teaching
reflect jupyter list
reflect jupyter status 12 --json
reflect jupyter stop 12
reflect jupyter remove 12
reflect jupyter target remove gpu --stop
```
