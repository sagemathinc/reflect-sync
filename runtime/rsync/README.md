# ReflectSync managed rsync runtime

ReflectSync uses an unmodified, separately executed upstream rsync build. It is
not linked into or embedded in ReflectSync. The runtime version, source URL,
source digest, configure flags, and initially supported targets are pinned in
`runtime.json`.

Linux artifacts are statically linked with musl. macOS artifacts use only Apple
system libraries. Optional OpenSSL, xxHash, zstd, LZ4, iconv, ACL, and xattr
dependencies are disabled in the initial portability-oriented build; bundled
zlib remains available for compression. Any future feature expansion must keep
the dependency closure explicit and pass the runtime conformance test.

ReflectSync models regular files, directories, and symlinks. It explicitly
disables rsync's device-node and special-file transfer modes; FIFOs, sockets,
and device nodes are outside the supported synchronization contract.

Rsync is GPL-3.0-or-later. Each binary artifact contains upstream `COPYING`,
`README.md`, and `NEWS.md`, the generated capability/build manifest, and the
upstream test log. A separately downloadable corresponding-source archive
contains the exact source tarball, its digest, the build recipe, and build
configuration. ReflectSync itself remains under its existing MIT license.

The upstream suite runs before packaging. Exclusions must be named and
justified in `runtime.json`; every other failure aborts the runtime build.
