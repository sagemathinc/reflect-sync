#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
config="$repo_root/runtime/rsync/runtime.json"

read_config() {
  "$NODE" -e "const fs=require('node:fs'); const x=JSON.parse(fs.readFileSync(process.argv[1])); process.stdout.write(String(x[process.argv[2]]));" "$config" "$1"
}

NODE=${NODE:-node}
version=$(read_config upstreamVersion)
runtime_version=$(read_config runtimeVersion)
source_url=$(read_config sourceUrl)
source_sha256=$(read_config sourceSha256)

kernel=$(uname -s)
machine=$(uname -m)
case "$kernel:$machine" in
  Linux:x86_64) target=linux-x64 ;;
  Linux:aarch64|Linux:arm64) target=linux-arm64 ;;
  Darwin:arm64) target=darwin-arm64 ;;
  *) echo "unsupported rsync runtime build target: $kernel/$machine" >&2; exit 1 ;;
esac

output=${1:-"$repo_root/dist/rsync-runtime/$target"}
case "$output" in
  /*) ;;
  *) output="$repo_root/$output" ;;
esac
if [ -e "$output" ]; then
  echo "output already exists: $output" >&2
  exit 1
fi

work_base=${RUNNER_TEMP:-${TMPDIR:-/tmp}}
work=$(mktemp -d "$work_base/reflect-rsync-build.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM
archive="$work/rsync-$version.tar.gz"
curl --proto '=https' --tlsv1.2 -fsSLo "$archive" "$source_url"
actual_sha256=$(
  "$NODE" -e "const fs=require('node:fs'),c=require('node:crypto'); const h=c.createHash('sha256'); h.update(fs.readFileSync(process.argv[1])); process.stdout.write(h.digest('hex'));" "$archive"
)
if [ "$actual_sha256" != "$source_sha256" ]; then
  echo "rsync source digest mismatch: expected $source_sha256, got $actual_sha256" >&2
  exit 1
fi

tar -xzf "$archive" -C "$work"
mkdir "$work/build"
cd "$work/build"

configure_flags="--with-included-popt --with-included-zlib --disable-openssl --disable-xxhash --disable-zstd --disable-lz4 --disable-iconv --disable-acl-support --disable-xattr-support --disable-roll-simd --disable-md5-asm --disable-md2man"
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-1788134400}
prefix_map="-ffile-prefix-map=$work/rsync-$version=/usr/src/rsync-$version -fmacro-prefix-map=$work/rsync-$version=/usr/src/rsync-$version -fdebug-prefix-map=$work/rsync-$version=/usr/src/rsync-$version"
export CFLAGS="${CFLAGS:--O2 -fno-ident} $prefix_map"

if [ "$kernel" = Linux ]; then
  if ! command -v musl-gcc >/dev/null 2>&1; then
    echo "musl-gcc is required for portable static Linux builds (install musl-tools)" >&2
    exit 1
  fi
  export CC=musl-gcc
  export LDFLAGS="${LDFLAGS:--static}"
  upstream_test_platform=linux
else
  export CC=${CC:-clang}
  export MACOSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-13.5}
  upstream_test_platform=darwin
fi

"$work/rsync-$version/configure" $configure_flags
make_jobs=${MAKE_JOBS:-2}
make -j "$make_jobs" rsync
upstream_test_exclusions=$(
  "$NODE" -e "const fs=require('node:fs'); const x=JSON.parse(fs.readFileSync(process.argv[1])); process.stdout.write((x.upstreamTestExclusions[process.argv[2]] || []).map((item) => item.name).join(','));" "$config" "$upstream_test_platform"
)
export RSYNC_EXCLUDE=$upstream_test_exclusions
upstream_test_log="$work/upstream-tests.log"
if ! make check CHECK_J="$make_jobs" >"$upstream_test_log" 2>&1; then
  cat "$upstream_test_log"
  exit 1
fi
cat "$upstream_test_log"

mkdir -p "$output/bin" "$output/licenses" "$output/source" "$output/tests"
install -m 755 rsync "$output/bin/rsync"
if [ "$kernel" = Darwin ]; then
  strip -x "$output/bin/rsync"
else
  strip "$output/bin/rsync"
fi
cp "$work/rsync-$version/COPYING" "$output/licenses/rsync-COPYING"
cp "$work/rsync-$version/README.md" "$output/licenses/rsync-README.md"
cp "$work/rsync-$version/NEWS.md" "$output/licenses/rsync-NEWS.md"
cp "$archive" "$output/source/rsync-$version.tar.gz"
cp "$repo_root/runtime/rsync/runtime.json" "$output/build-config.json"
cp "$repo_root/scripts/build-rsync-runtime.sh" "$output/source/build-rsync-runtime.sh"
cp "$upstream_test_log" "$output/tests/upstream.log"

"$NODE" "$repo_root/scripts/create-rsync-runtime-manifest.mjs" \
  "$output" "$target" "$runtime_version"
"$NODE" "$repo_root/scripts/test-rsync-runtime.mjs" "$output"

echo "built managed rsync runtime at $output"
