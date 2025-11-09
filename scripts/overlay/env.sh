set -ev

# this is the path to the rootfs of an ubuntu image.
export lowerdir=$HOME/build/cocalc-lite/src/data/cache/images/docker.io/ubuntu\\:25.10
export target=/tmp/overlay-test

export root=$target/root
export upperdir=$target/upperdir
export upperdir2=$target/upperdir2
export workdir=$target/workdir

