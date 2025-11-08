set -ev

# this is the path to the rootfs of an ubuntu image.
export lowerdir=$HOME/build/cocalc-lite/src/data/cache/images/docker.io/ubuntu\\:25.10
export target=/tmp/overlay-test

export root=/tmp/$target/root
export upperdir=/tmp/$target/upperdir
export upperdir2=/tmp/$target/upperdir2
export workdir=/tmp/$target/workdir

