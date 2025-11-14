set -ev

# you might want to run something like this, depending on test goals:
#
#  reflect daemon stop; REFLECT_RSYNC_BWLIMIT=10M SCHED_MAX_BACKOFF_MS=2000  SCHED_MIN_MS=2000 SCHED_MAX_MS=2000 REFLECT_TERMINATE_ON_CHANGE_ALPHA=1 REFLECT_VERY_VERBOSE=1 reflect daemon run

pnpm build

./clean.sh

. env.sh

mkdir -p $upperdir $workdir $root $upperdir2

echo "sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root"

sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root

cp -v run.sh stress.sh $root/

reflect terminate o || true

export REFLECT_LOG_LEVEL=debug
export REFLECT_TRACE_ALL=1
export SCHED_MIN_MS="1000"
export SCHED_MAX_MS="2000"
export SCHED_MAX_BACKOFF_MS="2000"
reflect daemon stop
reflect daemon start

# this fails very quickly with fb68a88f18ae81ec5cf5c546226399fcfd825349

#reflect create --disable-hot-sync  --name=o $upperdir2 $upperdir


#reflect create --name=o  --disable-full-cycle  $upperdir2 $upperdir

reflect create --name=o $upperdir2 $upperdir

# passed 17 in a row with fb68a88f18ae81ec5cf5c546226399fcfd825349
#reflect create --disable-hot-sync  --name=o $upperdir2 $upperdir

# trying this with fb68a88f18ae81ec5cf5c546226399fcfd825349:
#reflect create --disable-hot-sync  --name=o localhost:$upperdir2 $upperdir

#reflect create --name=o localhost:$upperdir2 $upperdir

#podman run -it --rm --rootfs "$root" bash /stress.sh

#echo "manually run /run.sh to start the stress test"
podman run -it --rm --rootfs "$root" /run.sh
