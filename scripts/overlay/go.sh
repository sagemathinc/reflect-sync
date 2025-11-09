set -ev

# you might want to run something like this, depending on test goals:
#
#  REFLECT_RSYNC_BWLIMIT=10M  REFLECT_TERMINATE_ON_CHANGE_ALPHA=1 REFLECT_VERY_VERBOSE=1 reflect daemon run


./clean.sh

. env.sh

mkdir -p $upperdir $workdir $root $upperdir2

echo "sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root"

sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root

cp -v run.sh stress.sh $root/

reflect terminate overlay
reflect create  --disable-micro-sync  --name=o $upperdir localhost:$upperdir2

podman run -it --rm --rootfs "$root" bash /stress.sh

#echo "manually run /run.sh to start the stress test"
#podman run -it --rm --rootfs "$root" bash