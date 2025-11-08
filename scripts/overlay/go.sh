set -ev

./clean.sh

. env.sh

mkdir -p $upperdir $workdir $root $upperdir2

echo "sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root"

sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root

cp -v run.sh stress.sh $root/

reflect terminate overlay
reflect create  --disable-micro-sync  --name=o $upperdir $upperdir2

echo "manually run /run.sh to start the stress test"
podman run -it --rm --rootfs "$root" bash