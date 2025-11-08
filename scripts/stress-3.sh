set -ev

# this is the path to the rootfs of an ubuntu image.
lowerdir=$HOME/build/cocalc-lite/src/data/cache/images/docker.io/ubuntu\\:25.10


root=/tmp/t3/root
sudo umount $root || true


upperdir=/tmp/t3/upperdir
upperdir2=/tmp/t3/upperdir2
workdir=/tmp/t3/workdir


sudo rm -rf /tmp/t3
mkdir -p $upperdir $workdir $root $upperdir2

echo "sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root"

sudo mount -t overlay overlay -o lowerdir=$lowerdir,upperdir=$upperdir,workdir=$workdir,xino=off,metacopy=off,redirect_dir=off $root

cp stress-3-inside.sh $root/run.sh

reflect terminate t3
reflect create --name=t3 $upperdir localhost:$upperdir2



podman run -it --rm --rootfs "$root" bash