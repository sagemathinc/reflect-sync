set -ev

. env.sh

sudo umount "$root" || true
sudo rm -rf "$root" "$upperdir" "$upperdir2" "$workdir"

reflect terminate o