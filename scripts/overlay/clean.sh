set -ev

. env.sh

sudo umount "$root" || true
sudo rm -rf "$root" "$upperdir" "$upperdir2" "$workdir"

# work around HORRIBLE bug
rm -f $HOME/.local/share/reflect-sync/beta.db

reflect terminate o
