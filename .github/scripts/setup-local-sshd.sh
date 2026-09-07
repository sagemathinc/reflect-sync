#!/bin/sh
set -eu

sudo apt-get update
sudo apt-get install --no-install-recommends -y openssh-server rsync

project_root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
node_path=$(command -v node)

install -d -m 755 "$HOME/.local/bin"
rm -f "$HOME/.local/bin/reflect-sync"
{
  printf '%s\n' '#!/bin/sh'
  printf 'exec "%s" "%s" "$@"\n' "$node_path" "$project_root/bin/reflect-sync.mjs"
} > "$HOME/.local/bin/reflect-sync"
chmod 755 "$HOME/.local/bin/reflect-sync"

install -d -m 700 "$HOME/.ssh"
if [ ! -f "$HOME/.ssh/id_ed25519" ]; then
  ssh-keygen -q -t ed25519 -N '' -f "$HOME/.ssh/id_ed25519"
fi
touch "$HOME/.ssh/authorized_keys"
if ! grep -qF "$(cat "$HOME/.ssh/id_ed25519.pub")" "$HOME/.ssh/authorized_keys"; then
  cat "$HOME/.ssh/id_ed25519.pub" >> "$HOME/.ssh/authorized_keys"
fi
chmod 600 "$HOME/.ssh/authorized_keys"

sudo install -d -m 755 /run/sshd
if ! ssh-keyscan -T 2 localhost >/dev/null 2>&1; then
  sudo /usr/sbin/sshd
fi
ssh-keyscan -H localhost >> "$HOME/.ssh/known_hosts" 2>/dev/null
chmod 600 "$HOME/.ssh/known_hosts"
ssh -o BatchMode=yes localhost true
ssh -o BatchMode=yes localhost 'test -x "$HOME/.local/bin/reflect-sync"'
