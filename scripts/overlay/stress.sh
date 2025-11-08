set -ev

apt-get update
apt-get install -y latexmk
apt-get remove -y latexmk
apt-get autoremove -y
sync