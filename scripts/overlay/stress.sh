set -ev

apt-get update
apt-get install -y latexmk

# apt-get install -y git
# git clone https://github.com/sagemathinc/cocalc
# cd cocalc
# git log |wc -l
# cd ..
# rm -rf cocalc

apt-get remove -y latexmk git
apt-get autoremove -y
sync