The setup is using overlayfs which has upper layer /tmp/overlay/upperdir, and doing linux install stuff
on that overlayfs. reflect-sync is ONLY syncing data from the upperdir to /tmp/overlay/upperdir2.
We are not pointing reflect-sync at an overlayfs mount, but instead at the upperdir, which is on a Linux
tmpfs.

beta = /tmp/overlay/upperdir
alpha = localhost:/tmp/overlay/upperdir2     # so over ssh

We do lots of IO on beta and never ever touch alpha,
so reflect sync should never schedule a sync operation
with target beta.