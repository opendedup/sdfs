gsutil cp gs://${_DIST_BUCKET}/sdfscli/sdfscli-$BRANCH_NAME /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/sdfscli
rm -rf /workspace/install-packages/deb/usr/share/sdfs/sdfscli
cp /tmp/sdfscli-$BRANCH_NAME  /workspace/install-packages/deb/sbin/sdfscli 
cp /tmp/sdfscli-$BRANCH_NAME  /workspace/install-packages/deb/usr/share/sdfs/sdfscli
md5sum /workspace/install-packages/deb/sbin/sdfscli
md5sum /tmp/sdfscli-$BRANCH_NAME
rm /tmp/sdfscli-$BRANCH_NAME
gsutil cp gs://${_DIST_BUCKET}/hybrics-fs/mount.sdfs-$BRANCH_NAME /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/mount.sdfs
rm -rf /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
cp /tmp/mount.sdfs-$BRANCH_NAME  /workspace/install-packages/deb/sbin/mount.sdfs 
cp /tmp/mount.sdfs-$BRANCH_NAME  /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
ls -lah /workspace/install-packages/deb/sbin/

