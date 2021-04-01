gsutil cp gs://$1/sdfscli/sdfscli-$2 /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/sdfscli
rm -rf /workspace/install-packages/deb/usr/share/sdfs/sdfscli
cp /tmp/sdfscli-$2  /workspace/install-packages/deb/sbin/sdfscli 
cp /tmp/sdfscli-$2  /workspace/install-packages/deb/usr/share/sdfs/sdfscli
md5sum /workspace/install-packages/deb/sbin/sdfscli
md5sum /tmp/sdfscli-$2
rm /tmp/sdfscli-$2
gsutil cp gs://$1/hybrics-fs/mount.sdfs-$2 /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/mount.sdfs
rm -rf /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
cp /tmp/mount.sdfs-$2  /workspace/install-packages/deb/sbin/mount.sdfs 
cp /tmp/mount.sdfs-$2  /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
ls -lah /workspace/install-packages/deb/sbin/

