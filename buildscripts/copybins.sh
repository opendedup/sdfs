gsutil cp gs://$1/sdfscli/sdfscli-$2 /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/sdfscli
rm -rf /workspace/install-packages/deb/usr/share/sdfs/sdfscli
cp /tmp/sdfscli-$2  /workspace/install-packages/deb/sbin/sdfscli 
cp /tmp/sdfscli-$2  /workspace/install-packages/deb/usr/share/sdfs/sdfscli
md5sum /workspace/install-packages/deb/sbin/sdfscli
md5sum /tmp/sdfscli-$2
chmod 777 /workspace/install-packages/deb/sbin/sdfscli
chmod 777 /workspace/install-packages/deb/usr/share/sdfs/sdfscli
rm /tmp/sdfscli-$2
gsutil cp gs://$1/hybrics-fs/mount.sdfs-$2 /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/mount.sdfs
rm -rf /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
cp /tmp/mount.sdfs-$2  /workspace/install-packages/deb/sbin/mount.sdfs 
cp /tmp/mount.sdfs-$2  /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
chmod 777 /workspace/install-packages/deb/sbin/mount.sdfs 
chmod 777 /workspace/install-packages/deb/usr/share/sdfs/mount.sdfs
ls -lah /workspace/install-packages/deb/sbin/
gsutil cp gs://$1/sdfs-proxy/sdfs-proxy-$2 /tmp/ 
rm -rf /workspace/install-packages/deb/sbin/sdfs-proxy
rm -rf /workspace/install-packages/deb/usr/share/sdfs/sdfs-proxy
cp /tmp/sdfs-proxy-$2  /workspace/install-packages/deb/sbin/sdfs-proxy 
cp /tmp/sdfs-proxy-$2  /workspace/install-packages/deb/usr/share/sdfs/sdfs-proxy
md5sum /workspace/install-packages/deb/sbin/sdfs-proxy
md5sum /tmp/sdfs-proxy-$2
chmod 777 /workspace/install-packages/deb/sbin/sdfs-proxy
chmod 777 /workspace/install-packages/deb/usr/share/sdfs/sdfs-proxy
rm /tmp/sdfs-proxy-$2

