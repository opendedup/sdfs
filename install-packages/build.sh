sudo rm *.rpm
sudo rm *.deb
sudo rm deb/usr/share/sdfs/bin/libfuse.so.2
sudo rm deb/usr/share/sdfs/bin/libulockmgr.so.1
sudo rm deb/usr/share/sdfs/bin/libjavafs.so
sudo cp DEBIAN/libfuse.so.2 deb/usr/share/sdfs/bin/
sudo cp DEBIAN/libulockmgr.so.1 deb/usr/share/sdfs/bin/
sudo cp DEBIAN/libjavafs.so deb/usr/share/sdfs/bin/
sudo cp ../src/readme.txt deb/usr/share/sdfs/
sudo chown -R root:root ./deb
sudo fpm -s dir -t deb -n sdfs -v 3.0.1 -C deb/ -d fuse --url http://www.opendedup.org -d libxml2 -m sam.silverberg@gmail.com --vendor datishsystems --description "SDFS is an inline deduplication based filesystem"
sudo rm deb/usr/share/sdfs/bin/libfuse.so.2
sudo rm deb/usr/share/sdfs/bin/libulockmgr.so.1
sudo rm deb/usr/share/sdfs/bin/libjavafs.so
sudo cp RHEL/libfuse.so.2 deb/usr/share/sdfs/bin/
sudo cp RHEL/libulockmgr.so.1 deb/usr/share/sdfs/bin/
sudo cp RHEL/libjavafs.so deb/usr/share/sdfs/bin/
sudo fpm -s dir -t rpm -n sdfs -v 3.0.1 -C deb/ -d fuse --url http://www.opendedup.org -d libxml2 -m sam.silverberg@gmail.com --vendor datishsystems --description "SDFS is an inline deduplication based filesystem"
cp sdfs_3.0.1_amd64.deb ../../datish-iso/ubuntu-14.04.3-server-amd64/pool/extra/
sudo chown -R samsilverberg:samsilverberg ./deb
deb-s3 upload --bucket datishdeb sdfs_3.0.1_amd64.deb
