export VERSION=master
sudo umount /media/pool0
sudo killall -9 jsvc
#sudo rm -rf /opt/sdfs/volumes/gcs0/
cd ../
rm -rf target/classes/
rm -rf target/sdfs*.jar
mvn package
cd dbg
sudo cp ../target/sdfs-${VERSION}.jar /usr/share/sdfs/lib/sdfs.jar
sudo mount -t sdfs gcs0 /media/pool0/