export VERSION=3.11.0
sudo umount /media/pool0
cd ../
rm -rf target/classes/
rm -rf target/sdfs*.jar
mvn package
cd dbg
sudo cp ../target/sdfs-${VERSION}-jar-with-dependencies.jar /usr/share/sdfs/lib/sdfs.jar
sudo mount -t sdfs pool0 /media/pool0/