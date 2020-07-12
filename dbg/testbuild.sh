export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export VERSION=3.10.9
umount /media/azure0
cd ../
mvn package
cd dbg
cp ../target/sdfs-${VERSION}-jar-with-dependencies.jar /usr/share/sdfs/lib/sdfs.jar
mount -t sdfs s3 /media/azure0/