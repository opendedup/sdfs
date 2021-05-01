export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
VERSION=master
DEBFILE="sdfs_${VERSION}-amd64.deb"
echo $DEBFILE
sudo rm -rf deb/usr/share/sdfs/lib/*
mvn package -f "../pom.xml"
cd install-packages
wget https://cdn.azul.com/zulu/bin/zulu11.35.13-ca-jdk11.0.5-linux_x64.tar.gz
rm -rf install-packages/deb/usr/share/sdfs/bin/jre
tar -xzvf zulu11.35.13-ca-jdk11.0.5-linux_x64.tar.gz
cp -rf zulu11.35.13-ca-jdk11.0.5-linux_x64 install-packages/deb/usr/share/sdfs/bin/jre
rm -rf zulu11.35.13-ca-jdk11.0.5-linux_x64 zulu11.35.13-ca-jdk11.0.5-linux_x64.tar.gz
cp ../target/sdfs-${VERSION}.jar deb/usr/share/sdfs/lib/sdfs.jar
cp ../target/lib/*.jar deb/usr/share/sdfs/lib/
sudo rm *.rpm
sudo rm *.deb
sudo cp ../src/readme.txt deb/usr/share/sdfs/
sudo chown -R root:root ./deb
sudo fpm -s dir -t deb -n sdfs -v $VERSION -C deb/ -d fuse --url http://www.opendedup.org -d libxml2 -d libxml2-utils -m sam.silverberg@gmail.com --vendor datishsystems --description "SDFS is an inline deduplication based filesystem"
sudo fpm -s dir -t rpm -n sdfs -v $VERSION -C deb/ -d fuse --url http://www.opendedup.org -d libxml2 -m sam.silverberg@gmail.com --vendor datishsystems --description "SDFS is an inline deduplication based filesystem"
#cp $DEBFILE ../../datish-iso/ubuntu-14.04.3-server-amd64/pool/extra/
sudo chown -R samsilverberg:samsilverberg ./deb
deb-s3 upload --bucket datishdeb $DEBFILE
