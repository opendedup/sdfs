FROM gcr.io/hybrics/hybrics-base:master AS builder
ARG DEBIAN_FRONTEND=noninteractive
LABEL email=sam.silverberg@gmail.com
LABEL author="Sam Silverberg"

COPY pom.xml /sdfs-build/
COPY src /sdfs-build/src/
COPY .git /sdfs-build/.git
COPY install-packages /sdfs-build/install-packages/
WORKDIR "/sdfs-build"
RUN DEBIAN_FRONTEND="noninteractive" apt update && DEBIAN_FRONTEND="noninteractive" apt upgrade -y && DEBIAN_FRONTEND="noninteractive" apt install -y \
    -y nsis
RUN wget https://nsis.sourceforge.io/mediawiki/images/7/7f/EnVar_plugin.zip && \
    unzip EnVar_plugin.zip -d /usr/share/nsis/
RUN wget https://nsis.sourceforge.io/mediawiki/images/4/4a/AccessControl.zip && \
    unzip AccessControl.zip -d /usr/share/nsis/ && \
    cp /usr/share/nsis/Plugins/i386-unicode/AccessControl.dll /usr/share/nsis/Plugins/x86-unicode/
RUN wget https://cdn.azul.com/zulu/bin/zulu11.48.21-ca-jre11.0.11-win_x64.zip && \
    rm -rf install-packages/windows/bin/jre && \
    unzip zulu11.48.21-ca-jre11.0.11-win_x64.zip -d /tmp/ && \
    mv /tmp/zulu11.48.21-ca-jre11.0.11-win_x64  install-packages/windows/bin/jre

RUN wget https://cdn.azul.com/zulu/bin/zulu11.35.13-ca-jdk11.0.5-linux_x64.tar.gz && \
    rm -rf install-packages/deb/usr/share/sdfs/bin/jre && \
    tar -xzvf zulu11.35.13-ca-jdk11.0.5-linux_x64.tar.gz && \
    mkdir -p install-packages/deb/usr/share/sdfs/bin/ && \
    cp -rf zulu11.35.13-ca-jdk11.0.5-linux_x64 install-packages/deb/usr/share/sdfs/bin/jre
RUN DEBIAN_FRONTEND="noninteractive" apt update && DEBIAN_FRONTEND="noninteractive" apt upgrade -y && DEBIAN_FRONTEND="noninteractive" apt install -y \
        git
ENV VERSION=master
ENV PKG_VERSION=0.0.1
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
ENV DEBFILE="sdfs_${VERSION}_amd64.deb"
RUN echo $DEBFILE
WORKDIR "/sdfs-build/install-packages/"
RUN rm -rf deb/usr/share/sdfs/lib/*
WORKDIR "/sdfs-build/"
RUN mvn package


RUN cp target/lib/*.jar install-packages/deb/usr/share/sdfs/lib/ && \
    cp target/sdfs-${VERSION}.jar install-packages/deb/usr/share/sdfs/lib/sdfs.jar && \
    cp target/sdfs-${VERSION}.jar install-packages
WORKDIR "/sdfs-build/install-packages/windows"
RUN makensis -V4 -DVERSION=${PKG_VERSION} -DJARVERSION=${VERSION} sdfs_win.nsi 
WORKDIR "/sdfs-build/install-packages/"
RUN chmod 777 deb/sbin/sdfscli && \
    chmod 777 deb/usr/share/sdfs/sdfscli

RUN rm -rf *.deb *.rpm && \
    cp ../src/readme.txt deb/usr/share/sdfs/ && \
    fpm -s dir -t deb -n sdfs -v $PKG_VERSION -C deb/ -d fuse --url http://www.opendedup.org -d libxml2 -d libxml2-utils -m sam.silverberg@gmail.com --vendor datishsystems --description "SDFS is an inline deduplication based filesystem" && \
    fpm -s dir -t rpm -n sdfs -v $PKG_VERSION -C deb/ -d fuse --url http://www.opendedup.org -d libxml2 -m sam.silverberg@gmail.com --vendor datishsystems --description "SDFS is an inline deduplication based filesystem" 
WORKDIR "/sdfs-build/install-packages/"
RUN echo "tar cvf - sdfs-${VERSION}.jar sdfs_${PKG_VERSION}_amd64.deb sdfs-${PKG_VERSION}-1.x86_64.rpm SDFS-${PKG_VERSION}-Setup.exe" > export_data.sh && \
    chmod 700 export_data.sh
ENTRYPOINT tar cvf - sdfs-${VERSION}.jar sdfs_${PKG_VERSION}_amd64.deb sdfs-${PKG_VERSION}-1.x86_64.rpm SDFS-${PKG_VERSION}-Setup.exe
FROM ubuntu:20.04
ENV VERSION=master
ENV PKG_VERSION=0.0.1
LABEL email=samsilverberg@google.com
LABEL author="Sam Silverberg"
RUN DEBIAN_FRONTEND="noninteractive" apt update && DEBIAN_FRONTEND="noninteractive" apt upgrade -y && DEBIAN_FRONTEND="noninteractive" apt install -y \
		openjdk-11-jdk \
        maven \
        libfuse2 \
        ssh \
        openssh-server \
        jsvc \
        libxml2 \
        ruby-dev \
        build-essential \
        libxml2-utils \
        fuse
WORKDIR "/tmp"
COPY --from=0 /sdfs-build/install-packages/sdfs_${PKG_VERSION}_amd64.deb .
RUN dpkg -i sdfs_${PKG_VERSION}_amd64.deb && \
    rm sdfs_${PKG_VERSION}_amd64.deb
RUN echo "* hard nofile 65535" >> /etc/security/limits.conf
RUN	echo "* soft nofile 65535" >> /etc/security/limits.conf
COPY --from=0 /sdfs-build/install-packages/docker_run.sh /usr/share/sdfs/docker_run.sh
RUN chmod 700 /usr/share/sdfs/docker_run.sh
ENV DOCKER_DETATCH="-nodetach"
ENV CAPACITY=1TB

CMD ["/usr/share/sdfs/docker_run.sh"]