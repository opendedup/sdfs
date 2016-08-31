include ../build.conf
include ../jvm_ldpath.def

SRCS := javafs.c javafs_bindings.c
HDRS := javafs.h javafs_bindings.h
LIB_SO := libjavafs.so

INCLUDES := -I:/usr/include -I/usr/include/fuse/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux/

LDPATH := ${LDPATH} -L${FUSE_HOME}/lib -L/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/ -L/usr/lib/x86_64-linux-gnu/


all: ${LIB_SO}

${LIB_SO}: ${SRCS} ${HDRS}
	gcc -O2 -fno-omit-frame-pointer -fno-strict-aliasing -fPIC -shared -D_FILE_OFFSET_BITS=64 -o ${LIB_SO} ${INCLUDES} ${LDPATH} -ljvm -lfuse -lpthread ${SRCS}

clean:
	rm -f ${LIB_SO}
