
include build.conf

JAVAC := javac
JAVA := java

BUILD_CLASSPATH := lib/commons-logging-1.0.4.jar

JAVASRC := $(shell find src -name '*.java')
JNI_BINDINGS_H := jni/javafs_bindings.h
JNI_BINDINGS_C := jni/javafs_bindings.c
JNI_COMMON_H := javafs.h


.PHONY: all java_all jni_all clean java_clean jni_clean

all: java_all jni_all
	@echo ""
	@echo "FUSE-J is built."
	@echo ""

clean: java_clean jni_clean


# Java part

java_all: ${JAVASRC}
	${JAVAC} -Xlint -source 5 -classpath ${BUILD_CLASSPATH} -d build ${JAVASRC}

java_clean:
	rm -rf build/*



# JNI part

jni_all: ${JNI_BINDINGS_H} ${JNI_BINDINGS_C} jvm_ldpath.def
	${MAKE} -C jni all

${JNI_BINDINGS_H} ${JNI_BINDINGS_C}: ${JAVASRC}
	${JAVA} -classpath build java2c.CAPIGenerator \
	${JNI_BINDINGS_H} ${JNI_BINDINGS_C} ${JNI_COMMON_H}

jvm_ldpath.def:
	${JAVA} -classpath build java2c.DumpJVMLdPath > jvm_ldpath.def

jni_clean:
	${MAKE} -C jni clean
	rm -f ${JNI_BINDINGS_H} ${JNI_BINDINGS_C}
	rm -f jvm_ldpath.def



