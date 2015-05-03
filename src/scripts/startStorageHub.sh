#!/bin/bash

CONFIG=/etc/default/sdfs
[ -f $CONFIG ] && . $CONFIG

RUN_DIR=${RUN_DIR:-"/run/sdfs/"}

start_storage_hub(){
    "${JAVA_BIN:-"java"}" \
    -Dfuse.logging.level=INFO \
    -Xmx${MEM_MAX:-"2g"} \
    -Xms2g \
    -server \
    -XX:+UseCompressedOops \
    -XX:+DisableExplicitGC \
    -XX:+UseParallelGC \
    -XX:+UseParallelOldGC \
    -XX:ParallelGCThreads=4 \
    -XX:InitialSurvivorRatio=3 \
    -XX:TargetSurvivorRatio=90 \
    -Djava.awt.headless=true \
    -classpath ./bin/:./lib/* \
    org.opendedup.sdfs.network.ClusteredHCServer "$@"
}

start_storage_hub "$@"
