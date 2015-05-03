#!/bin/bash

CONFIG=/etc/default/sdfs
[ -f $CONFIG ] && . $CONFIG

RUN_DIR=${RUN_DIR:-"/run/sdfs/"}

sdfs_cli(){
    "${JAVA_BIN:-"java"}"\
    -classpath /home/samsilverberg/workspace_sdfs/sdfs/bin/:/home/samsilverberg/java_api/sdfs-bin/lib/* \
    org.opendedup.sdfs.mgmt.cli.SDFSCmdline $@
}

sdfs_cli $@
