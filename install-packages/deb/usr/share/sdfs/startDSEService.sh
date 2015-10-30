java -Xmx4g -XX:+AggressiveOpts -XX:+UseConcMarkSweepGC \
   -XX:+UseCompressedOops -Dorg.apache.commons.logging.Log=fuse.logging.FuseLog -Dfuse.logging.level=INFO\
 -classpath /usr/share/sdfs/lib/sdfs.jar:/usr/share/sdfs/lib/*:/usr/share/sdfs/bin/* org.opendedup.sdfs.network.ClusteredHCServer "$@"
