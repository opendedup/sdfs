java \
  -Xmx2g -Xms2g -XX:ParallelGCThreads=20 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=8 -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=31 -XX:+AggressiveOpts  \
  -XX:+UseCompressedOops \
 -classpath /home/samsilverberg/workspace_sdfs/sdfs/bin/:/home/samsilverberg/java_api/sdfs-bin/lib/* \
   org.opendedup.sdfs.network.ClusteredHCServer "$@"
