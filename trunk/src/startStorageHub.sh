java \
   -Dfuse.logging.level=INFO -Xmx2g  -Xms2g \
  -server -XX:+UseCompressedOops -XX:+DisableExplicitGC -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:ParallelGCThreads=4 \
  -XX:InitialSurvivorRatio=3 -XX:TargetSurvivorRatio=90 -Djava.awt.headless=true \
  -classpath ./bin/:./lib/* \
   org.opendedup.sdfs.network.ClusteredHCServer "$@"
