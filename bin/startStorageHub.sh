/usr/lib/jvm/jdk1.7.0/bin/java -Xmx4g -XX:ParallelGCThreads=20 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=8\
 -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=31 -XX:+AggressiveOpts -XX:+UseCompressedOops -Xms2g -Dfile.encoding=UTF-8\
  -classpath ./lib/quartz-1.7.3.jar:./lib/commons-collections-3.2.1.jar:./lib/log4j-1.2.15.jar:./lib/jdbm.jar:\
  ./lib/clhm-production.jar:./lib/bcprov-jdk16-143.jar:./lib/commons-codec-1.3.jar:./lib/commons-httpclient-3.1.jar:\
  ./lib/commons-logging-1.1.1.jar:./lib/java-xmlbuilder-1.jar:./lib/jets3t-0.7.1.jar:./lib/commons-cli-1.2.jar:\
  ./lib/sdfs.jar com.annesam.sdfs.network.NetworkHCServer /home/annesam/workspace/sdfs/src/etc/sdfs/server1-config.xml 2222