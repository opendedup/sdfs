#!/bin/bash
modprobe fuse > /dev/null
MEMORY="1000"
CFG=""
MPTG=4
MU="M"
EXEC="/usr/share/sdfs/jsvc"
PF="sdfs-ncfg.pid"
if [[ $1:0:1} == '-' ]]; then
while getopts ":v:" opt; do
  case $opt in
    c)
      CFG="/etc/sdfs/$OPTARG-dse-cfg.xml"
        PF="$OPTARG.pid"
        ;;
    z)
      MEM=$OPTARG
        ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done
else
	CFG="/etc/sdfs/$1-dse-cfg.xml"
fi

if [ ! -n "$MEM" ]; then
if [ -n "$CFG" ] && [ -f "$CFG" ]; then
        ac=$(echo 'cat //subsystem-config/chunkstore/@allocation-size' | xmllint --shell "$CFG" | grep -v ">" | cut -f 2 -d "=" | tr -d - | tr -d \");
        MEMORY=$(((ac/10737418240*MPTG)+1000))
fi
else
        MEMORY=$MEM
fi

$EXEC -server -outfile '&1' -errfile '&2' -Djava.net.preferIPv4Stack=true -home /usr/share/sdfs/bin/jre \
 -Xmx$MEMORY$MU -Xms$MEMORY$MU \
-XX:+DisableExplicitGC -pidfile /var/run/$PF -XX:+UseG1GC -Djava.awt.headless=true \
 -cp /usr/share/sdfs/lib/commons-daemon-1.0.15.jar:/usr/share/sdfs/lib/sdfs.jar:/usr/share/sdfs/lib/* org.opendedup.sdfs.network.ClusteredHCServer "$@"
