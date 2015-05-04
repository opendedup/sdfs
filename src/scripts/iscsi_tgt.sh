tgtadm --lld iscsi --op new --mode target --tid 1 -T iqn.2001-04.org.opendedup:dedupe
tgtadm --lld iscsi --op new --mode logicalunit --tid 1 --lun 1 -b /media/dedup/vmfslun0
tgtadm --lld iscsi --op new --mode logicalunit --tid 1 --lun 2 -b /media/dedup/vmfslun1
tgtadm --lld iscsi --op bind --mode target --tid 1 -I ALL

