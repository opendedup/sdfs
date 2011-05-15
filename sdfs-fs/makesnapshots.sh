for (( c=1; c<=15; c++ ))
do
	setfattr -n user.cmd.snapshot -v 5555:vmware/win7-$c /media/dedup/vmware/win7-0	
	echo "Snapshot $c created"
done
