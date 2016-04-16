#
# Regular cron jobs for the sdfs package
#
0 4	* * *	root	[ -x /usr/bin/sdfs_maintenance ] && /usr/bin/sdfs_maintenance
