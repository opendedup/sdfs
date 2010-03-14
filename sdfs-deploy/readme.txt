SDFS

What is this?

  A deduplicated file system based on fuse.

System Requirements
  
  * x64 Linux Distribution. The application was tested and developed on ubuntu 9.1
  * Fuse 2.8+ . Debian Packages for this are available at http://code.google.com/p/dedupfilesystem-sdfs/downloads/list
  * 2 GB of RAM
  * Java 7 - available at https://jdk7.dev.java.net/

Optional Packages

  * attr - (setfattr and getfattr) if you plan on doing snapshotting or setting extended file attributes

Getting Started

  Step 1: Set your JAVA_HOME to the path of the java 1.7 jdk or jre folder, or edit the path in the shell scripts 
	  (mount.sdfs and mkfs.sdfs) to reflect the java path 
		e.g. export JAVA_HOME=/usr/lib/jvm/jdk  

  Step 2: Create an sdfs file system.

	To create and SDFS file System you must run the following command:
		sudo ./mkfs.sdfs --volume-name=<volume-name> --volume-capacity=<capacity>
	      e.g.
		sudo ./mkfs.sdfs --volume-name=sdfs_vol1 --volume-capacity=100GB

  Step 3: Mount the sdfs

	To mount SDFS run the following command:
		sudo ./mount.sdfs -v <volume-name> -m <mount-point>
	      e.g.
		sudo ./mount.sdfs -v sdfs_vol1 -m /media/sdfs

Known Limitation(s)

	Testing has been limited thus far. Please test and report bugs	
	Deleting of data and reclaiming of space has not been implemented yet. ETA: This will be available in the coming days.
	Graceful exit if physical disk capacity is reached. ETA : Will be implemented shortly 
	Maximum filesize it currently 250GB

Tips and Tricks

	There are plenty of options to create a file system and to see them run "./mkfs.sdfs --help".

	Chunk Size:

	The most notable option is --io-chunk-size. The option --io-chunk-size sets the size of chunks that are hashed and can only
	be changed before the file system is mounted to for the first time. The default setting is 128k but can be set as low as 4k. 
	The size of chucks determine the efficient at which files will be deduplicated at the cost of RAM. As an example a 4k chunk 
	size is perfect for Virtual Machines (VMDKs) because it matches the cluster size of most guest os file systems but can cost
	as much as 8GB of RAM per 1TB to store. In contrast setting the chunk size to 128k is perfect of archived data, such as backups,
	and will allow you to store as much as 32TB of data with the same 8GB of memory.

	To create a volume that will store VMs (VMDK files) create a volume using 4k chunk size as follows:
		sudo ./mkfs.sdfs --volume-name=sdfs_vol1 --volume-capacity=150GB --io-chunk-size=4k --io-max-file-write-buffers=150


	File Placement:

	Deduplication is IO Intensive. SDFS, by default writes data to /opt/sdfs. SDFS does a lot of writes went persisting data and a 
	lot of random io when reading data. For high IO intensive applications it is suggested that you split at least the chunk-store-data-location 
	and chunk-store-hashdb-location onto fast and separate physical disks. From experience these are the most io intensive stores and
	could take advantage of faster IO.

	Inline vs Batch based deduplication:

	SDFS provides the option to do either inline or batch based deduplication. Inline deduplication stores the data to the deduplicated
	chunkstore in realtime. Batch based deduplication stores data to disk as a normal file unless a match is found in the chunk store to
	a previously persisted deduplicated chunk of data. Inline deduplication is perfect for backup but not as good for live data such as 
	VMs or databases because there is a high rate of change for those types of files. Batch based deduplication has a lot of overhead since
	there is eventually twice as much IO per file, one to store it to disk and once to dedup it. By default, inline is enabled, but can be 
	disabled with the option "--io-dedup-files=false" when creating the volume.
	
	When this option is set to false all data will be stored in its origional format until deduplication is set on a per file basis. To set
	deduplication to on, run the command "setfattr -n user.cmd.dedupAll -v 556:true <path to file>". Conversely, setting 
	"setfattr -n user.cmd.dedupAll -v 556:false <path to file>" will disable deduplication of a specific file from that point forward.
	
	Finally, batch based deduplicated files can be checked to see if duplicate chunks exist with the command 
	"setfattr -n user.cmd.optimize -v 555:100 <path to file>"

	Snapshots:
	
	SDFS provides snapshot functions for files and folders. To snapshot a file or folder you will need the setfattr command available on your system.
	On ubuntu this is available through the Attr package. The snapshot command is "setfattr -n user.cmd.snapshot -v 5555:<destination path> <snapshot source>".
	The destination path is relative to the mount point of the sdfs filesystem.

	Other options and extended attributes:

	SDFS uses extended attributes to manipulate the SDFS file system and files contained within. It is also used to report on IO performance. To get a list
	of commands and readable IO statistics run "getfattr -d *" within the mount point of the sdfs file system.
		e.g.
			user@server:/media/dedup$ getfattr -d * .
 		
	NFS Shares:

	SDFS can be shared through NFS exports on linux kernel 2.6.31 and above. It can be shared on kernel levels below that but performance will 
	suffer at you will need to disable the fuse direct_io option when mounting the sdfs filesystem. NFS opens and closes files with every read
	or write. File open and closes are expensive for SDFS and as such can degrade performance when running over NFS. SDFS volumes can be optimized
	for NFS with the option "--io-safe-close=false" when creating the volume. This will leave files open for NFS reads and writes. Files will be 
	closed after an inactivity period has been reached. By default this inactivity period is 15 (900) seconds minutes but can be changed at any time, 
	along with the io-safe-close option within the xml configuration file located in /etc/sdfs/<volume-name>-volume-cfg.xml.

	More on Virtual Machines:

	It was the origional goal of SDFS to be a file system of virutal machines. Again, to get proper deduplication rates for VMDK files set io-chunk-size
	to "4" when creating the volume. This will match the chunk size of the guest os file system usually. NTFS allow 32k chunk sizes but not on root volumes.
	It may be advantageous, for windows environments, to have the root volume on one mounted SDFS path at 4k chunk size and data volumes in another SDFS path 
	at 32k chunk sizes. Then format the data ntfs volumes, within the guest, for 32k chunk sizes. This will provide optimal performance.

	SDFS provides a convenience function to create flat VMDK files quickly and in a fashion that will be easy to snapshot (see snapshots above). To use this 
	convenience function run "setfattr -n user.cmd.vmdk.make -v 5556:<name of vmdk>:<size of vmdk> <path where vmdk will be placed>".
		e.g.
			setfattr -n user.cmd.vmdk.make -v 5556:win2k8-1:100GB dedup/vmfs/

	Memory:
	
	The mount.sdfs shell script currently allocates up to 2GB of RAM for the SDFS file system. This is fine for SDFS file systems of around 200GB for 4k chunk size
	and around 6TB for 128k chunk size. To expand the memory edit the "-Xmx2g" within mount.sdfs script to something better for your environment.
			

3/12/10
