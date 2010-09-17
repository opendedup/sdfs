What is this?

  A deduplicated file system.

System Requirements
  
  * Windows (7,XP,Vista,2003,2008) Distribution. The application was tested and developed on Windows 7 (32bit)
  * Dokan 5.3+.  Dokan can be downloaded from http://dokan-dev.net/en/
  * 2 GB of Available RAM
  * Java 7 (32 bit) - included in package.


Getting Started

  Step 1: Install the Dokan library and driver. It can be downloaded from:
  	http://dokan-dev.net/wp-content/uploads/dokaninstall_053.exe
  
  Step 1: Create an sdfs file system.
	To create and SDFS file System you must run the mksdfs command from within the SDFS binary directory. Make sure you have "Full Control
	permissions to the "c:\program files\sdfs" directory.
	
	Example to create a SDFS volume:
	
		mksdfs --volume-name=<volume-name> --volume-capacity=<capacity>
	      e.g.
		mksdfs --volume-name=sdfs_vol1 --volume-capacity=100GB

  Step 2: Mount the sdfs

	To mount SDFS run the following command:
		mountsdfs -v <volume-name> -m <mount-point>
	      e.g.
		mountsdfs -v sdfs_vol1 -m S

Known Limitation(s)

	Testing has been limited thus far. Please test and report bugs	
	Graceful exit if physical disk capacity is reached. ETA : Will be implemented shortly 
	Maximum individual filesize within sdfs currently 250GB at 4k chunks and multiples of that at higher chunk sizes.
	Only one SDFS volume can be mounted at a time.

Performance :

	By default SDFS on Windows is single threaded and this will be changed in the future once more testing is done. Based on testing, a single
	threaded SDFS volume will write at about 15 MB/s.  
	To enable faster throughput multithreading can be enabled either when the volume is created on within the XML file. To enable 
	multithreading at volume creation add  "--io-safe-close=false" as a command line option to mksdfs. Otherwise edit the xml configuration
	file (<volume-name>-volume-cfg.xml) within c:\program files\sdfs\etc and set "io-saf-close=false" .
	
	Example to create a SDFS volume with multi-threaded support:
	
		mksdfs --volume-name=<volume-name> --volume-capacity=<capacity> --io-safe-close=false
		
	Safe close prevents the file from closing when commanded by the filesystem, but is instead closed based on inactivity. Setting this to
	"false" could cause corruption if the file is still open while the system crashes.

Data Removal
	
	SDFS uses a batch process to remove unused blocks of hashed data.This process is used because the file-system is 
	decoupled from the back end storage (ChunkStore) where the actual data is held. As hashed data becomes stale they 
	are removed from the chunk store. The process for determining and removing stale chunks is as follows.
		
		1. SDFS file-system informs the ChunkStore what chunks are currently in use. This happens when
		chunks are first created and then every 6 hours on the hour after that.
		2. The Chunk Store checks for data that has not been claimed, in the last two days, by the SDFS file system 
		every 24 hours.
		3. The chunks that have not been claimed in the last 25 hours are put into a pool and overwritten as new data
		is written to the ChunkStore.

	All of this is configurable and can be changed after a volume is written to. Take a look at cron format for more details.

		

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
		mksdfs --volume-name=sdfs_vol1 --volume-capacity=150GB --io-chunk-size=4k --io-max-file-write-buffers=150


	File Placement:

	Deduplication is IO Intensive. SDFS, by default writes data to c:\program files\sdfs. MAKE SURE YOU HAVE READ AND WRITE PERMISSIONS 
	TO THIS DIRECTORY SDFS does a lot of writes went persisting data and a lot of random io when reading data. For high IO intensive 
	applications it is suggested that you split at least the chunk-store-data-location and chunk-store-hashdb-location onto fast and 
	separate physical disks. From experience these are the most io intensive stores and could take advantage of faster IO.

	Inline vs Batch based deduplication:

	SDFS provides the option to do either inline or batch based deduplication. Inline deduplication stores the data to the deduplicated
	chunkstore in realtime. Batch based deduplication stores data to disk as a normal file unless a match is found in the chunk store to
	a previously persisted deduplicated chunk of data. Inline deduplication is perfect for backup but not as good for live data such as 
	VMs or databases because there is a high rate of change for those types of files. Batch based deduplication has a lot of overhead since
	there is eventually twice as much IO per file, one to store it to disk and once to dedup it. By default, inline is enabled, but can be 
	disabled with the option "--io-dedup-files=false" when creating the volume.
	
	When this option is set to false all data will be stored in its origional format until deduplication is set on a per file basis. To set
	deduplication to on, run the command "sdfscli --dedup-file=true --file-path=<path to file>". Conversely, setting 
	"sdfscli --dedup-file=true --file-path=<path to file>" will disable deduplication of a specific file from that point forward.
	
	

	Snapshots:
	
	SDFS provides snapshot functions for files and folders. The snapshot command is "sdfscli --snapshot --file-path=<path to file without 
	drive letter> --snapshot-path=<destination>". The destination path is relative to the mount point of the sdfs filesystem.

	Other options and extended attributes:

	SDFS keeps deduplication rates and IO per file. To get IO statistics run "sdfscli --file-info --file-path=<path to file without drive 
	letter>" within the mount point of the sdfs file system.

	Memory:
	
	 The mountsdfs shell script currently allocates up to 2GB of RAM for the SDFS file system. This is fine for SDFS file systems of around
	 200GB for 4k chunk size and around 6TB for 128k chunk size. To expand the memory edit the "-Xmx2g" within mount.sdfs script to something
	 better for your environment. Each stored chunk takes up approximately 33 bytes of RAM. To calculate how much RAM you will need for a 
	 specific volume divide the volume size (in bytes) by the chunk size (in bytes) and multiply that times 33.

    		Memory Requirements Calculation: (volume size/chunk size)*33
    
    Cloud Storage and Amazon S3 Web Serivce
    
    It is now possible to store dedup chunks to the Amazon S3 cloud storage service. This will allow you to store unlimited amounts of data
    without the need for local storage. AES 256 bit encryption and compression (default) is provided for storing data to AWS. It is suggested
    that the chunk size be set to the default (128k) to allow for maximum compression and fewest round trips for data. 
    To Setup AWS enable dedup volume follow these steps:
    1. Go to http://aws.amazon.com and create an account.
    2. Sign up for S3 data storage
    3. Get your Access Key ID and Secret Key ID.
    4. Make an SDFS volume using the following parameters:
    	mksdfs  --volume-name=<volume name> --volume-capacity=<volume capacity> --aws-enabled=true --aws-access-key=<the aws assigned access key> --aws-bucket-name=<a universally unique bucket name such as the aws-access-key> --aws-secret-key=<assigned aws secret key> --chunk-store-encrypt=true
    5. Mount volume and go to town!
    	mountsdfs -v <volume name> -m <mount point>
    				

9/12/10