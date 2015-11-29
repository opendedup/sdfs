What is this?

  A deduplicated file system.

System Requirements
  
  * Windows x64 (7,10,2008,2012) Distribution. The application was tested and developed on Windows 10 (64bit)
  * Dokany 8.0+ (Included in package) 
  * 2 GB of Available RAM
  * Java 8 (64 bit) - included in package.


Getting Started

  Step 1: Install the . It can be downloaded from:
  	http://www.opendedup.org/downloads/sdfs-latest-setup.exe
  
  Step 2: Create an sdfs file system.
	To create and SDFS file System you must run the mksdfs command from within the SDFS binary directory. Make sure you have "Full Control
	permissions to the "c:\program files\sdfs" directory.
	
	Example to create a SDFS volume:
	
		mksdfs --volume-name=<volume-name> --volume-capacity=<capacity>
	      e.g.
		mksdfs --volume-name=sdfs_vol1 --volume-capacity=100GB

  Step 3: Mount the sdfs

	To mount SDFS run the following command:
		mountsdfs -v <volume-name> -m <mount-point>
	      e.g.
		mountsdfs -v sdfs_vol1 -m S

  Step 4: Unmount volume
	To unmount the volume run sdfscli --shutdown

  Step 5: Mount sdfs as a service
	To have sdfs run on boot you will need to use the sc tool to add each volume you want to mount as a service. The service will not show
	that it is running because mountsdfs is not yet built as a service, but the volume will be mounted. Below is how you add an sdfs volume as a
	service.
		sc create sdfs<drive-letter> binPath="\"C:\Program Files\sdfs\mountsdfs.exe\" -v sdfs -m <drive-letter> -cp" DisplayName="SDFS <drive-letter> Drive" start=auto
	e.g.
		sc create sdfss binPath="\"C:\Program Files\sdfs\mountsdfs.exe\" -v sdfs_vol1 -m s -cp" DisplayName="SDFS s Drive" start=auto

Using Cloud storage with SDFS

SDFS can use cloud object storage to store data. To enable this make the volume with the following commands:

	**AWS Storage**
	mksdfs  --volume-name=pool0 --volume-capacity=1TB --aws-enabled true --cloud-access-key <access-key> --cloud-secret-key <secret-key> --cloud-bucket-name <unique bucket name>
		
	**Azure Storage**
	mksdfs  --volume-name=pool0 --volume-capacity=1TB --azure-enabled true --cloud-access-key <access-key> --cloud-secret-key <secret-key> --cloud-bucket-name <unique bucket name>
		
	**Google Storage**
	mksdfs  --volume-name=pool0 --volume-capacity=1TB --google-enabled true --cloud-access-key <access-key> --cloud-secret-key <secret-key> --cloud-bucket-name <unique bucket name>
		

For more information take a look at the opendedup website at www.opendedup.org