What is this?

  A deduplicated file system.

System Requirements
  
  * Windows (7,XP,Vista,2003,2008) Distribution. The application was tested and developed on Windows 7 (64bit)
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

For more information take a look at the opendedup website at www.opendedup.org