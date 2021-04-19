# sdfs
What is this?

  A deduplicated file system that can store data in object storage or block storage.

## License

GPLv2

## Requirements

### System Requirements

	1. x64 Linux Distribution. The application was tested and developed on ubuntu 18.04
	2. At least 8 GB of RAM
	3. Minimum of 2 cores
	4. Minimum of 16GB or Storage
	
### Optional Packages

	* Docker

## Installation

### Ubuntu/Debian (Ubuntu 14.04+)

	Step 1: Download the latest sdfs version
		wget http://opendedup.org/downloads/sdfs-latest.deb

	Step 2: Install sdfs and dependencies
		sudo apt-get install fuse libfuse2 ssh openssh-server jsvc libxml2-utils
		sudo dpkg -i sdfs-latest.deb
		 
	Step 3: Change the maximum number of open files allowed
		echo "* hard nofile 65535" >> /etc/security/limits.conf
		echo "* soft nofile 65535" >> /etc/security/limits.conf
		exit
	Step 5: Log Out and Proceed to Initialization Instructions

### CentOS/RedHat (Centos 7.0+)

	Step 1: Download the latest sdfs version
		wget http://opendedup.org/downloads/sdfs-latest.rpm

	Step 2: Install sdfs and dependencies
		yum install jsvc libxml2 java-1.8.0-openjdk
		rpm -iv --force sdfs-latest.rpm
		 
	Step 3: Change the maximum number of open files allowed
		echo "* hardnofile 65535" >> /etc/security/limits.conf
		echo "* soft nofile 65535" >> /etc/security/limits.conf
		exit
	Step 5: Log Out and Proceed to Initialization Instructions

	Step 6: Disable the IPTables firewall

		service iptables save
		service iptables stop
		chkconfig iptables off

	Step 7: Log Out and Proceed to Initialization Instructions

## Docker Usage

### Setup
	Step 1:

		docker pull gcr.io/hybrics/hybrics:3.12
	
	Step 2:

		docker run --name=sdfs1 -p 0.0.0.0:6442:6442 -d gcr.io/hybrics/hybrics:3.12
	
	Step 3:

		wget https://storage.cloud.google.com/hybricsbinaries/hybrics-fs/mount.sdfs-master
		sudo mv mount.sdfs-master /usr/sbin/mount.sdfs
		sudo chmod 777 /usr/sbin/mount.sdfs
		sudo mkdir /media/sdfs
	
	Step 4:
		sudo ./mount.sdfs -d sdfs://localhost:6442 /mnt


### Docker Parameters:

| Envronmental Variable | Description | Default |
|-----------------------|-------------|---------|
|CAPACITY				| The Maximum Phyiscal Capacity of the volume. This is Specified in GB or TB| 100GB|
|TYPE					| The type of backend storage. This can be specified as AZURE, GOOGLE, AWS, BACKBLAZE. If none is specified local storage is used.| local storage|
|URL					| The url of for the oject storage used|None|
|BACKUP_VOLUME			| If set to true, the sdfs volume will be setup for deduping archive data better and faster. If not set it will default to better read/write access for random IO| false|
|GCS_CREDS_FILE			| The location of a GCS creds file for authicating to Google cloud storage and GCP Pubsub. Will be required for Google Cloud Storage and GCP Pubsub access.|None|
|ACCESS_KEY				| S3 or Azure Access Key|None|
|SECRET_KEY				| The S3 or Azure Secret Key used to Access object storage|None|
|AWS_AIM				| If set to true AWS AIM will be used for access| false|
|PUBSUB_PROJECT			| The project where the pubsub notification should be setup for file changes and replication|None|
|PUBSUB_CREDS_FILE		| The credentials file used for pubsub creation and access with GCP. If not set GCS_CREDS_FILE will be used.|None|
|DISABLE_TLS			| Disable TLS for api access is set to true| false|
|REQUIRE_AUTH			| Whether to require authication for access to the sdfs APIs|false|
|PASSWORD				| The password to use when creating the volume|admin|
|EXTENDED_CMD			| Any addition command parameters to run during creation|None|

### Docker run examples

Optimize usage running using local storage:

```bash
sudo mkdir /opt/sdfs1
sudo docker run --name=sdfs1 --env CAPACITY=1TB --volume /home/A_USER/sdfs1:/opt/sdfs -p 0.0.0.0:6442:6442 -d gcr.io/hybrics/hybrics:3.12
```

Optimize usage running using Google Cloud Storage:

```bash
sudo mkdir /opt/sdfs1
sudo docker run --name=sdfs1 --env BUCKET_NAME=ABUCKETNAME --env TYPE=GOOGLE --env=GCS_CREDS_FILE=/keys/service_account_key.json --env=PUBSUB_PROJECT=A_GCP_PROJECT --env CAPACITY=1TB --volume=/home/A_USER/keys:/keys --volume /home/A_USER/sdfs1:/opt/sdfs -p 0.0.0.0:6442:6442 -d gcr.io/hybrics/hybrics:3.12
```

## Build Instructions

	Linux Version Must be build from a Linux System and Windows must be build from a Windows System
	
	Linux build Requirements:
		1. Docker
		2. git 

	Docker Build Steps	
	```bash
	git clone https://github.com/opendedup/sdfs.git
	cd sdfs
	git fetch
	git checkout -b 3.12 origin/3.12
	#Build image with packages
	docker build -t sdfs-package:latest --target build -f Dockerbuild.localbuild .
	mkdir pkgs
	#Extract Package
	docker run --rm sdfs-package:latest | tar --extract --verbose -C pkgs/
	#Build docker sdfs container
	docker build -t sdfs:latest -f Dockerbuild.localbuild .
	```





## Initialization Instructions for Standalone Volumes


	Step 1: Log into the linux system as root or use sudo

	Step 2: Create the SDFS Volume. This will create a volume with 256 GB of capacity using a Variable block size.
		**Local Storage**
		sudo mkfs.sdfs --volume-name=pool0 --volume-capacity=256GB

		**AWS Storage**
		sudo mkfs.sdfs --volume-name=pool0 --volume-capacity=1TB --aws-enabled true --cloud-access-key <access-key> --cloud-secret-key <secret-key> --cloud-bucket-name <unique bucket name>
		
		**Azure Storage**
		sudo mkfs.sdfs --volume-name=pool0 --volume-capacity=1TB --azure-enabled true --cloud-access-key <access-key> --cloud-secret-key <secret-key> --cloud-bucket-name <unique bucket name>
		
		**Google Storage**
		sudo mkfs.sdfs --volume-name=pool0 --volume-capacity=1TB --google-enabled true --cloud-access-key <access-key> --cloud-secret-key <secret-key> --cloud-bucket-name <unique bucket name>
		
		
	Step 3: Create a mount point on the filesystem for the volume

		sudo mkdir /media/pool0

	Step 4: Mount the Volume

		sudo mount -t sdfs pool0 /media/pool0/

	Set 5: Add the filesystem to fstab
		pool0           /media/pool0    sdfs    defaults                0       0
		


## Troubleshooting and other Notes

	Running on a Multi-Node clusting on KVM guest.

	By default KVM networking does not seem to allow guest to communicate over multicast. It also doesn't seem to work when bridging from a nic. From my reseach it looks like you have 		to setup a routed network from a KVM host and have all the guests on that shared network. In addition, you will want to enable multicast on the virtual nic that is shared by those 		guest. Here is is the udev code to make this happen. A reference to this issue is found here.

	# cat /etc/udev/rules.d/61-virbr-querier.rules 
	ACTION=="add", SUBSYSTEM=="net", RUN+="/etc/sysconfig/network-scripts/vnet_querier_enable"

	# cat /etc/sysconfig/network-scripts/vnet_querier_enable 
	#!/bin/sh
	if [[ $INTERFACE == virbr* ]]; then
	    /bin/echo 1 > /sys/devices/virtual/net/$INTERFACE/bridge/multicast_querier
	fi

	Testing multicast support on nodes in the cluster

	The jgroups protocol includes a nice tool to verify multicast is working on all nodes. Its an echo tool and sends messages from a sender to a reciever

	On the receiver run

		java -cp /usr/share/sdfs/lib/jgroups-3.4.1.Final.jar org.jgroups.tests.McastReceiverTest -mcast_addr 231.12.21.132 -port 45566

	On the sender run

		java -cp /usr/share/sdfs/lib/jgroups-3.4.1.Final.jar org.jgroups.tests.McastSenderTest -mcast_addr 231.12.21.132 -port 45566

	Once you have both sides running type a message on the sender and you should see it on the receiver after you press enter. You may also want to switch rolls to make sure multicast 		works both directions.

	take a look at http://docs.jboss.org/jbossas/docs/Clustering_Guide/4/html/ch07s07s11.html for more detail.

	Further reading:

		Take a look at the administration guide for more detail. http://www.opendedup.org/sdfs-20-administration-guide

	Ask for Help

		If you still need help check out the message board here https://groups.google.com/forum/#!forum/dedupfilesystem-sdfs-user-discuss
