What is this?

  A clustered deduplicated file system.

Requirements

System Requirements

	1. x64 Linux Distribution. The application was tested and developed on ubuntu 13.1.0 and CentOS 6.5
	2. Modified Fuse 2.8+ .
		* Debian Packages for this are available at https://opendedup.googlecode.com/files/fuse_2.9.2-opendedup_amd64.tar.gz
		* RPM Package is available at https://opendedup.googlecode.com/files/sdfs_fuse_rpm.tar.gz
		* Source Patch is available at https://opendedup.googlecode.com/files/opendedup-thread-spawn-prevent
	3. At least 4 GB of RAM
	4. Java 7 - available at https://jdk7.dev.java.net/
	
Optional Packages

	* NFS Kernel Server
	* LIO - for ISCSI support

Installation

Ubuntu/Debian (Ubuntu 13.10)

	Step 1: Download and Install the modified fuse libraries

		wget https://opendedup.googlecode.com/files/fuse_2.9.2-opendedup_amd64.tar.gz
		tar -xvf fuse_2.9.2-opendedup_amd64.tar.gz
		cd fuse_2.9.2-opendedup_amd64/
		sudo dpkg -i fuse_2.9.2-opendedup_amd64.deb libfuse2_2.9.2-opendedup_amd64.deb
	
	Step 2: Install Java JRE

		sudo apt-get install openjdk-7-jre-headless

	Step 3: Install SDFS File System

		wget https://opendedup.googlecode.com/files/sdfs-2.0-beta1_amd64.deb
		sudo dpkg -i sdfs-2.0-beta1_amd64.deb
	Step 4: Change the maximum number of open files allowed

		echo "* hardnofile 65535" >> /etc/security/limits.conf
		echo "* soft nofile 65535" >> /etc/security/limits.conf
		exit
	Step 5: Log Out and Proceed to Initialization Instructions

CentOS/RedHat (Centos 6.5)

	Step 1: Log in as root

	Step 2: Download and Install the modified fuse libraries as root

		wget https://opendedup.googlecode.com/files/sdfs_fuse_rpm.tar.gz
		tar -xzf sdfs_fuse_rpm.tar.gz
		cd x86_64/
		rpm -Uv --force fuse-2.8.3-4.el6.x86_64.rpm fuse-libs-2.8.3-4.el6.x86_64.rpm

	Step 3: Install Java JRE

  		yum install java

	Step 4: Install the SDFS File System

		wget https://opendedup.googlecode.com/files/SDFS-2.0.0-1.x86_64.rpm
		rpm -iv SDFS-2.0.0-1.x86_64.rpm

	Step 5: Change the maximum number of open files allowed

		echo "* hardnofile 65535" >> /etc/security/limits.conf
		echo "* soft nofile 65535" >> /etc/security/limits.conf
		exit

	Step 6: Disable the IPTables firewall

		service iptables save
		service iptables stop
		chkconfig iptables off

	Step 7: Log Out and Proceed to Initialization Instructions

Initialization Instructions for Standalone Volumes

	Step 1: Log into the linux system as root or use sudo

	Step 2: Create the SDFS Volume. This will create a volume with 256 GB of capacity using a 4K block size.

		sudo mkfs.sdfs --volume-name=pool0 --volume-capacity=256GB

	Step 3: Create a mount point on the filesystem for the volume

		sudo mkdir /media/pool0

	Step 4: Mount the Volume

		sudo mount.sdfs pool0 /media/pool0/ &

Initialization Instructions for A Multi-Node Configuration

	In a mult-node cluster there are two components :

		* The Dedup Storage Engine (DSE) - This is the server/service that store all unique blocks. For redundancy, you should have at least two of these. They can live on the same 			  physical server as other components but this is not recommended for redundancy
		* The File System Service (FSS) - This is the server/service that mounts the sdfs volume. Multiple FSS services each serving its own volume can live in the same cluster.
	Prerequisits :

		* Static IP address and unique name for all nodes: All nodes should have a static IP address to reduce network issues.
		* Disable host firewalls : IPTables should be disabled for testing and then enabled again if needed with open ports for multicast and udp.
		* Network Communication : Multicast communication is the default configuration. All nodes in the cluster will need to be able to talk to eachother using multicast unless 			  the Jgroups configuration is modified.Multicast communication is usually not a problem on the same subnet but can be an issue between subnets or over wans
		* Fast Low Latency Network : SDFS will work within most networks but speed will suffer is network speeds low or network latency is high. 1Gb/s or above dedicated network is 			  recommeded. 
		* Update your /etc/sysctl.conf to improve network IO performance. Below are some recommeded options :
			vm.swappiness=1
			net.core.rmem_max=254800000
			net.core.wmem_max=254800000
			net.core.rmem_default=254800000
			net.core.wmem_default=254800000
			net.core.optmem_max=25480000
			net.ipv4.tcp_timestamps=0
			net.ipv4.tcp_sack=0
			net.core.netdev_max_backlog=250000
			net.ipv4.tcp_mem=25480000 25480000 25480000
			net.ipv4.tcp_rmem=4096 87380 25480000
			net.ipv4.tcp_wmem=4096 65536 25480000
			net.ipv4.tcp_low_latency=1
			Creating and Starting a DSE

The following steps will create 2 DSEs on server1 and server2

	Step 1: Create a DSE on Server1 using a 4K block size and 200GB of capacity and cluster node id of "1"

		mkdse --dse-name=sdfs --dse-capacity=200GB --cluster-node-id=1

	Step 2: Edit the /etc/sdfs/jgroups.cfg.xml and add the bind_addr attribute with Server1's IP address to the <UDP> tag.

		<UDP         
		mcast_port="${jgroups.udp.mcast_port:45588}"         
		tos="8"         
		ucast_recv_buf_size="5M"       
		 ucast_send_buf_size="640K"         
		mcast_recv_buf_size="5M"       
		 mcast_send_buf_size="640K"         
		loopback="true"         
		max_bundle_size="64K"         
		bind_addr="SERVER1 IP Address"
	
	Step 3: Start the DSE service on Server1

		startDSEService.sh -c /etc/sdfs/sdfs-dse-cfg.xml &

	Step 4: Create a DSE on Server1 using a 4K block size and 200GB of capacity and cluster node id of "1"

		mkdse --dse-name=sdfs --dse-capacity=200GB --cluster-node-id=1

	Step 5: Edit the /etc/sdfs/jgroups.cfg.xml and add the bind_addr attribute with Server1's IP address to the <UDP> tag.

		<UDP
		mcast_port="${jgroups.udp.mcast_port:45588}"
		tos="8"
		ucast_recv_buf_size="5M"
		ucast_send_buf_size="640K"
		mcast_recv_buf_size="5M"
		mcast_send_buf_size="640K"
		loopback="true"
		max_bundle_size="64K"
		bind_addr="SERVER1 IP Address"

	Step 6: Start the DSE service on Server1

		startDSEService.sh -c /etc/sdfs/sdfs-dse-cfg.xml &

Creating and Starting a SDFS FSS - Mounting a Volume

	The following steps will create 1 SDFS Volume on Server0. These are run as root.

	Step 1: Create the volume configuration. The following will create a volume using 4K blocks. This must match the block size used by the DSEs

		mkfs.sdfs --volume-name=pool0 --volume-capacity=400GB --chunk-store-local false

	Step 2: Create a mount point on the filesystem for the volume

		mkdir /media/pool0

	Step 3: Mount the SDFS FSS (Volume)

		mount.sdfs pool0 /media/pool0

Verify its all Working

	Step 1: Make sure you can see all your DSE Servers from the FSS Node. 

		On the node that you mounted the volume run sdfscli --cluster-dse-info. You should see all the nodes in the cluster. If you don't then there is probably a networking issue 			somewhere that is preventing them from talking.

	Step 2: Make sure you can see all of your other volumes in the cluster.

		On any node that has a mounted volume run sdfscli --cluster-volumes. You should see all of the volumes in the cluster.

	Step 3: Copy some data to the volume and make sure it all is there

		md5sum /etc/sdfs/pool0-volume-cfg.xml
		cp /etc/sdfs/pool0-volume-cfg.xml /media/pool0
		md5sum /media/pool0/pool0-volume-cfg.xml
Troubleshooting and other Notes

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