package org.opendedup.sdfs.cluster;

import java.util.List;

import org.jgroups.Address;

interface ServerWeighting {
	public void init(List<DSEServer> servers);
	public Address getAddress(byte [] ignoredHosts);
	public List<Address> getAddresses(int sz,byte [] ignoredHosts);
	public List<DSEServer> getServers(int sz,byte [] ignoredHosts);
	
}