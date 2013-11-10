package org.opendedup.sdfs.cluster;

import java.util.List;

interface ServerWeighting {
	public void init(List<DSEServerWeighting> weights);
	public DSEServer getServer(byte [] ignoredHosts);
	public List<DSEServer> getServers(int sz,byte [] ignoredHosts);
	
	
	
}