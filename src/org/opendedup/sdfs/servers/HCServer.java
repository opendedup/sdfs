package org.opendedup.sdfs.servers;

public class HCServer {
	String hostName;
	int port;
	boolean useUDP;
	boolean compress;
	boolean useSSL;

	public HCServer(String hostName, int port, boolean useUDP, boolean compress,boolean useSSL) {
		this.hostName = hostName;
		this.port = port;
		this.useUDP = useUDP;
		this.compress = compress;
		this.useSSL = useSSL;
	}

	public boolean isCompress() {
		return compress;
	}

	public boolean isUseUDP() {
		return useUDP;
	}

	public String getHostName() {
		return hostName;
	}

	public int getPort() {
		return port;
	}
	
	public boolean isSSL() {
		return this.useSSL;
	}

}
