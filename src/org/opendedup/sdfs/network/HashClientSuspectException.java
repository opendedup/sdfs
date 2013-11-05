package org.opendedup.sdfs.network;

import org.opendedup.sdfs.servers.HCServer;

public class HashClientSuspectException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5398045346438784590L;
	
	public HashClientSuspectException(HCServer server) {
		super("DSEServer " + server.getHostName() + ":" + server.getPort() + " is suspect");
	}

}
