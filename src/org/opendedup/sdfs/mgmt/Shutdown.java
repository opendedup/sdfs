package org.opendedup.sdfs.mgmt;


import org.opendedup.logging.SDFSLogger;

public class Shutdown {

	public void getResult() throws Exception {
		SDFSLogger.getLog().info("shutting down volume");
		System.out.println("shutting down volume");
		System.exit(0);
		
	}

}
