package org.opendedup.sdfs.mgmt;



import org.opendedup.buse.sdfsdev.VolumeShutdownHook;
import org.opendedup.logging.SDFSLogger;

public class Shutdown {

	public void getResult() throws Exception {
		SDFSLogger.getLog().info("shutting down volume");
		System.out.println("shutting down volume");
		VolumeShutdownHook.shutdown();
		System.exit(0);
		
	}

}
