package fuse.SDFS;

import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.util.SDFSLogger;

class ShutdownHook extends Thread {
	private SDFSService service;
	private String mountPoint;

	public ShutdownHook(SDFSService service, String mountPoint) {
		this.service = service;
		this.mountPoint = mountPoint;
	}

	@Override
	public void run() {

		SDFSLogger.getLog().info("Please Wait while shutting down SDFS");
		SDFSLogger.getLog().info("Data Can be lost if this is interrupted");
		service.stop();
		SDFSLogger.getLog().info("All Data Flushed");
		try {
			Process p = Runtime.getRuntime().exec("umount " + mountPoint);
			p.waitFor();
		} catch (Exception e) {
		}
		SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
	}
}
