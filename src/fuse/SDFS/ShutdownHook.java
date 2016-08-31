package fuse.SDFS;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.servers.SDFSService;

class ShutdownHook extends Thread {
	private SDFSService service;
	private String mountPoint;
	private boolean sr;

	public ShutdownHook(SDFSService service, String mountPoint) {
		this.service = service;
		this.mountPoint = mountPoint;
	}

	@Override
	public void run() {
		this.shutdown();
	}

	public void shutdown() {
		synchronized (service) {
			if (!sr) {
				SDFSLogger.getLog()
						.info("Please Wait while shutting down SDFS");
				SDFSLogger.getLog().info(
						"Data Can be lost if this is interrupted");
				service.stop();
				SDFSLogger.getLog().info("All Data Flushed");
				try {
					Process p = Runtime.getRuntime().exec(
							"umount " + mountPoint);
					p.waitFor();
				} catch (Exception e) {
				}
				SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
				sr=true;
				
			}
		}
	}
}

