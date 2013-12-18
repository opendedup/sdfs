package org.opendedup.buse.sdfsdev;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.servers.SDFSService;

class ShutdownHook extends Thread {
	private SDFSService service;
	private String mountPoint;
	private SDFSBlockDev dev;

	public ShutdownHook(SDFSService service, String mountPoint,SDFSBlockDev dev) {
		this.service = service;
		this.mountPoint = mountPoint;
		this.dev = dev;
	}

	@Override
	public void run() {

		SDFSLogger.getLog().info("Please Wait while shutting down SDFS");
		SDFSLogger.getLog().info("Data Can be lost if this is interrupted");
		try {
			Process p = Runtime.getRuntime().exec("umount " + dev.devicePath);
			p.waitFor();
		}catch(Exception e) {
			e.printStackTrace();
		}
		dev.close();
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
