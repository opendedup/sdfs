package fuse.SDFS;

import org.opendedup.sdfs.servers.SDFSService;

class ShutdownHook extends Thread {
	private SDFSService service;
	private String mountPoint;

	public ShutdownHook(SDFSService service, String mountPoint) {
		this.service = service;
		this.mountPoint = mountPoint;
	}

	public void run() {

		System.out.println("Please Wait while shutting down SDFS");
		System.out.println("Data Can be lost if this is interrupted");
		service.stop();
		System.out.println("All Data Flushed");
		System.out.println("SDFS Shut Down Cleanly");
		try {
			Process p = Runtime.getRuntime().exec("umount " + mountPoint);
			p.waitFor();
		} catch (Exception e) {
		}

	}
}
