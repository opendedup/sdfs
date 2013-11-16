package org.opendedup.sdfs.replication;

import org.opendedup.logging.SDFSLogger;

class ShutdownHook extends Thread {
	ReplicationScheduler sched;
	String name;

	public ShutdownHook(ReplicationScheduler sched, String name) {
		this.sched = sched;
		this.name = name;
	}

	@Override
	public void run() {
		SDFSLogger.getLog().info(
				"Please Wait while shutting down SDFS Relication Service for "
						+ name);
		sched.stopSchedules();
		SDFSLogger.getLog().info(
				"SDFS Relication Service Shut Down Cleanly for " + name);
	}
}