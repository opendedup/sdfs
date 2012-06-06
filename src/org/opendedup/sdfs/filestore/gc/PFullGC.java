package org.opendedup.sdfs.filestore.gc;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class PFullGC implements GCControllerImpl {

	double prevPFull = 0;
	double nextPFull = 0;
	

	public PFullGC() {
		this.prevPFull = calcPFull();
		this.nextPFull = Math.ceil(this.prevPFull * 10)/10;
		
		SDFSLogger.getLog().info(
				"Current DSE Percentage Full is [" + this.prevPFull
						+ "] will run GC when [" + this.nextPFull + "]");
	}

	@Override
	public void runGC() {
		if (this.calcPFull() >= this.nextPFull) {
			SDFSEvent task = SDFSEvent.gcInfoEvent("Percentage Full Exceeded : Running Orphaned Block Collection");
			task.longMsg = "Running Garbage Collection because percentage full is " + this.calcPFull() + " and threshold is " +this.nextPFull;
			try {
			ManualGC.clearChunks(5);
			if (Main.firstRun) {
				Thread.sleep(5*60*1000);
				Main.firstRun = false;
				ManualGC.clearChunks(5);
			}
			this.prevPFull = calcPFull();
			this.nextPFull = this.calcNxtRun();
			SDFSLogger.getLog().info(
					"Current DSE Percentage Full is [" + this.prevPFull
							+ "] will run GC when [" + this.nextPFull + "]");
			task = SDFSEvent.gcInfoEvent("Garbage Collection Succeeded");
			task.shortMsg = "Garbage Collection Succeeded";
			task.longMsg = "Current DSE Percentage Full is [" + this.prevPFull
							+ "] will run GC when [" + this.nextPFull + "]";
			}catch(Exception e) {
				SDFSLogger.getLog().error("Garbage Collection failed",e);
				task = SDFSEvent.gcErrorEvent("Garbage Collection failed");
				task.longMsg = "Garbage Collection failed because " + e.getMessage();
			}
		}
	}

	private double calcPFull() {
		double pFull = 0;
		if (HashChunkService.getSize() > 0) {
			pFull = (double) HashChunkService.getSize()
					/ (double) HashChunkService.getMaxSize();
		}
		return pFull;
	}

	private double calcNxtRun() {
		double next = this.calcPFull() + .1;
		if (next >= .92)
			next = (double) .91;
		return next;
	}

}
