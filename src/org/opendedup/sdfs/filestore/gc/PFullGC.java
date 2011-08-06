package org.opendedup.sdfs.filestore.gc;

import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class PFullGC implements GCControllerImpl {

	double prevPFull = 0;
	double nextPFull = 0;
	boolean firstRun = true;

	public PFullGC() {
		this.prevPFull = calcPFull();
		this.nextPFull = this.calcNxtRun();
		SDFSLogger.getLog().info(
				"Current DSE Percentage Full is [" + this.prevPFull
						+ "] will run GC when [" + this.nextPFull + "]");
	}

	@Override
	public void runGC() {
		if (this.calcPFull() >= this.nextPFull) {
			ManualGC.clearChunks(2);
			if (firstRun) {
				this.firstRun = false;
				ManualGC.clearChunks(2);
			}
			this.prevPFull = calcPFull();
			this.nextPFull = this.calcNxtRun();
			SDFSLogger.getLog().info(
					"Current DSE Percentage Full is [" + this.prevPFull
							+ "] will run GC when [" + this.nextPFull + "]");
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
