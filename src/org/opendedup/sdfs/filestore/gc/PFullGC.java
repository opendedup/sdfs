package org.opendedup.sdfs.filestore.gc;

import java.text.DecimalFormat;
import java.util.concurrent.locks.Lock;

import org.opendedup.logging.SDFSLogger;


import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class PFullGC implements GCControllerImpl {

	double prevPFull = 0;
	double nextPFull = .05;

	public PFullGC() {
		this.prevPFull = calcPFull();
		this.nextPFull = Math.ceil(this.prevPFull * 10) / 10;
		double pFull = (this.prevPFull * 100);
		double nFull = (this.nextPFull * 100);
		DecimalFormat twoDForm = new DecimalFormat("#.##");
		pFull = Double.valueOf(twoDForm.format(pFull));
		nFull = Double.valueOf(twoDForm.format(nFull));
		SDFSLogger.getLog().info(
				"Current DSE Percentage Full is [" + pFull
						+ "] will run GC when [" + nFull + "]");
	}

	@Override
	public void runGC() {
		if (this.calcPFull() >= this.nextPFull) {
			SDFSEvent task = SDFSEvent
					.gcInfoEvent("Percentage Full Exceeded : Running Orphaned Block Collection");
			task.longMsg = "Running Garbage Collection because percentage full is "
					+ this.calcPFull() + " and threshold is " + this.nextPFull;
			try {
				ManualGC.clearChunks(1);
				this.prevPFull = calcPFull();
				this.nextPFull = this.calcNxtRun();
				double pFull = (this.prevPFull * 100);
				double nFull = (this.nextPFull * 100);
				DecimalFormat twoDForm = new DecimalFormat("#.##");
				pFull = Double.valueOf(twoDForm.format(pFull));
				nFull = Double.valueOf(twoDForm.format(nFull));
				SDFSLogger.getLog().info(
						"Current DSE Percentage Full is [" + pFull
								+ "] will run GC when [" + nFull + "]");
				task.endEvent("Garbage Collection Succeeded");
				task.shortMsg = "Garbage Collection Succeeded";
				task.longMsg = "Current DSE Percentage Full is [" + pFull
						+ "] will run GC when [" + nFull + "]";
			} catch (Exception e) {
				SDFSLogger.getLog().error("Garbage Collection failed", e);
				task.endEvent(
						"Garbage Collection failed because " + e.getMessage(),
						SDFSEvent.ERROR);
			}
		}
		
	}

	private double calcPFull() {
		Lock l = null;
		try {
		if(!Main.chunkStoreLocal) {
				l = HCServiceProxy.cs.getLock("fdisk");
		}
		double pFull = 0;
		if (HCServiceProxy.getSize() > 0) {
			pFull = (double) HCServiceProxy.getSize()
					/ (double) HCServiceProxy.getMaxSize();
		}
		return pFull;
		}finally {
			if(l !=null)
				l.unlock();
		}
	}
	
	

	private double calcNxtRun() {
		double next = this.calcPFull();
		if (next >= .92)
			return .90;
		else {
			next = Math.ceil(next * 10.0) / 10;
		}
		if (next == 0)
			next = .1;
		return next;
	}

	@Override
	public void reCalc() {
		this.prevPFull = calcPFull();
		this.nextPFull = this.calcNxtRun();
		SDFSLogger.getLog()
		.debug("Current DSE Percentage Full is ["
				+ this.prevPFull + "] will run GC when ["
				+ this.nextPFull + "]");
	}
	
	public static void main(String [] args) {
		double num = 0.800338958916741818D;
		
		System.out.println(Math.ceil(num * 10.0) / 10);
	}

}
