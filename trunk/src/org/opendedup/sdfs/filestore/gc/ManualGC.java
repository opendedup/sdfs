package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;

import java.util.concurrent.locks.Lock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDisk;
import org.opendedup.mtools.ThreadedFDisk;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class ManualGC {

	public static SDFSEvent evt = null;

	public static long clearChunks(int minutes) throws InterruptedException,
			IOException {
		return clearChunksMills((long) minutes * 60 * 1000);
	}

	public static synchronized long clearChunksMills(long milliseconds)
			throws InterruptedException, IOException {
		Lock l = null;
		if (Main.chunkStoreLocal) {
			l = GCMain.gclock.writeLock();

		} else {
			l = HCServiceProxy.cs.getLock("fdisk");
		}
		l.lock();
		try {

			long rm = 0;
			if (Main.chunkStoreLocal)
				evt = SDFSEvent
						.gcInfoEvent("SDFS Volume Cleanup Initiated for "
								+ Main.volume.getName());
			else
				evt = SDFSEvent
						.gcInfoEvent("SDFS Volume Cleanup Initiated for "
								+ Main.DSEClusterID);
			evt.maxCt = 100;
			evt.curCt = 0;
			try {
				runGC(milliseconds);
			} catch (IOException e) {
				if (!Main.firstRun)
					throw e;
			}
			if (Main.firstRun) {
				SDFSLogger.getLog().info("Waiting 10 Seconds to run again");
				SDFSEvent wevt = SDFSEvent.waitEvent(
						"Waiting 10 Seconds to run again", evt);
				wevt.maxCt = 10;
				for (int i = 0; i < 10; i++) {
					Thread.sleep(1000);
					wevt.curCt++;
				}
				wevt.endEvent("Done Waiting");
				rm = rm + runGC(10 * 1000);
				Main.firstRun = false;
			}
			evt.endEvent("SDFS Volume Cleanup Finished for "
					+ Main.volume.getName());
			return rm;
		} finally {
			l.unlock();
		}
	}

	private static long runGC(long milliseconds) throws IOException {
		long rm = 0;
		try {

			long tm = System.currentTimeMillis();
			if (Main.chunkStoreLocal) {
				new FDisk(evt);
			} else {
				HCServiceProxy.runFDisk(evt);
			}
			evt.curCt = 33;
			HCServiceProxy.processHashClaims(evt);
			evt.curCt = 66;
			rm = HCServiceProxy
					.removeStailHashes(tm - milliseconds, false, evt);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish garbage collection", e);
			evt.endEvent(
					"SDFS Volume Cleanup Failed because " + e.getMessage(),
					SDFSEvent.ERROR);
			evt.success = false;
			throw new IOException(e);
		} finally {
			try {
				Main.pFullSched.recalcScheduler();
			} catch (Exception e) {
			}
		}
		return rm;
	}

}
