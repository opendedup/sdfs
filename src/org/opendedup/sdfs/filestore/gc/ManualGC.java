package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.BloomFDisk;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class ManualGC {

	public static SDFSEvent evt = null;

	public static long clearChunks() throws InterruptedException,
			IOException {
		return clearChunksMills();
	}

	public static synchronized long clearChunksMills()
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
				runGC();
			} catch (IOException e) {
				if (!Main.firstRun)
					throw e;
			} finally {
				try {
					Main.pFullSched.recalcScheduler();
				} catch (Exception e) {
				}
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
				try {
					rm = rm + runGC();
				} finally {
					try {
						Main.pFullSched.recalcScheduler();
					} catch (Exception e) {
					}
				}
				Main.firstRun = false;
			}
			evt.endEvent("SDFS Volume Cleanup Finished for "
					+ Main.volume.getName());
			return rm;
		} finally {
			l.unlock();
		}
	}

	private static long runGC() throws IOException {
		long rm = 0;
		try {

			if (Main.chunkStoreLocal && Main.volume.getName() != null) {
				BloomFDisk fd = new BloomFDisk(evt);
				evt.curCt = 33;
				rm = HCServiceProxy.processHashClaims(evt,fd.getResults());
				evt.curCt = 66;
			} else {
				FDiskEvent fevt = SDFSEvent.fdiskInfoEvent("running distributed fdisk", evt);
				HCServiceProxy.runFDisk(fevt);
				evt.curCt = 33;
				HCServiceProxy.processHashClaims(evt,null);
				evt.curCt = 66;
			}

		} catch (Throwable e) {
			SDFSLogger.getLog().warn("unable to finish garbage collection", e);
			evt.endEvent(
					"SDFS Volume Cleanup Failed because " + e.getMessage(),
					SDFSEvent.ERROR);
			evt.success = false;
			throw new IOException(e);
		}
		return rm;
	}

}
