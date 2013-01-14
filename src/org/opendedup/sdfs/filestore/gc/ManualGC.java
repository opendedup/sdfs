package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;


import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDisk;


import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class ManualGC {

	public static SDFSEvent evt = null;
	public static long clearChunks(int minutes) throws InterruptedException, IOException {
		return clearChunksMills((long)minutes * 60 *1000);
	}
	
	public static long clearChunksMills(long milliseconds) throws InterruptedException, IOException {
		WriteLock l = GCMain.gclock.writeLock();
		l.lock();
			try {
			long rm = 0;
			evt = SDFSEvent.gcInfoEvent("SDFS Volume Cleanup Initiated for " + Main.volume.getName());
			evt.maxCt = 100;
			evt.curCt = 0;
			runGC(milliseconds);
			if (Main.firstRun) {
				SDFSEvent wevt = SDFSEvent.waitEvent("Waiting 10 Seconds to run again",evt);
				wevt.maxCt = 10;
				for(int i = 0;i<10;i++) {
				Thread.sleep(1000);
				wevt.curCt++;
				}
				wevt.endEvent("Done Waiting");
				rm = rm + runGC(10 * 1000);
				Main.firstRun = false;
			}
			evt.endEvent("SDFS Volume Cleanup Finished for " + Main.volume.getName());
			return rm;
			}finally {
				l.unlock();
			}
	}
	
	private static long runGC(long milliseconds) {
		long rm = 0;
		try {
			
			long tm = System.currentTimeMillis();
			
			new FDisk(evt);
			evt.curCt = 33;
			if (Main.chunkStoreLocal) {
				HCServiceProxy.processHashClaims(evt);
				evt.curCt = 66;
				rm = HCServiceProxy.removeStailHashes(tm
						- milliseconds, false,evt);
			}
			
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish garbage collection",
					e);
			evt.endEvent("SDFS Volume Cleanup Failed because " + e.getMessage(), SDFSEvent.ERROR);
		} finally {
			Main.pFullSched.recalcScheduler();
		}
		return rm;
	}

}
