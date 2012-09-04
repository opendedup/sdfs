package org.opendedup.sdfs.filestore.gc;

import org.opendedup.mtools.FDisk;


import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class ManualGC {

	public static SDFSEvent evt = null;
	public static long clearChunks(int minutes) throws InterruptedException {
		return clearChunksMills((long)minutes * 60 *1000);
	}
	
	public static long clearChunksMills(long milliseconds) throws InterruptedException {
		GCMain.gclock.lock();
		if (GCMain.isLocked()) {

			GCMain.gclock.unlock();
			return -1;
		} else {
			try {
			long rm = 0;
			evt = SDFSEvent.gcInfoEvent("SDFS Volume Cleanup Initiated for " + Main.volume.getName());
			evt.maxCt = 100;
			evt.curCt = 0;
			runGC(milliseconds);
			if (Main.firstRun) {
				SDFSEvent wevt = SDFSEvent.waitEvent("Waiting 10 Seconds to run again");
				wevt.maxCt = 10;
				evt.children.add(wevt);
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
				GCMain.gclock.unlock();
			}
		}
	}
	
	private static long runGC(long milliseconds) {
		long rm = 0;
		try {
			GCMain.lock();
			long tm = System.currentTimeMillis();
			
			new FDisk(evt);
			evt.curCt = 33;
			if (Main.chunkStoreLocal) {
				HashChunkService.processHashClaims(evt);
				evt.curCt = 66;
				rm = HashChunkService.removeStailHashes(tm
						- milliseconds, true,evt);
			}
			
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish garbage collection",
					e);
			evt.endEvent("SDFS Volume Cleanup Failed because " + e.getMessage(), SDFSEvent.ERROR);
		} finally {
			GCMain.unlock();
		}
		return rm;
	}

}
