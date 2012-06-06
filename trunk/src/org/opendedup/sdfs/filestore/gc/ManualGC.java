package org.opendedup.sdfs.filestore.gc;

import org.opendedup.mtools.FDisk;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class ManualGC {

	public static long clearChunks(int minutes) {
		return clearChunksMills((long)minutes * 60 *1000);
	}
	
	public static long clearChunksMills(long milliseconds) {
		GCMain.gclock.lock();
		if (GCMain.isLocked()) {

			GCMain.gclock.unlock();
			return -1;
		} else {
			try {
				GCMain.lock();
				long tm = System.currentTimeMillis();
				SDFSEvent.gcInfoEvent("SDFS Volume Cleanup Initiated");
				new FDisk();
				if (Main.chunkStoreLocal) {
					HashChunkService.processHashClaims();
					return HashChunkService.removeStailHashes(tm
							- milliseconds, true);
				}
				SDFSEvent.gcInfoEvent("SDFS Volume Cleanup Succeeded");
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish garbage collection",
						e);
				SDFSEvent.gcWarnEvent("SDFS Volume Cleanup Failed because " + e.getMessage());
			} finally {
				GCMain.unlock();
				GCMain.gclock.unlock();
			}
			return 0;
		}
	}

}
