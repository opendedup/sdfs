package org.opendedup.sdfs.filestore.gc;

import org.opendedup.mtools.FDisk;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class ManualGC {

	public static long clearChunks(int minutes) {
		GCMain.gclock.lock();
		if (GCMain.isLocked()) {

			GCMain.gclock.unlock();
			return -1;
		} else {

			try {
				GCMain.lock();
				long tm = System.currentTimeMillis();
				new FDisk();
				if (Main.chunkStoreLocal) {
					HashChunkService.processHashClaims();
					return HashChunkService.removeStailHashes(tm
							- (minutes * 60 * 1000), true);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish garbage collection",
						e);
			} finally {
				GCMain.unlock();
				GCMain.gclock.unlock();
			}
			return 0;
		}
	}

}
