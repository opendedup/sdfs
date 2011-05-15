package org.opendedup.sdfs.filestore.gc;

import org.opendedup.mtools.FDisk;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class ManualGC {

	public static long clearChunks(int minutes) {
		GCMain.gclock.lock();
		if (GCMain.gcRunning) {
			
			GCMain.gclock.unlock();
			return -1;
		} else {

			try {
				GCMain.gcRunning = true;

				long tm = System.currentTimeMillis();
				new FDisk();
				if (Main.chunkStoreLocal) {
					HashChunkService.processHashClaims();
					long crtm = System.currentTimeMillis() - tm;
					return HashChunkService.removeStailHashes(minutes
							+ (int) ((crtm / 1000) / 60), true);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish garbage collection",
						e);
			} finally {
				GCMain.gcRunning = false;
				GCMain.gclock.unlock();
			}
			return 0;
		}
	}

}
