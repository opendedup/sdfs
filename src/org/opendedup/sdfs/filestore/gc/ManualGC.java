package org.opendedup.sdfs.filestore.gc;

import org.opendedup.mtools.FDisk;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.SDFSLogger;

public class ManualGC {

	public static boolean gcStarted = false;

	public static void clearChunks(int minutes) {
		if (!gcStarted) {
			gcStarted = true;
			try {
				new FDisk();
				if (Main.chunkStoreLocal) {
					HashChunkService.processHashClaims();
					HashChunkService.removeStailHashes(minutes);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish garbage collection",
						e);
			} finally {
				gcStarted = false;
			}
		}
	}

}
