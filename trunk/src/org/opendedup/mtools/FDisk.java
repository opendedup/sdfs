package org.opendedup.mtools;

import java.io.File;

import java.io.IOException;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

public class FDisk {
	private long files = 0;
	private long corruptFiles = 0;
	private SDFSEvent fEvt = null;

	public FDisk(SDFSEvent evt) throws IOException {
		File f = new File(Main.dedupDBStore);
		fEvt = SDFSEvent.fdiskInfoEvent("Starting FDISK for " + Main.volume.getName() + " file count = " + FileCounts.getCount(f, false) + " file size = " +FileCounts.getSize(f, false));
		evt.addChild(fEvt);
		fEvt.maxCt = FileCounts.getSize(f, false);
		SDFSLogger.getLog().info("Starting FDISK");
		long start = System.currentTimeMillis();
		
		try {
			this.traverse(f);
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "]. Found ["
							+ this.corruptFiles + "] corrupt files");
			
			fEvt.endEvent("took [" + (System.currentTimeMillis() - start) / 1000
					+ "] seconds to check [" + files + "]. Found ["
					+ this.corruptFiles + "] corrupt files");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]", SDFSEvent.ERROR);
			throw new IOException(e);
		}
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			try {
				String[] children = dir.list();
				for (int i = 0; i < children.length; i++) {
					traverse(new File(dir, children[i]));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error traversing " + dir.getPath(),
						e);
			}
		} else {
			if (dir.getPath().endsWith(".map")) {
				this.checkDedupFile(dir);
			}
		}
	}

	private void checkDedupFile(File mapFile) throws IOException {
		LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath(), "r");
		long prevpos =  0;
		try {
			byte[] val = new byte[0];
			mp.iterInit();
			boolean corruption = false;
			long corruptBlocks = 0;
			while (val != null) {
				fEvt.curCt += (mp.getIterFPos()-prevpos);
				prevpos = mp.getIterFPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val);
					if (!ck.isLocalData()) {
						boolean exists = HCServiceProxy
								.hashExists(ck.getHash());
						if (!exists) {
							SDFSLogger.getLog().debug("file ["+ mapFile +"] could not find " + StringUtils.getHexString(ck.getHash()));
							corruption = true;
							corruptBlocks ++;
						}
					}
				}
			}
			if (corruption) {
				this.corruptFiles++;
				SDFSLogger.getLog().info(
						"map file " + mapFile.getPath() + " is suspect, [" + corruptBlocks + "] missing blocks found.");
			}
		} catch (Exception e) {
			SDFSLogger.getLog().debug(
					"error while checking file [" + mapFile.getPath() + "]", e);
			throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files++;
	}

}
