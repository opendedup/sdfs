package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.StringUtils;

public class FDisk {
	private long files = 0;
	private long corruptFiles = 0;
	private SDFSEvent fEvt = null;
	private static final int MAX_BATCH_SIZE = 100;

	public FDisk(SDFSEvent evt) throws FDiskException {
		init(evt);
	}

	public void init(SDFSEvent evt) throws FDiskException {
		File f = new File(Main.dedupDBStore);
		if (!f.exists()) {
			SDFSEvent
					.fdiskInfoEvent(
							"FDisk Will not start because the volume has not been written too",
							evt)
					.endEvent(
							"FDisk Will not start because the volume has not been written too");
			throw new FDiskException(
					"FDisk Will not start because the volume has not been written too");
		}
		try {
			fEvt = SDFSEvent.fdiskInfoEvent(
					"Starting FDISK for " + Main.volume.getName()
							+ " file count = " + FileCounts.getCount(f, false)
							+ " file size = " + FileCounts.getSize(f, false),
					evt);
			fEvt.maxCt = FileCounts.getSize(f, false);
			SDFSLogger.getLog().info(
					"Starting FDISK for " + Main.volume.getName());
			long start = System.currentTimeMillis();

			this.traverse(f);
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "]. Found ["
							+ this.corruptFiles + "] corrupt files");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check [" + files + "]. Found ["
					+ this.corruptFiles + "] corrupt files");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]",
					SDFSEvent.ERROR);
			throw new FDiskException(e);

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

	private int batchCheck(ArrayList<SparseDataChunk> chunks)
			throws IOException {
		List<SparseDataChunk> pchunks = HCServiceProxy.batchHashExists(chunks);
		int corruptBlocks = 0;
		for (SparseDataChunk ck : pchunks) {
			byte[] exists = ck.getHashLoc();
			if (exists[0] == -1) {
				SDFSLogger.getLog().debug(
						"could not find "
								+ StringUtils.getHexString(ck.getHash()));
				corruptBlocks++;
			}
		}
		return corruptBlocks;
	}

	private void checkDedupFile(File mapFile) throws IOException {
		LongByteArrayMap mp = null;
		try {
			mp = new LongByteArrayMap(mapFile.getPath());
			long prevpos = 0;
			ArrayList<SparseDataChunk> chunks = new ArrayList<SparseDataChunk>(
					MAX_BATCH_SIZE);
			byte[] val = new byte[0];
			mp.iterInit();
			long corruptBlocks = 0;
			while (val != null) {
				fEvt.curCt += (mp.getIterFPos() - prevpos);
				prevpos = mp.getIterFPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val);
					if (!ck.isLocalData()) {
						if (Main.chunkStoreLocal) {

							byte[] exists = HCServiceProxy.hashExists(
									ck.getHash(), false,
									Main.volume.getClusterCopies());
							if (exists[0] == -1) {
								SDFSLogger.getLog().debug(
										"file ["
												+ mapFile
												+ "] could not find "
												+ StringUtils.getHexString(ck
														.getHash()));
								corruptBlocks++;
							}
						} else {
							chunks.add(ck);
							if (chunks.size() >= MAX_BATCH_SIZE) {
								corruptBlocks += batchCheck(chunks);
								chunks = new ArrayList<SparseDataChunk>(
										MAX_BATCH_SIZE);
							}
						}
					}
				}
			}
			if (chunks.size() > 0) {
				corruptBlocks += batchCheck(chunks);
			}
			if (corruptBlocks > 0) {
				this.corruptFiles++;
				SDFSLogger.getLog().warn(
						"map file " + mapFile.getPath() + " is suspect, ["
								+ corruptBlocks + "] missing blocks found.");
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().info(
					"error while checking file [" + mapFile.getPath() + "]", e);
			throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files++;
	}

}
