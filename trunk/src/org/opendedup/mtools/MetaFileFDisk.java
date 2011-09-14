package org.opendedup.mtools;

import java.io.File;

import java.io.IOException;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

public class MetaFileFDisk {
	private long files = 0;
	private long corruptFiles = 0;

	public MetaFileFDisk(String path) throws IOException {
		SDFSLogger.getLog().info("Starting MetaFile FDISK");
		long start = System.currentTimeMillis();
		File f = new File(path);
		try {
			this.traverse(f);
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "]. Found ["
							+ this.corruptFiles + "] corrupt files");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
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
				SDFSLogger.getLog().error("error traversing " + dir.getPath(),
						e);
			}
		} else {
			
				this.checkDedupFile(dir);
		}
	}

	private void checkDedupFile(File metaFile) throws IOException {
		MetaDataDedupFile mf = MetaDataDedupFile.getFile(metaFile.getPath());
		String dfGuid = mf.getDfGuid();
		File mapFile = new File(Main.dedupDBStore + File.separator + dfGuid.substring(0, 2) + File.separator + dfGuid + File.separator + dfGuid + ".map");
		LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath(), "r");
		try {
			byte[] val = new byte[0];
			mp.iterInit();
			boolean corruption = false;
			long corruptBlocks = 0;
			while (val != null) {
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
			SDFSLogger.getLog().warn(
					"error while checking file [" + mapFile.getPath() + "]", e);
			throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files++;
	}

}
