package org.opendedup.mtools;

import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.StringUtils;

public class MetaFileImport {
	private long files = 0;
	private List<MetaDataDedupFile> corruptFiles = new ArrayList<MetaDataDedupFile>();

	public MetaFileImport(String path, SDFSEvent evt) throws IOException {
		SDFSLogger.getLog().info("Starting MetaFile FDISK");
		long start = System.currentTimeMillis();
		File f = new File(path);
		this.traverse(f);
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - start) / 1000
						+ "] seconds to check [" + files + "]. Found ["
						+ this.corruptFiles.size() + "] corrupt files");
	}

	public List<MetaDataDedupFile> getCorruptFiles() {
		return this.corruptFiles;
	}

	private void traverse(File dir) throws IOException {

		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {

			this.checkDedupFile(dir);
		}
	}

	private void checkDedupFile(File metaFile) throws IOException {
		MetaDataDedupFile mf = MetaDataDedupFile.getFile(metaFile.getPath());
		if (!mf.isSymlink()) {
			String dfGuid = mf.getDfGuid();
			File mapFile = new File(Main.dedupDBStore + File.separator
					+ dfGuid.substring(0, 2) + File.separator + dfGuid
					+ File.separator + dfGuid + ".map");
			LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath());
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
							byte[] exists = HCServiceProxy.hashExists(
									ck.getHash(), false);
							if (exists[0] == -1) {
								SDFSLogger.getLog().debug(
										"file ["
												+ mapFile
												+ "] could not find "
												+ StringUtils.getHexString(ck
														.getHash()));
								corruption = true;
								corruptBlocks++;
							}
						}
					}
				}
				if (corruption) {
					if (this.corruptFiles.size() > 1000)
						throw new IOException(
								"Unable to continue MetaFile Import because there are too many missing blocks");
					this.corruptFiles.add(mf);
					SDFSLogger.getLog()
							.info("map file " + mapFile.getPath()
									+ " is suspect, [" + corruptBlocks
									+ "] missing blocks found.");
				}
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("error while checking file [" + mapFile.getPath()
								+ "]", e);
				throw new IOException(e);
			} finally {
				mp.close();
				mp = null;
			}
		}
		this.files++;
	}
}
