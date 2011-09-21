package org.opendedup.sdfs.replication;

import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

public class MetaFileImport {
	private long files = 0;
	private List<MetaDataDedupFile> corruptFiles = new ArrayList<MetaDataDedupFile>();

	public MetaFileImport(String path) throws IOException {
		SDFSLogger.getLog().info("Starting MetaFile FDISK");
		long start = System.currentTimeMillis();
		File f = new File(path);
		this.traverse(f);
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - start) / 1000
						+ "] seconds to import [" + files + "]. Found ["
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
		String dfGuid = mf.getDfGuid();
		File mapFile = new File(Main.dedupDBStore + File.separator
				+ dfGuid.substring(0, 2) + File.separator + dfGuid
				+ File.separator + dfGuid + ".map");
		if(!mapFile.exists()) {
			SDFSLogger.getLog().error(mapFile.getPath() + " does not exist!");
			throw new IOException(mapFile.getPath() + " does not exist!");
		}
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
				MetaFileStore.removeMetaFile(mf.getPath());
				if (this.corruptFiles.size() > 1000)
					throw new IOException(
							"Unable to continue MetaFile Import because there are too many missing blocks");
				this.corruptFiles.add(mf);
				SDFSLogger.getLog().info(
						"map file " + mapFile.getPath() + " is suspect, ["
								+ corruptBlocks + "] missing blocks found.");
				
			} else {
				Main.volume.addVirtualBytesWritten(mf.length());
				Main.volume.updateCurrentSize(mf.getIOMonitor().getVirtualBytesWritten());
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
