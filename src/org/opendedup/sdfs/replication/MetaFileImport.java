package org.opendedup.sdfs.replication;

import java.io.File;


import java.io.IOException;
import java.util.ArrayList;

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
	private ArrayList<String> hashes = null;
	private static int MAX_SZ = ((30*1024*1024)/Main.CHUNK_LENGTH);
	boolean corruption = false;
	private long entries = 0;

	public MetaFileImport(String path) throws IOException {
		SDFSLogger.getLog().info("Starting MetaFile FDISK. Max entries per batch are " + MAX_SZ);
		hashes = new ArrayList<String>();
		long start = System.currentTimeMillis();
		File f = new File(path);
		
		this.traverse(f);
		if(hashes.size() != 0) {
			try {
				HCServiceProxy.fetchChunks(hashes);
			} catch(Exception e) {
				SDFSLogger.getLog().error("Corruption Suspected on import",e);
				corruption = true;
			}
		}
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - start) / 1000
						+ "] seconds to import [" + files + "] files and [" + entries + "] blocks.");
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
	
	public boolean isCorrupt() {
		return this.corruption;
	}

	private void checkDedupFile(File metaFile) throws IOException {
		MetaDataDedupFile mf = MetaDataDedupFile.getFile(metaFile.getPath());
		String dfGuid = mf.getDfGuid();
		if (dfGuid != null) {
			File mapFile = new File(Main.dedupDBStore + File.separator
					+ dfGuid.substring(0, 2) + File.separator + dfGuid
					+ File.separator + dfGuid + ".map");
			if (!mapFile.exists()) {
				return;
			}
			LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath(), "r");
			try {
				byte[] val = new byte[0];
				mp.iterInit();
				while (val != null) {
					val = mp.nextValue();
					if (val != null) {
						SparseDataChunk ck = new SparseDataChunk(val);
						if (!ck.isLocalData()) {
							boolean exists = HCServiceProxy.localHashExists(ck
									.getHash());
							if (!exists) {
								hashes.add(StringUtils.getHexString(ck.getHash()));
								entries ++;
							}
							if(hashes.size()>=MAX_SZ) {
								try {
									SDFSLogger.getLog().debug("fetching " + hashes.size() + " blocks");
									HCServiceProxy.fetchChunks(hashes);
									SDFSLogger.getLog().debug("fetched " + hashes.size() + " blocks");
									hashes = null;
									hashes = new ArrayList<String>();
								} catch(Exception e) {
									SDFSLogger.getLog().error("Corruption Suspected on import",e);
									corruption = true;
								}
							}
						}
					}
				}
				if (corruption) {
					MetaFileStore.removeMetaFile(mf.getPath());
						throw new IOException(
								"Unable to continue MetaFile Import because there are too many missing blocks");

				} else {
					Main.volume.addVirtualBytesWritten(mf.length());
					Main.volume.updateCurrentSize(mf.getIOMonitor()
							.getVirtualBytesWritten());
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
