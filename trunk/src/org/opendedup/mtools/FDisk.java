package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class FDisk {
	private static Logger log = Logger.getLogger("sdfs");
	private long files = 0;
	private long corruptFiles = 0;

	public FDisk() {
		long start = System.currentTimeMillis();
		File f = new File(Main.dedupDBStore);
		this.traverse(f);
		log.info("took [" + (System.currentTimeMillis() - start) / 1000
				+ "] seconds to check [" + files + "]. Found ["
				+ this.corruptFiles + "] corrupt files");
	}

	private void traverse(File dir) {

		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			if (dir.getPath().endsWith(".map")) {
				try {
					this.checkDedupFile(dir);
				} catch (Exception e) {
					log.log(Level.WARNING, "error traversing for FDISK", e);
				}
			}
		}
	}

	private void checkDedupFile(File mapFile) throws IOException {
		LongByteArrayMap mp = new LongByteArrayMap(SparseDataChunk.RAWDL,
				mapFile.getPath());
		try {
			byte[] val = new byte[0];
			mp.iterInit();
			boolean corruption = false;
			while (val != null) {
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val);
					if (!ck.isLocalData()) {
						boolean exists = HCServiceProxy
								.hashExists(ck.getHash());
						if (!exists) {
							corruption = true;
						}
					}
				}
			}
			if (corruption) {
				this.corruptFiles++;
				log.warning("map file " + mapFile.getPath() + " is corrupt");
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "error while checking file ["
					+ mapFile.getPath() + "]", e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files++;
	}

}
