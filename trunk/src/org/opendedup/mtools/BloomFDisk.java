package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;
import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.SparseDataChunk.HashLocPair;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.FileCounts;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFDisk {
	private long files = 0;
	private SDFSEvent fEvt = null;
	private long entries = 0;
	transient BloomFilter<KeyBlob> bf = null;

	public BloomFDisk(SDFSEvent evt) throws FDiskException {
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
			this.entries = Main.volume.getActualWriteBytes() / HashFunctionPool.min_page_size;
			int tr = 0;
			if(entries > Integer.MAX_VALUE)
				tr = Integer.MAX_VALUE;
			else
				tr = (int)this.entries;
			SDFSLogger.getLog().info("entries = " + tr);
			bf = BloomFilter.create(kbFunnel, tr, .01);
			SDFSLogger.getLog().info(
					"Starting FDISK for " + Main.volume.getName());
			long start = System.currentTimeMillis();

			this.traverse(f);
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "].");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check [" + files + "].");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]",
					SDFSEvent.ERROR);
			throw new FDiskException(e);

		}
	}
	
	public BloomFilter<KeyBlob> getResults() {
		return this.bf;
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			if (dir.getPath().endsWith(".map")) {
				this.checkDedupFile(dir);
			}
		}
	}

	private void checkDedupFile(File mapFile) throws IOException {
		DataMapInterface mp = null;
		try {
			mp = new LongByteArrayMap(mapFile.getPath());
			long prevpos = 0;
			byte[] val = new byte[0];
			mp.iterInit();
			while (val != null) {
				fEvt.curCt += (mp.getIterPos() - prevpos);
				prevpos = mp.getIterPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val);
					if (!ck.isLocalData()) {
							List<HashLocPair> al = ck.getFingers();
							for (HashLocPair p : al) {
								bf.put(new KeyBlob(p.hash));
							}
					}
				}
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
	
	Funnel<KeyBlob> kbFunnel = new Funnel<KeyBlob>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -1612304804452862219L;

		/**
		 * 
		 */

		@Override
		public void funnel(KeyBlob key, PrimitiveSink into) {
			into.putBytes(key.key);
		}
	};

}
