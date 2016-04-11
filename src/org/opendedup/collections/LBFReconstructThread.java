package org.opendedup.collections;

import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.logging.SDFSLogger;

public class LBFReconstructThread implements Runnable {

	LargeBloomFilter bf;
	ProgressiveFileByteArrayLongMap m;
	Exception ex = null;

	LBFReconstructThread(LargeBloomFilter bf, ProgressiveFileByteArrayLongMap m) {
		this.bf = bf;
		this.m = m;
	}

	@Override
	public void run() {
		try {
			m.iterInit();
			byte[] key = m.nextKey();
			while (key != null) {
				bf.put(key);
				key = m.nextKey();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to reconstruct bf for " + m, e);
			ex = e;
		}
	}

}
