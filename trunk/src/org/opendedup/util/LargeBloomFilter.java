package org.opendedup.util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;


public class LargeBloomFilter  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	transient LBF[] bfs = new LBF[256];

	public LargeBloomFilter() {

	}

	public LargeBloomFilter(LBF[] bfs) {
		this.bfs = bfs;
	}

	public LargeBloomFilter(long sz, double fpp) {
		bfs = new LBF[256];
		int isz = (int) (sz / bfs.length);
		for (int i = 0; i < bfs.length; i++) {
			bfs[i] = new LBF(isz, fpp);
		}
	}

	public LargeBloomFilter(File dir, long sz, double fpp, boolean fb)
			throws IOException {
		bfs = new LBF[256];
		CommandLineProgressBar bar = null;
		if (fb)
			bar = new CommandLineProgressBar("Loading BloomFilters",
					bfs.length, System.out);
		for (int i = 0; i < bfs.length; i++) {
			try {
				bfs[i] = new LBF(new File(dir.getPath() + File.separator
						+ "lbf" + i + ".bf"));
				if (bar != null)
					bar.update(i);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			} finally {

			}
		}
		bar.finish();
	}

	private LBF getMap(byte[] hash) {

		int hashb = hash[1];
		if (hashb < 0) {
			hashb = ((hashb * -1) + 127);
		}
		LBF m = bfs[hashb];
		return m;
	}
	
	public void putAll(LBF that,int pos) {
		this.bfs[pos].putAll(that);
	}

	public boolean mightContain(byte[] b) {
		return getMap(b).mightContain(new KeyBlob(b));
	}

	public void put(byte[] b) {
		getMap(b).put(new KeyBlob(b));
	}

	

	public void save(File dir) throws IOException {
		CommandLineProgressBar bar = new CommandLineProgressBar(
				"Saving BloomFilters", bfs.length, System.out);
		for (int i = 0; i < bfs.length; i++) {
			File f = new File(dir.getPath() + File.separator + "lbf" + i
					+ ".bf");
			bfs[i].save(f);
			bar.update(i);
		}
		bar.finish();
	}

	public LBF[] getArray() {
		return this.bfs;
	}

	public static void main(String[] args) {
		int[] ht = new int[32];
		for (int i = 0; i < 128; i++) {
			int z = i / 4;
			ht[z]++;
		}
		for (int i : ht) {
			System.out.println("i=" + i);
		}
	}

}
