package org.opendedup.hashing;

import java.io.File;

import java.io.IOException;
import java.io.Serializable;

import org.opendedup.util.CommandLineProgressBar;

public class LargeBloomFilter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final int AR_SZ = 256;
	transient FLBF[] bfs = new FLBF[AR_SZ];

	public LargeBloomFilter() {

	}

	public LargeBloomFilter(FLBF[] bfs) {
		this.bfs = bfs;
	}

	public static boolean exists(File dir) {
		for (int i = 0; i < AR_SZ; i++) {
			File f = new File(dir.getPath() + File.separator + "lbf" + i
					+ ".nbf");
			if (!f.exists())
				return false;

		}
		return true;
	}

	public LargeBloomFilter(File dir, long sz, double fpp, boolean fb)
			throws IOException {
		bfs = new FLBF[AR_SZ];
		CommandLineProgressBar bar = null;
		if (fb)
			bar = new CommandLineProgressBar("Loading BloomFilters",
					bfs.length, System.out);
		int isz = (int) (sz / bfs.length);
		for (int i = 0; i < bfs.length; i++) {
			File f = new File(dir.getPath() + File.separator + "lbf" + i
					+ ".nbf");
			bfs[i] = new FLBF(isz, fpp, f, true);
			if (bar != null)
				bar.update(i);

		}
		bar.finish();
	}

	private FLBF getMap(byte[] hash) {

		int hashb = hash[1];
		if (hashb < 0) {
			hashb = ((hashb * -1) + 127);
		}
		FLBF m = bfs[hashb];
		return m;
	}

	public boolean mightContain(byte[] b) {
		return getMap(b).mightContain(b);
	}

	public void put(byte[] b) {
		getMap(b).put(b);
	}

	public void save(File dir) throws IOException {
		CommandLineProgressBar bar = new CommandLineProgressBar(
				"Saving BloomFilters", bfs.length, System.out);
		for (int i = 0; i < bfs.length; i++) {
			bfs[i].save();
			bar.update(i);
		}
		bar.finish();
	}

	public FLBF[] getArray() {
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
