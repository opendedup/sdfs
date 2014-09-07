package org.opendedup.util;

import java.io.File;

import java.io.IOException;
import java.io.Serializable;

import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;
import org.opendedup.logging.SDFSLogger;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class LargeBloomFilter implements Serializable{

	private static final long serialVersionUID = 1L;
	LBF[] bfs = new LBF[16];

	public LargeBloomFilter(long sz, double fpp) {

		bfs = new LBF[16];
		int isz = (int) (sz / bfs.length);
		for (int i = 0; i < bfs.length; i++) {
			bfs[i] = new LBF(BloomFilter.create(kbFunnel, isz, fpp));
		}
	}

	public static int guessSz(long sz) {
		long msz = (long) 8 * (long) Integer.MAX_VALUE;
		if (sz > msz) {
			return 16;
		} else {
			return 8;
		}
	}

	public LargeBloomFilter(File dir, long sz, double fpp, boolean fb)
			throws IOException {
			bfs = new LBF[16];
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

		int hashb = hash[2];
		if (hashb < 0) {
				hashb = ((hashb * -1) - 1);
		}
		int hashRoute = 0;
		hashRoute = hashb/8;
		LBF m = bfs[hashRoute];
		return m;
	}

	public boolean mightContain(byte[] b) {
		return getMap(b).mightContain(new KeyBlob(b));
	}

	public void put(byte[] b) {
		getMap(b).put(new KeyBlob(b));
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

	public void save(File dir) throws IOException {
		CommandLineProgressBar bar = new CommandLineProgressBar("Saving BloomFilters",
				bfs.length, System.out);
		for (int i = 0; i < bfs.length; i++) {
			File f = new File(dir.getPath() + File.separator + "lbf" + i
					+ ".bf");
			bfs[i].save(f);
			bar.update(i);
		}
		bar.finish();
	}
	
	public static void main(String [] args) {
		int [] ht = new int[16];
		for(int i = 0; i < 128;i++) {
			int z = i/8;
			ht[z]++;
		}
		for(int i :ht) {
			System.out.println("i=" + i);
		}
	}

}
