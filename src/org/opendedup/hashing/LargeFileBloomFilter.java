package org.opendedup.hashing;

import java.io.File;

import java.io.IOException;
import java.io.Serializable;

import org.opendedup.util.CommandLineProgressBar;

import com.google.common.io.Files;

public class LargeFileBloomFilter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	transient FLBF[] bfs = new FLBF[32];

	public LargeFileBloomFilter(long sz, double fpp,boolean sync) {
		File td = Files.createTempDir();
		td.mkdirs();
		bfs = new FLBF[32];
		int isz = (int) (sz / bfs.length);
		for (int i = 0; i < bfs.length; i++) {
			bfs[i] = new FLBF(isz, fpp,new File(td,i+".bfs"),sync );
		}
	}
	
	public LargeFileBloomFilter(FLBF[] bfs) {
		this.bfs = bfs;
	}

	private FLBF getMap(byte[] hash) {

		int hashb = hash[1];
		if (hashb < 0) {
			hashb = ((hashb * -1) + 127);
		}
		FLBF m = bfs[hashb/8];
		return m;
	}

	public void putAll(byte [] that, int pos) {
		this.bfs[pos].putAll(that);
	}

	public boolean mightContain(byte[] b) {
		return getMap(b).mightContain(b);
	}

	public void put(byte[] b) {
		getMap(b).put(b);
	}

	public void save() throws IOException {
		CommandLineProgressBar bar = new CommandLineProgressBar(
				"Saving BloomFilters", bfs.length, System.out);
		for (int i = 0; i < bfs.length; i++) {
			bfs[i].save();
			bar.update(i);
		}
		bar.finish();
	}
	
	public void vanish() {
		for (int i = 0; i < bfs.length; i++) {
			bfs[i].vanish();
		}
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
