/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.hashing;

import java.io.File;


import java.io.IOException;
import java.io.Serializable;

import org.opendedup.sdfs.Main;
import org.opendedup.util.CommandLineProgressBar;


public class LargeFileBloomFilter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	transient FLBF[] bfs = new FLBF[256];
	boolean counting = false;

	public LargeFileBloomFilter(long sz, double fpp,boolean sync,boolean counting) throws IOException {
		File td = new File(new File(Main.dedupDBStore).getParent()+ File.separator + "tmp");
		td.mkdirs();
		bfs = new FLBF[256];
		long isz = sz / bfs.length;
		for (int i = 0; i < bfs.length; i++) {
			bfs[i] = new FLBF(isz, fpp,new File(td,i+".bfs"),sync, counting);
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
		FLBF m = bfs[hashb];
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
