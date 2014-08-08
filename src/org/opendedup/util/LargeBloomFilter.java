package org.opendedup.util;

import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class LargeBloomFilter {
	@SuppressWarnings("unchecked")
	BloomFilter<KeyBlob> [] bfs = new BloomFilter[128];
	
	public LargeBloomFilter(long sz, double fpp) {
		int isz  = (int)(sz/bfs.length)+2000;
		for(int i = 0;i<bfs.length;i++) {
			bfs[i] = BloomFilter.create(kbFunnel,isz,fpp);
		}
	}
	
	private BloomFilter<KeyBlob> getMap(byte[] hash) {

		int hashb = hash[2];
		if (hashb < 0) {
			hashb = ((hashb * -1) - 1);
		}
		int hashRoute = hashb;

		BloomFilter<KeyBlob> m = bfs[hashRoute];

		return m;
	}
	
	public boolean mightContain(byte [] b) {
		return getMap(b).mightContain(new KeyBlob(b));
	}
	
	public void put(byte [] b) {
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

}
