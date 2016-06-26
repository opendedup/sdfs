package org.opendedup.hashing;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.ProgressiveFileByteArrayLongMap.KeyBlob;
import org.opendedup.utils.hashing.FileBasedBloomFilter;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class FLBF implements Serializable {
	private static final long serialVersionUID = 1L;
	public transient FileBasedBloomFilter<KeyBlob> bfs = null;
	public transient ReentrantReadWriteLock l = new ReentrantReadWriteLock();
	private static Funnel<KeyBlob> kbFunnel = new Funnel<KeyBlob>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * 
		 */

		@Override
		public void funnel(KeyBlob key, PrimitiveSink into) {
			into.putBytes(key.key);
		}
	};

	public FLBF(long sz, double fpp,File path) {
		this.bfs = FileBasedBloomFilter.create(getFunnel(), sz, fpp,path.getPath());
	}
	
	public boolean mightContain(KeyBlob kb) {
		l.readLock().lock();
		try {
			return bfs.mightContain(kb);
		} finally {
			l.readLock().unlock();
		}
	}

	public void put(KeyBlob kb) {
		l.writeLock().lock();
		try {
			bfs.put(kb);
		} finally {
			l.writeLock().unlock();
		}
	}

	public void putAll(byte[] that) {
		bfs.readIn(that);
	}

	public void save() throws IOException {
		bfs.close();
	}
	
	public void vanish() {
		bfs.vanish();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		this.bfs.writeTo(baos);
		return baos.toByteArray();
	}

	public static Funnel<KeyBlob> getFunnel() {

		return kbFunnel;
	}
}
