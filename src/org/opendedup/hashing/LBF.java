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

import java.io.ByteArrayOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.ProgressiveFileByteArrayLongMap.KeyBlob;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class LBF implements Serializable {
	private static final long serialVersionUID = 1L;
	public transient BloomFilter<org.opendedup.collections.ProgressiveFileByteArrayLongMap.KeyBlob> bfs = null;
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

	public LBF(long sz, double fpp) {
		this.bfs = BloomFilter.create(getFunnel(), sz, fpp);
	}

	public LBF(BloomFilter<KeyBlob> bfs) {
		this.bfs = bfs;
	}

	@SuppressWarnings("unchecked")
	LBF(File f) throws IOException, ClassNotFoundException {
		if (!f.exists()) {
			throw new IOException("bf does not exist " + f.getPath());
		}
		FileInputStream fin = new FileInputStream(f);
		ObjectInputStream oon = new ObjectInputStream(fin);
		BloomFilter<KeyBlob> kb = (BloomFilter<KeyBlob>) oon.readObject();
		bfs = kb;
		oon.close();
		f.delete();
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

	public void putAll(LBF that) {
		bfs.putAll(that.bfs);
	}

	public void save(File f) throws IOException {
		FileOutputStream fout = new FileOutputStream(f);
		ObjectOutputStream oon = new ObjectOutputStream(fout);
		oon.writeObject(this.bfs);
		oon.flush();
		oon.close();
		fout.flush();
		fout.close();
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
