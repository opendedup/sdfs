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

	public FLBF(long sz, double fpp,File path,boolean memory) {
		this.bfs = FileBasedBloomFilter.create(getFunnel(), sz, fpp,path.getPath(),memory);
	}
	
	public boolean mightContain(byte [] bytes) {
		l.readLock().lock();
		try {
			return bfs.mightContain(bytes);
		} finally {
			l.readLock().unlock();
		}
	}

	public void put(byte [] bytes) {
		l.writeLock().lock();
		try {
			bfs.put(bytes);
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
