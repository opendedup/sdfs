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

import org.opendedup.collections.ShardedFileByteArrayLongMap.KeyBlob;
import org.opendedup.utils.hashing.FileBasedBloomFilter;

import com.duprasville.guava.probably.CuckooFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;


public class FLBF implements Serializable {
	private static final long serialVersionUID = 1L;
	public transient FileBasedBloomFilter<KeyBlob> bfs = null;
	public transient ReentrantReadWriteLock l = new ReentrantReadWriteLock();
	boolean counting = false;
	File path = null;
	CuckooFilter filter = null;
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

	public FLBF(long sz, double fpp, File path, boolean memory, boolean counting) throws IOException {
		this.counting = counting;
		if (counting) {
			this.path = path;
			if (path.exists()) {

				FileInputStream fin = new FileInputStream(path);
				ObjectInputStream oon = new ObjectInputStream(fin);
				try {
					filter = (CuckooFilter) oon.readObject();
				} catch (Exception e) {
					try {
						oon.close();
						path.delete();
					} catch (Exception e1) {

					}
					throw new IOException(e);
				}

				oon.close();
				path.delete();
			} else {
				this.filter =  CuckooFilter.create( sz, fpp);
			}
		} else {
			this.bfs = FileBasedBloomFilter.create(getFunnel(), sz, fpp, path.getPath(), memory);
		}
	}

	public long getSize() {
		
			return 0;
	}

	public boolean mightContain(byte[] bytes) {
		l.readLock().lock();
		try {
			if (this.counting)
				return this.filter.contains(bytes);
			else
				return bfs.mightContain(bytes);
		} finally {
			l.readLock().unlock();
		}
	}
	
	public void put(byte[] bytes) {
		l.writeLock().lock();
		try {
			if (this.counting) {
				this.filter.add(bytes);
				return;
			} else {

				bfs.put(bytes);
			}
		} finally {
			l.writeLock().unlock();
		}
	}

	public void putAll(byte[] that) {
		bfs.readIn(that);
	}

	public void save() throws IOException {
		if (this.counting) {
			//SDFSLogger.getLog().info("writing to " + path);
			FileOutputStream fout = new FileOutputStream(path);
			ObjectOutputStream oon = new ObjectOutputStream(fout);
			oon.writeObject(this.filter);
			oon.flush();
			oon.close();
			fout.flush();
			fout.close();
			//SDFSLogger.getLog().info("wrote to " + path);
		} else {
			bfs.close();
		}
	}

	public void vanish() {
		if (counting)
			path.delete();
		else {
			try {
				bfs.close();
			} catch (IOException e) {
				
			}
			bfs.vanish();
			path.delete();
		}
	}

	public void remove(byte[] data) throws IOException {
		l.writeLock().lock();
		try {
			if (counting) {
				this.filter.remove(data);
			} else {
				throw new IOException("not implemented");
			}
		} finally {
			l.writeLock().unlock();
		}
	}

	public byte[] getBytes() throws IOException {
		if (counting)
			throw new IOException("not implemented");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		this.bfs.writeTo(baos);
		return baos.toByteArray();
	}

	public static Funnel<KeyBlob> getFunnel() {

		return kbFunnel;
	}
}
