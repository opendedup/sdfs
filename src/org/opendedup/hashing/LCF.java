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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.ShardedFileByteArrayLongMap.KeyBlob;

import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class LCF implements Serializable {
	private static final long serialVersionUID = 1L;
	public transient ReentrantReadWriteLock l = new ReentrantReadWriteLock();
	CuckooFilter<org.opendedup.collections.ShardedFileByteArrayLongMap.KeyBlob> filter = null;
	File f = null;
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

	public LCF(long sz, double fpp) {
		this.filter = new CuckooFilter.Builder<>(getFunnel(), sz).build();
	}

	public LCF(CuckooFilter<KeyBlob> bfs) {
		filter = bfs;
	}

	@SuppressWarnings("unchecked")
	LCF(long sz, File f) throws IOException, ClassNotFoundException {
		this.f = f;
		if (f.exists()) {
		FileInputStream fin = new FileInputStream(f);
		ObjectInputStream oon = new ObjectInputStream(fin);
		filter = (CuckooFilter<KeyBlob>) oon.readObject();
		oon.close();
		f.delete();
		}else {
			this.filter = new CuckooFilter.Builder<>(getFunnel(), sz).build();
		}
		System.out.println(this.filter.getStorageSize());
	}
	
	public void remove(KeyBlob kb) {
			filter.delete(kb);
	}

	public boolean mightContain(KeyBlob kb) {
		
		l.readLock().lock();
		try {
			return filter.mightContain(kb);
		} finally {
			l.readLock().unlock();
		}
	}

	public void put(KeyBlob kb) {
		
		l.writeLock().lock();
		try {
			filter.put(kb);
		} finally {
			l.writeLock().unlock();
		}
	}

	public void save() throws IOException {
		FileOutputStream fout = new FileOutputStream(f);
		ObjectOutputStream oon = new ObjectOutputStream(fout);
			oon.writeObject(this.filter);
		oon.flush();
		oon.close();
		fout.flush();
		fout.close();
	}

	public static Funnel<KeyBlob> getFunnel() {

		return kbFunnel;
	}

}
