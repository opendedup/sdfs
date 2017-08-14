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
package org.opendedup.sdfs.io;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.notification.ReadAheadEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Longs;

public class ReadAhead {
	SparseDedupFile df;
	ReadAheadEvent evt = null;
	boolean closeWhenDone;
	private static transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	protected static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(1, Main.readAheadThreads, 10,
			TimeUnit.SECONDS, worksQueue);
	LongByteArrayMap mp = null;
	long maxExtent;
	private static LoadingCache<Long, Boolean> readAheads =  CacheBuilder.newBuilder().maximumSize(10000)
			.build(new CacheLoader<Long, Boolean>() {
				public Boolean load(Long pos) throws Exception {
					try {
						SDFSLogger.getLog().info("downloading "+ pos);
						HCServiceProxy.cacheData(pos);
						SDFSLogger.getLog().info("done downloading "+ pos);
					}catch(Exception e) {
						SDFSLogger.getLog().debug("error caching chunk [" + pos + "] ", e);
						return false;
					}
					return true;
				}
			});
	
	

	public static ReadAhead getReadAhead(SparseDedupFile df) throws ExecutionException, IOException {
		
		if (Main.readAhead)
			return new ReadAhead(df);
		else
			throw new IOException("ReadAhead disabled");
	}

	public static ReadAhead getReadAhead(MetaDataDedupFile mf) throws ExecutionException, IOException {
		SparseDedupFile df = (SparseDedupFile) DedupFileStore.getDedupFile(mf);
		return new ReadAhead(df);
	}

	public ReadAhead(SparseDedupFile df) throws IOException {
		SDFSLogger.getLog().debug("initiating readahead for " + df.mf.getPath());
		this.df = df;
	}


	private static class CacheChunk implements Runnable {
		long pos;
		SparseDedupFile df;
		

		public void run() {
			if (pos >= df.mf.length())
					return;
				SparseDataChunk sck;
				try {
					sck = df.getSparseDataChunk(pos);
				} catch (IOException | FileClosedException e) {
					return;
				}
				TreeMap<Integer, HashLocPair> al = sck.getFingers();
				for (HashLocPair p : al.values()) {
					long ppos = Longs.fromByteArray(p.hashloc);
					if (ppos > 100 || ppos < -100) {
						if(ppos != 0) {
							try {
								readAheads.get(ppos);
							} catch (ExecutionException e) {
								
							}
						}
					}
				}
			}
	}

	public void readAhead(long pos) {
		synchronized(this) {
		if(pos <= this.maxExtent)
			return;
		else
			this.maxExtent = pos + (Main.readAheadThreads-2)*Main.CHUNK_LENGTH;
		}
		try {
				for(int i = 0;i<Main.readAheadThreads;i++) {
					CacheChunk ck = new CacheChunk();
					ck.df = df;
					ck.pos = pos;
					pos +=Main.CHUNK_LENGTH;
					executor.execute(ck);
				}
			}catch(Exception e) {
				
			} finally {
				synchronized(this) {
					this.maxExtent = pos;
				}
			}
		
		
	}

}
